"""The WatchThread Class is responsible for monitoring the cluster for
resource events
"""
# Standard
from threading import Lock
from typing import Dict, List, Optional, Set
import copy
import dataclasses
import os

# Third Party
from kubernetes import watch

# First Party
import alog

# Local
from .... import config
from ....deploy_manager import DeployManagerBase, KubeEventType, KubeWatchEvent
from ....managed_object import ManagedObject
from ..filters import FilterManager, get_configured_filter
from ..leader_election import LeadershipManagerBase
from ..utils import (
    ReconcileRequest,
    ReconcileRequestType,
    ResourceId,
    WatchedResource,
    WatchRequest,
    parse_time_delta,
)
from .base import ThreadBase

log = alog.use_channel("WTCHTHRD")

# Forward declaration of ReconcileThread
RECONCILE_THREAD_TYPE = "ReconcileThread"


class WatchThread(ThreadBase):  # pylint: disable=too-many-instance-attributes
    """The WatchThread monitors the cluster for changes to a specific GroupVersionKind either
    cluster-wide or for a particular namespace. When it detects a change it checks the event
    against the registered Filters and submits a ReconcileRequest if it passes. Every resource
    that has at least one watch request gets a corresponding WatchedResource object whose main
    job is to store the current Filter status
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        reconcile_thread: RECONCILE_THREAD_TYPE,
        kind: str,
        api_version: str,
        namespace: Optional[str] = None,
        deploy_manager: DeployManagerBase = None,
        leadership_manager: LeadershipManagerBase = None,
    ):
        """Initialize a WatchThread by assigning instance variables and creating maps

        Args:
            reconcile_thread: ReconcileThread
                The reconcile thread to submit requests to
            kind: str
                The kind to watch
            api_version: str
                The api_version to watch
            namespace: Optional[str] = None
                The namespace to watch. If none then cluster-wide
            deploy_manager: DeployManagerBase = None
                The deploy_manager to watch events
            leadership_manager: LeadershipManagerBase = None
                The leadership manager to use for elections
        """
        # Setup initial variables
        self.reconcile_thread = reconcile_thread
        self.kind = kind
        self.api_version = api_version
        self.namespace = namespace

        name = f"watch_thread_{self.api_version}_{self.kind}"
        if self.namespace:
            name = name + f"_{self.namespace}"
        super().__init__(
            name=name,
            daemon=True,
            deploy_manager=deploy_manager,
            leadership_manager=leadership_manager,
        )

        # Setup kubernetes watch resource
        self.kubernetes_watch = watch.Watch()

        # Setup watched resources and request mappings. watched_resources
        # is used to track the current status of a resource in a cluster and also includes
        # the current filters. watch_request tracks all of the Controllers that have watched
        # a specific resource or groupings of resources
        self.watched_resources: Dict[str, WatchedResource] = {}
        self.watch_requests: Dict[str, Set[WatchRequest]] = {}

        # Lock for adding/gathering watch requests
        self.watch_request_lock = Lock()

        # Variables for tracking retries
        self.attempts_left = config.python_watch_manager.watch_retry_count
        self.retry_delay = parse_time_delta(
            config.python_watch_manager.watch_retry_delay or ""
        )

    def run(self):
        """The WatchThread's control loop continuously watches the DeployManager for any new
        events. For every event it gets it gathers all the WatchRequests whose `watched` value
        applies. The thread then initializes a WatchedObject if one doesn't already exist and
        tests the event against each request's Filter. Finally, it submits a ReconcileRequest
        for all events that pass
        """

        # Check for leadership and shutdown at the start
        list_resource_version = 0
        while True:
            try:
                if not self.check_preconditions():
                    log.debug("Checking preconditions failed. Shutting down")
                    return

                for event in self.deploy_manager.watch_objects(
                    self.kind,
                    self.api_version,
                    namespace=self.namespace,
                    resource_version=list_resource_version,
                    watch_manager=self.kubernetes_watch,
                ):
                    # Validate leadership on each event
                    if not self.check_preconditions():
                        log.debug("Checking preconditions failed. Shutting down")
                        return

                    resource = event.resource

                    # Gather all the watch requests which apply to this event
                    watch_requests = self._gather_resource_requests(resource)
                    if not watch_requests:
                        log.debug2("Skipping resource without requested watch")
                        self._clean_event(event)
                        continue

                    # Ensure a watched object exists for every resource
                    if resource.uid not in self.watched_resources:
                        self._create_watched_resource(resource, watch_requests)

                    # Check both global and watch specific filters
                    watch_requests = self._check_filters(
                        watch_requests, resource, event.type
                    )
                    if not watch_requests:
                        log.debug2(
                            "Skipping event %s as all requests failed filters", event
                        )
                        self._clean_event(event)
                        continue

                    # Push a reconcile request for each watch requested
                    for watch_request in watch_requests:
                        log.debug(
                            "Requesting reconcile for %s",
                            resource,
                            extra={"resource": watch_request.requester.get_resource()},
                        )
                        self._request_reconcile(event, watch_request)

                    # Clean up any resources used for the event
                    self._clean_event(event)

                # Update the resource version to only get new events
                list_resource_version = self.kubernetes_watch.resource_version
            except Exception as exc:
                log.info(
                    "Exception raised when attempting to watch %s",
                    repr(exc),
                    exc_info=exc,
                )
                if self.attempts_left <= 0:
                    log.error(
                        "Unable to start watch within %d attempts",
                        config.python_watch_manager.watch_retry_count,
                    )
                    os._exit(1)

                if not self.wait_on_precondition(self.retry_delay.total_seconds()):
                    log.debug(
                        "Checking preconditions failed during retry. Shutting down"
                    )
                    return
                self.attempts_left = self.attempts_left - 1
                log.info("Restarting watch with %d attempts left", self.attempts_left)

    ## Class Interface ###################################################

    def stop_thread(self):
        """Override stop_thread to stop the kubernetes client's Watch as well"""
        super().stop_thread()
        self.kubernetes_watch.stop()

    ## Public Interface ###################################################

    def request_watch(self, watch_request: WatchRequest):
        """Add a watch request if it doesn't exist

        Args:
            watch_request: WatchRequest
                The watch_request to add
        """
        requester_id = watch_request.requester

        # Acquire the watch request lock before starting work
        with self.watch_request_lock:
            if watch_request in self.watch_requests.get(requester_id.global_id, []):
                log.debug3("Request already added")
                return

            # Create watch request for this kind/api_version. Use global id
            # as watch thread is already namespaced/global
            log.debug3("Adding action with key %s", requester_id.global_id)
            self.watch_requests.setdefault(requester_id.global_id, set()).add(
                watch_request
            )

    ## WatchRequest Functions  ###################################################

    def _gather_resource_requests(self, resource: ManagedObject) -> List[WatchRequest]:
        """Gather the list of actions that apply to this specific Kube event based on
        the ownerRefs and the resource itself.

        Args:
            resource: ManagedObject
                The resource for this event

        Returns:
            request_list: List[WatchRequest]
                The list of watch requests that apply
        """

        request_list = []

        # Acquire the watch request lock
        with self.watch_request_lock:
            # Check if the event resource can be reconciled directly like in the case of
            # Controllers
            resource_id = ResourceId.from_resource(resource)
            for request in self.watch_requests.get(resource_id.global_id, []):
                # Check if request has a specific name and if this event matches
                if request.requester.name and request.requester.name != resource.name:
                    continue

                unique_request = copy.deepcopy(request)
                if not unique_request.requester.name:
                    unique_request.requester = dataclasses.replace(
                        unique_request.requester, name=resource_id.name
                    )

                log.debug3(
                    "Gathering request for controller %s from %s",
                    unique_request.controller_type,
                    resource_id.global_id,
                )
                request_list.append(unique_request)

            # Check for any owners watching this resource
            for owner_ref in resource.metadata.get("ownerReferences", []):
                owner_id = ResourceId.from_owner_ref(
                    owner_ref, namespace=resource_id.namespace
                )

                if owner_id.global_id not in self.watch_requests:
                    log.debug3("Skipping event with owner_key: %s", owner_id.global_id)
                    continue

                for request in self.watch_requests.get(owner_id.global_id, []):
                    # If request has a specific name then ensure it matches
                    if (
                        request.requester.name
                        and request.requester.name != owner_ref.get("name")
                    ):
                        continue

                    # If request doesn't already have a name then force
                    # this resource. This allows multiple controllers with
                    # the same kind/api_version to own the same resource
                    unique_request = copy.deepcopy(request)
                    if not unique_request.requester.name:
                        unique_request.requester = dataclasses.replace(
                            unique_request.requester, name=owner_id.name
                        )

                    log.debug3(
                        "Gathering request for controller %s from %s",
                        unique_request.controller_type,
                        owner_ref,
                    )
                    request_list.append(unique_request)

        return request_list

    def _request_reconcile(self, event: KubeWatchEvent, request: WatchRequest):
        """Request a reconcile for a kube event

        Args:
            event: KubeWatchEvent
                The KubeWatchEvent that triggered the reconcile
            request: WatchRequest
                The object that's requested a reconcile
        """

        resource = event.resource
        event_type = event.type
        requester_id = request.requester

        # If the watch request is for a different object (e.g dependent watch) then
        # fetch the correct resource to reconcile
        if (
            requester_id.kind != event.resource.kind
            or requester_id.api_version != event.resource.api_version
            or (requester_id.name and requester_id.name != event.resource.name)
        ):
            success, obj = self.deploy_manager.get_object_current_state(
                kind=requester_id.kind,
                name=requester_id.name,
                namespace=event.resource.namespace,
                api_version=requester_id.api_version,
            )
            if not success or not obj:
                log.warning(
                    "Unable to fetch owner resource %s", requester_id.get_named_id()
                )
                return

            resource = ManagedObject(obj)
            event_type = ReconcileRequestType.DEPENDENT

        # Generate the request and push one for each watched action to the reconcile thread
        request = ReconcileRequest(request.controller_type, event_type, resource)
        self.reconcile_thread.push_request(request)

    ## Watched Resource Functions  ###################################################

    def _create_watched_resource(
        self,
        resource: ManagedObject,
        watch_requests: List[WatchRequest],
    ):
        """Create a WatchedResource and initialize it's filters

        Args:
            resource: ManagedObject
                The resource being watched
            watch_requests: List[WatchRequest]
                The list of requests that apply to this resource

        """
        # update the watched resources dict
        if resource.uid in self.watched_resources:
            return

        # Setup filter dict with global filters
        filter_dict = {None: FilterManager(get_configured_filter(), resource)}
        for request in watch_requests:
            filter_dict[request.requester.get_named_id()] = FilterManager(
                request.filters, resource
            )

        # Add watched resource to mapping
        self.watched_resources[resource.uid] = WatchedResource(
            gvk=ResourceId.from_resource(resource), filters=filter_dict
        )

    def _clean_event(self, event: KubeWatchEvent):
        """Call this function after processing every event to clean any leftover resources

        Args:
            event: KubeWatchEvent
                The kube event to clean up
        """
        if event.type == KubeEventType.DELETED:
            self.watched_resources.pop(event.resource.uid, None)

    ## Filter Functions  ###################################################

    def _check_filters(
        self,
        watch_requests: List[WatchRequest],
        resource: ManagedObject,
        event: KubeEventType,
    ) -> List[WatchRequest]:
        """Check a resource and event against both global and request specific filters

        Args:
            watch_requests: List[WatchRequest]
                List of watch requests whose filters should be checked
            resource: ManagedObject
                The resource being filtered
            event: KubeEventType
                THe event type being filtered

        Returns:
            successful_requests: List[WatchRequest]
                The list of requests that passed the filter

        """

        if resource.uid not in self.watched_resources:
            return []

        # If the default watched resource filter fails then no need to
        # check any watch requests
        watched_resource = self.watched_resources[resource.uid]
        if not watched_resource.filters[None].update_and_test(resource, event):
            return []

        output_requests = []

        # Check the watch requests for any of their filters
        for request in watch_requests:
            requester_id = request.requester.get_named_id()

            # If this is the first time this watched resource has seen this request then
            # initialize the filters
            if requester_id not in watched_resource.filters:
                watched_resource.filters[requester_id] = FilterManager(
                    request.filters, resource
                )

            if not watched_resource.filters[requester_id].update_and_test(
                resource, event
            ):
                continue

            output_requests.append(request)

        return output_requests


# Keep track of watch_threads globally, This is required to keep an accurate track of
# existing watches
watch_threads: Dict[str, WatchThread] = {}


def create_resource_watch(
    watch_request: WatchRequest,
    reconcile_thread: RECONCILE_THREAD_TYPE,
    deploy_manager: DeployManagerBase,
    leadership_manager: LeadershipManagerBase,
) -> WatchThread:
    """Create or request a watch for a resource. This function will either append the request to
    an existing thread or create a new one. This function will also start the thread if any
    other watch threads have already been started.

    Args:
        watch_request: WatchRequest
            The watch request to submit
        reconcile_thread: ReconcileThread
            The ReconcileThread to submit ReconcileRequests to
        deploy_manager: DeployManagerBase
            The DeployManager to use with the Thread
        leadership_manager: LeadershipManagerBase
            The LeadershipManager to use for election

    Returns:
        watch_thread: WatchThread
            The watch_thread that is watching the request
    """
    watch_thread = None
    watched_id = watch_request.watched

    # First check for a global watch before checking for a specific namespace watch
    if watched_id.global_id in watch_threads:
        log.debug2("Found existing global watch thread for %s", watch_request)
        watch_thread = watch_threads[watched_id.global_id]

    elif watched_id.namespace and watched_id.namespaced_id in watch_threads:
        log.debug2("Found existing namespaced watch thread for %s", watch_request)
        watch_thread = watch_threads[watched_id.namespaced_id]

    # Create a watch thread if it doesn't exist
    if not watch_thread:
        log.debug2("Creating new WatchThread for %s", watch_request)
        watch_thread = WatchThread(
            reconcile_thread,
            watched_id.kind,
            watched_id.api_version,
            watched_id.namespace,
            deploy_manager,
            leadership_manager,
        )

        watch_key = watched_id.get_id()
        watch_threads[watch_key] = watch_thread

        # Only start the watch thread if another is already watching
        for thread in watch_threads.values():
            if thread.is_alive():
                watch_thread.start_thread()
                break

    # Add action to controller
    watch_thread.request_watch(watch_request)
    return watch_thread


def get_resource_watches() -> List[WatchThread]:
    """Get the list of all watch_threads

    Returns:
        list_of_watches: List[WatchThread]
            List of watch threads
    """
    return watch_threads.values()
