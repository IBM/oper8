"""
This DeployManager is responsible for delegating cluster operations to the
openshift library. It is the one that will be used when the operator is running
in the cluster or outside the cluster making live changes.
"""
# Standard
from collections import namedtuple
from typing import Callable, Iterator, List, Optional, Tuple
import copy
import threading
import time

# Third Party
from kubernetes import client
from kubernetes.watch import Watch
from openshift.dynamic import DynamicClient
from openshift.dynamic.apply import LAST_APPLIED_CONFIG_ANNOTATION, recursive_diff
from openshift.dynamic.exceptions import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ResourceNotFoundError,
    ResourceNotUniqueError,
    UnprocessibleEntityError,
)
from openshift.dynamic.resource import Resource
import kubernetes
import urllib3

# First Party
from aconfig import Config
import alog

# Local
from .. import config
from .. import status as oper8_status
from ..exceptions import assert_cluster
from ..managed_object import ManagedObject
from ..verify_resources import verify_subsystem
from .base import DeployManagerBase, DeployMethod
from .kube_event import KubeEventType, KubeWatchEvent
from .owner_references import update_owner_references
from .replace_utils import requires_replace

log = alog.use_channel("OSFTD")

## Deploy Manager ##############################################################


# See this document for value reasonings
# https://github.com/kubernetes-client/python/blob/master/examples/watch/timeout-settings.md
SERVER_WATCH_TIMEOUT = 3600
CLIENT_WATCH_TIMEOUT = 30


class OpenshiftDeployManager(DeployManagerBase):
    """This DeployManager uses the openshift DynamicClient to interact with the
    cluster
    """

    def __init__(
        self,
        manage_ansible_status: bool = False,
        owner_cr: Optional[dict] = None,
    ):
        """
        Args:
            manage_ansible_status:  bool
                If true, oper8 will emulate the status management done natively
                by ansible based on the readiness values of oper8's native status
                management
            owner_cr:  Optional[dict]
                The dict content of the CR that triggered this reconciliation.
                If given, deployed objects will have an ownerReference added to
                assign ownership to this CR instance.
        """
        self.manage_ansible_status = manage_ansible_status
        self._owner_cr = owner_cr

        # Set up the client
        log.debug("Initializing openshift client")
        self._client = None

        # Keep a threading lock for performing status updates. This is necessary
        # to avoid running into 409 Conflict errors if concurrent threads are
        # trying to perform status updates
        self._status_lock = threading.Lock()

    @property
    def client(self):
        """Lazy property access to the client"""
        if self._client is None:
            self._client = self._setup_client()
        return self._client

    @alog.logged_function(log.debug)
    def deploy(
        self,
        resource_definitions: List[dict],
        manage_owner_references: bool = True,
        retry_operation: bool = True,
        method: DeployMethod = DeployMethod.DEFAULT,
        **_,  # Accept any kwargs to compatibility
    ) -> Tuple[bool, bool]:
        """Deploy using the openshift client

        Args:
            resource_definitions:  list(dict)
                List of resource object dicts to apply to the cluster
            manage_owner_references:  bool
                If true, ownerReferences for the parent CR will be applied to
                the deployed object

        Returns:
            success:  bool
                True if deploy succeeded, False otherwise
            changed:  bool
                Whether or not the deployment resulted in changes
        """
        return self._retried_operation(
            resource_definitions,
            self._apply,
            max_retries=config.deploy_retries if retry_operation else 0,
            manage_owner_references=manage_owner_references,
            method=method,
        )

    @alog.logged_function(log.debug)
    def disable(self, resource_definitions: List[dict]) -> Tuple[bool, bool]:
        """The disable process is the same as the deploy process, but the child
        module params are set to 'state: absent'

        Args:
            resource_definitions:  list(dict)
                List of resource object dicts to apply to the cluster

        Returns:
            success:  bool
                True if deploy succeeded, False otherwise
            changed:  bool
                Whether or not the delete resulted in changes
        """
        return self._retried_operation(
            resource_definitions,
            self._disable,
            max_retries=config.deploy_retries,
            manage_owner_references=False,
        )

    def get_object_current_state(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Tuple[bool, dict]:
        """The get_current_objects function fetches the current state using
        calls directly to the api client

        Args:
            kind:  str
                The kind of the object ot fetch
            name:  str
                The full name of the object to fetch
            namespace:  Optional[str]
                The namespace to search for the object or None for no namespace
            api_version:  Optional[str]
                The api_version of the resource kind to fetch

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            current_state:  dict or None
                The dict representation of the current object's configuration,
                or None if not present
        """

        # Use the lazy discovery tool to first get all objects of the given type
        # in the given namespace, then look for the specific resource by name
        resources = self._get_resource_handle(kind, api_version)
        if not resources:
            return True, None

        if not namespace:
            resources.namespaced = False

        try:
            resource = resources.get(name=name, namespace=namespace)
        except ForbiddenError:
            log.debug(
                "Fetching objects of kind [%s] forbidden in namespace [%s]",
                kind,
                namespace,
            )
            return False, None
        except NotFoundError:
            log.debug(
                "No object named [%s/%s] found in namespace [%s]", kind, name, namespace
            )
            return True, None

        # If the resource was found, return it's dict representation
        return True, resource.to_dict()

    def watch_objects(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        api_version: Optional[str] = None,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
        resource_version: Optional[str] = None,
        watch_manager: Optional[Watch] = None,
    ) -> Iterator[KubeWatchEvent]:
        watch_manager = watch_manager if watch_manager else Watch()
        resource_handle = self._get_resource_handle(kind, api_version)
        assert_cluster(
            resource_handle,
            (
                "Failed to fetch resource handle for "
                + f"{namespace}/{api_version}/{kind}"
            ),
        )

        resource_version = resource_version if resource_version else 0

        while True:
            try:
                for event_obj in watch_manager.stream(
                    resource_handle.get,
                    resource_version=resource_version,
                    namespace=namespace,
                    name=name,
                    label_selector=label_selector,
                    field_selector=field_selector,
                    serialize=False,
                    timeout_seconds=SERVER_WATCH_TIMEOUT,
                    _request_timeout=CLIENT_WATCH_TIMEOUT,
                ):
                    event_type = KubeEventType(event_obj["type"])
                    event_resource = ManagedObject(event_obj["object"])
                    yield KubeWatchEvent(event_type, event_resource)
            except client.exceptions.ApiException as exception:
                if exception.status == 410:
                    log.debug2(
                        f"Resource age expired, restarting watch {kind}/{api_version}"
                    )
                    resource_version = None
                else:
                    log.info("Unknown ApiException received, re-raising")
                    raise exception
            except urllib3.exceptions.ReadTimeoutError:
                log.debug4(
                    f"Watch Socket closed, restarting watch {kind}/{api_version}"
                )
            except urllib3.exceptions.ProtocolError:
                log.debug2(
                    f"Invalid Chunk from server, restarting watch {kind}/{api_version}"
                )

            # This is hidden attribute so probably not best to check
            if watch_manager._stop:  # pylint: disable=protected-access
                log.debug(
                    "Internal watch stopped. Stopping deploy manager watch for %s/%s",
                    kind,
                    api_version,
                )
                return

    def filter_objects_current_state(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        namespace: Optional[str] = None,
        api_version: Optional[str] = None,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
    ) -> Tuple[bool, List[dict]]:
        """The filter_objects_current_state function fetches a list of objects
        that match either/both the label or field selector
        Args:
            kind:  str
                The kind of the object to fetch
            namespace:  str
                The namespace to search for the object
            api_version:  str
                The api_version of the resource kind to fetch
            label_selector:  str
                The label_selector to filter the resources
            field_selector:  str
                The field_selector to filter the resources

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            current_state:  List[dict]
                A list of  dict representations for the objects configuration,
                or an empty list if no objects match
        """
        # Use the lazy discovery tool to first get all objects of the given type
        # in the given namespace, then look for the specific resource by name
        resources = self._get_resource_handle(kind, api_version)
        if not resources:
            return True, []

        if not namespace:
            resources.namespaced = False

        try:
            list_obj = resources.get(
                label_selector=label_selector,
                field_selector=field_selector,
                namespace=namespace,
            )
        except ForbiddenError:
            log.debug(
                "Fetching objects of kind [%s] forbidden in namespace [%s]",
                kind,
                namespace,
            )
            return False, []
        except NotFoundError:
            log.debug(
                "No objects of kind [%s] found in namespace [%s]", kind, namespace
            )
            return True, []

        # If the resource was found, get it's dict representation
        resource_list = list_obj.to_dict().get("items", [])
        return True, resource_list

    def set_status(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        name: str,
        namespace: Optional[str],
        status: dict,
        api_version: Optional[str] = None,
    ) -> Tuple[bool, bool]:
        """Set the status in the cluster manifest for an object managed by this
        operator

        Args:
            kind:  str
                The kind of the object ot fetch
            name:  str
                The full name of the object to fetch
            namespace:  Optional[str]
                The namespace to search for the object.
            status:  dict
                The status object to set onto the given object
            api_version:  Optional[str]
                The api_version of the resource to update

        Returns:
            success:  bool
                Whether or not the status update operation succeeded
            changed:  bool
                Whether or not the status update resulted in a change
        """
        # Create a dummy resource to use in the common retry function
        resource_definitions = [
            {
                "kind": kind,
                "apiVersion": api_version,
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                },
            }
        ]

        # Run it with retries
        return self._retried_operation(
            resource_definitions,
            self._set_status,
            max_retries=config.deploy_retries,
            status=status,
            manage_owner_references=False,
        )

    ## Implementation Helpers ##################################################

    @staticmethod
    def _setup_client():
        """Create a DynamicClient that will work based on where the operator is
        running
        """
        # Try in-cluster config
        try:
            log.debug2("Running with in-cluster config")

            # Create Empty Config and load in-cluster information
            kube_config = kubernetes.client.Configuration()
            kubernetes.config.load_incluster_config(client_configuration=kube_config)

            # Generate ApiClient and return Openshift DynamicClient
            api_client = kubernetes.client.ApiClient(kube_config)
            return DynamicClient(api_client)

        # Fall back to out-of-cluster config
        except kubernetes.config.ConfigException:
            log.debug2("Running with out-of-cluster config")
            return DynamicClient(kubernetes.config.new_client_from_config())

    @staticmethod
    def _strip_last_applied(resource_definitions):
        """Make sure that the last-applied annotation is not present in any of
        the resources. This can lead to recursive nesting!
        """
        for resource_definition in resource_definitions:
            last_applied = (
                resource_definition.get("metadata", {})
                .get("annotations", {})
                .get(LAST_APPLIED_CONFIG_ANNOTATION)
            )
            if last_applied:
                log.debug3("Removing [%s]", LAST_APPLIED_CONFIG_ANNOTATION)
                del resource_definition["metadata"]["annotations"][
                    LAST_APPLIED_CONFIG_ANNOTATION
                ]
                if not resource_definition["metadata"]["annotations"]:
                    del resource_definition["metadata"]["annotations"]

    def _get_resource_handle(self, kind: str, api_version: str) -> Optional[Resource]:
        """Get the openshift resource handle for a specified kind and api_version"""
        resources = None
        try:
            resources = self.client.resources.get(kind=kind, api_version=api_version)
        except (ResourceNotFoundError, ResourceNotUniqueError):
            try:
                resources = self.client.resources.get(
                    short_names=[kind], api_version=api_version
                )
            except (ResourceNotFoundError, ResourceNotUniqueError):
                log.debug(
                    "No objects of kind [%s] found or multiple objects matching request found",
                    kind,
                )
        return resources

    def _update_owner_references(self, resource_definitions):
        """If configured to do so, add owner references to the given resources"""
        if self._owner_cr:
            for resource_definition in resource_definitions:
                update_owner_references(self, self._owner_cr, resource_definition)

    def _retried_operation(
        self,
        resource_definitions,
        operation,
        max_retries,
        manage_owner_references,
        **kwargs,
    ):
        """Shared wrapper for executing a client operation with retries"""

        # Make sure the resource_definitions is a list
        assert isinstance(
            resource_definitions, list
        ), "Programming Error: resource_definitions is not a list"
        log.debug3("Running module with %d retries", max_retries)

        # If there are no resource definitions given, consider it a success with
        # no change
        if not resource_definitions:
            log.debug("Nothing to do for an empty list of resources")
            return True, False

        # Strip out last-applied annotations from all resources to avoid nested
        # annotations
        self._strip_last_applied(resource_definitions)

        # Add owner references if configured to do so
        if manage_owner_references:
            self._update_owner_references(resource_definitions)

        # Run each resource individually so that we can track partial completion
        success = True
        changed = False
        for resource_definition in resource_definitions:
            # Perform the operation and update the aggregate changed status
            try:
                changed = (
                    self._run_individual_operation_with_retries(
                        operation,
                        max_retries,
                        resource_definition=resource_definition,
                        **kwargs,
                    )
                    or changed
                )

            # On failure, mark it and stop processing the rest of the resources.
            # This is done because the resources in the file are assumed to be
            # in an intentional sequence and resources later in the file may
            # depend on resources earlier in the file.
            except Exception as err:  # pylint: disable=broad-except
                log.warning(
                    "Operation [%s] failed to execute: %s",
                    operation,
                    err,
                    exc_info=True,
                )
                success = False
                break

        # Return the aggregate success and change values
        return success, changed

    def _run_individual_operation_with_retries(
        self,
        operation: Callable,
        remaining_retries: int,
        resource_definition: dict,
        **kwargs,
    ):
        """Helper to execute a single helper operation with retries

        Args:
            operation:  Callable
                The operation function to run
            remaining_retries:  int
                The number of remaining retries
            resource_definition:  dict
                The dict representation of the resource being applied
            **kwargs:  dict
                Keyword args to pass to the operation beyond resource_definition

        Returns:
            changed:  bool
                Whether or not the operation resulted in meaningful change
        """
        try:
            return operation(resource_definition=resource_definition, **kwargs)
        except ConflictError as err:
            log.debug2("Handling ConflictError: %s", err)

            # If we have retries left, try again
            if remaining_retries:
                # Sleep for the backoff duration
                backoff_duration = config.retry_backoff_base_seconds * (
                    config.deploy_retries - remaining_retries + 1
                )
                log.debug3("Retrying in %fs", backoff_duration)
                time.sleep(backoff_duration)

                # Fetch the current resourceVersion and update in the
                # resource definition
                # NOTE: This can overwrite changes made external to the operator
                #   but that's an acceptable case since resources managed by
                #   oper8 should only be managed by oper8. In the rare case where
                #   oper8 shares ownership of a resource, any conflicts should
                #   be resoled cleanly on the next reconciliation.
                res_id = self._get_resource_identifiers(resource_definition)
                api_version = res_id.api_version
                kind = res_id.kind
                name = res_id.name
                namespace = res_id.namespace
                success, content = self.get_object_current_state(
                    kind=kind,
                    name=name,
                    namespace=namespace,
                    api_version=api_version,
                )
                assert_cluster(
                    success and content is not None,
                    (
                        "Failed to fetch updated resourceVersion for "
                        + f"{namespace}/{api_version}/{kind}/{name}"
                    ),
                )
                updated_resource_version = content.get("metadata", {}).get(
                    "resourceVersion"
                )
                assert_cluster(
                    updated_resource_version is not None,
                    "No updated resource version found!",
                )
                log.debug3(
                    "Updating resourceVersion from %s -> %s",
                    resource_definition.get("metadata", {}).get("resourceVersion"),
                    updated_resource_version,
                )
                resource_definition.setdefault("metadata", Config({}))[
                    "resourceVersion"
                ] = updated_resource_version

                # Run the retry
                log.debug3("Retrying")
                return self._run_individual_operation_with_retries(
                    operation, remaining_retries - 1, resource_definition, **kwargs
                )
            raise

    _ANSIBLE_COND_TYPE = "Running"
    _ANSIBLE_COND_RES_READY = {"ok": 1, "changed": 0, "skipped": 0, "failures": 0}
    _ANSIBLE_COND_RES_UNREADY = {"ok": 0, "changed": 0, "skipped": 0, "failures": 0}

    def _inject_ansible_status(self, status, previous_status):
        """If manage_ansible_status is enabled, this will inject the right
        ansible status values to emulate the format that ansible natively
        supports
        """
        previous_status = previous_status or {}

        # Check if the oper8 status indicates readiness
        is_ready = verify_subsystem(
            {"status": status}, desired_version=oper8_status.get_version(status)
        )
        prev_is_ready = verify_subsystem(
            {"status": previous_status},
            desired_version=oper8_status.get_version(previous_status),
        )
        log.debug3(
            "Status shows ready? %s. Previous ready? %s", is_ready, prev_is_ready
        )

        # Create the ansible status blob
        ansible_result = (
            self._ANSIBLE_COND_RES_READY if is_ready else self._ANSIBLE_COND_RES_UNREADY
        )
        log.debug3("Ansible Result: %s", ansible_result)

        # Determine if the condition has changed to know whether this is a
        # transition time
        current_ready_timestamp = oper8_status.get_condition(
            oper8_status.READY_CONDITION, status
        ).get(oper8_status.TIMESTAMP_KEY)
        prev_ready_timestamp = oper8_status.get_condition(
            oper8_status.READY_CONDITION, previous_status
        ).get(oper8_status.TIMESTAMP_KEY)
        if prev_ready_timestamp is not None and prev_is_ready == is_ready:
            log.debug3("No readiness change. Not a transition.")
            transition_time = prev_ready_timestamp
        else:
            log.debug3(
                "Transitioning from Ready(%s) -> Ready(%s)", prev_is_ready, is_ready
            )
            transition_time = current_ready_timestamp

        # Inject the final ansible condition
        conditions = [
            cond
            for cond in status.get("conditions", [])
            if cond.get("type") != self._ANSIBLE_COND_TYPE
        ]
        conditions.append(
            {
                "type": self._ANSIBLE_COND_TYPE,
                "ansibleResult": ansible_result,
                "lastTransitionTime": transition_time,
            }
        )
        status["conditions"] = conditions
        log.debug4("Status With Ansible: %s", status)

    @classmethod
    def _clean_manifest(cls, manifest_a: dict, manifest_b: dict) -> Tuple[dict, dict]:
        """Clean two manifests before being compared. This removes fields that
        change every reconcile

        Returns:
            Tuple[dict, dict]: The cleaned manifests
        """
        manifest_a = copy.deepcopy(manifest_a)
        manifest_b = copy.deepcopy(manifest_b)
        for metadata_field in [
            "resourceVersion",
            "generation",
            "managedFields",
            "uid",
            "creationTimestamp",
        ]:
            manifest_a.get("metadata", {}).pop(metadata_field, None)
            manifest_b.get("metadata", {}).pop(metadata_field, None)
        return (manifest_a, manifest_b)

    @classmethod
    def _manifest_diff(cls, manifest_a, manifest_b) -> bool:
        """Helper to compare two manifests for meaningful diff while ignoring
        fields that always change.

        Returns:
            [bool, bool]: The first bool identifies if the resource changed while the
        """

        manifest_a, manifest_b = cls._clean_manifest(manifest_a, manifest_b)

        cls._strip_last_applied([manifest_b, manifest_a])
        diff = recursive_diff(
            manifest_a,
            manifest_b,
        )
        change = bool(diff)
        log.debug2("Found change? %s", change)
        log.debug3("A: %s", manifest_a)
        log.debug3("B: %s", manifest_b)
        return change

    @classmethod
    def _retain_kubernetes_annotations(cls, current: dict, desired: dict) -> dict:
        """Helper to update a desired manifest with certain annotations from the existing
        resource. This stops other controllers from re-reconciling this resource

        Returns:
            dict: updated resource
        """

        identifiers = cls._get_resource_identifiers(desired)

        for annotation, annotation_value in (
            current.get("metadata", {}).get("annotations", {}).items()
        ):
            for cluster_annotation in config.cluster_passthrough_annotations:
                if cluster_annotation in annotation and annotation not in desired[
                    "metadata"
                ].get("annotations", {}):
                    log.debug4(
                        "Retaining annotation %s for [%s/%s/%s]",
                        annotation,
                        identifiers.kind,
                        identifiers.api_version,
                        identifiers.name,
                    )
                    desired["metadata"].setdefault("annotations", {})[
                        annotation
                    ] = annotation_value
        return desired

    @classmethod
    def _requires_replace(cls, manifest_a, manifest_b) -> bool:
        """Helper to compare two manifests to see if they require
        replace

        Returns:
            bool: If the resource requires a replace operation
        """

        manifest_a, manifest_b = cls._clean_manifest(manifest_a, manifest_b)

        change = bool(requires_replace(manifest_a, manifest_b))
        log.debug2("Requires Replace? %s", change)
        return change

    # Internal struct to hold the key resource identifier elements
    _ResourceIdentifiers = namedtuple(
        "ResourceIdentifiers", ["api_version", "kind", "name", "namespace"]
    )

    @classmethod
    def _get_resource_identifiers(cls, resource_definition, require_api_version=True):
        """Helper for getting the required parts of a single resource definition"""
        api_version = resource_definition.get("apiVersion")
        kind = resource_definition.get("kind")
        name = resource_definition.get("metadata", {}).get("name")
        namespace = resource_definition.get("metadata", {}).get("namespace")
        assert None not in [
            kind,
            name,
        ], "Cannot apply resource without kind or name"
        assert (
            not require_api_version or api_version is not None
        ), "Cannot apply resource without apiVersion"
        return cls._ResourceIdentifiers(api_version, kind, name, namespace)

    ################
    ## Operations ##
    ################

    def _replace_resource(self, resource_definition: dict) -> dict:
        """Helper function to forcibly replace a resource on the cluster"""
        # Get the key elements of the resource
        res_id = self._get_resource_identifiers(resource_definition)
        api_version = res_id.api_version
        kind = res_id.kind
        name = res_id.name
        namespace = res_id.namespace

        # Strip out managedFields to let the sever set them
        resource_definition["metadata"]["managedFields"] = None

        # Get the resource handle
        log.debug2("Fetching resource handle [%s/%s]", api_version, kind)
        resource_handle = self._get_resource_handle(api_version=api_version, kind=kind)
        assert_cluster(
            resource_handle,
            (
                "Failed to fetch resource handle for "
                + f"{namespace}/{api_version}/{kind}"
            ),
        )

        log.debug2(
            "Attempting to put [%s/%s/%s] in %s",
            api_version,
            kind,
            name,
            namespace,
        )
        return resource_handle.replace(
            resource_definition,
            name=name,
            namespace=namespace,
            field_manager="oper8",
        ).to_dict()

    def _apply_resource(self, resource_definition: dict) -> dict:
        """Helper function to apply a single resource to the cluster"""
        # Get the key elements of the resource
        res_id = self._get_resource_identifiers(resource_definition)
        api_version = res_id.api_version
        kind = res_id.kind
        name = res_id.name
        namespace = res_id.namespace

        # Strip out managedFields to let the sever set them
        resource_definition["metadata"]["managedFields"] = None

        # Get the resource handle
        log.debug2("Fetching resource handle [%s/%s]", api_version, kind)
        resource_handle = self._get_resource_handle(api_version=api_version, kind=kind)
        assert_cluster(
            resource_handle,
            (
                "Failed to fetch resource handle for "
                + f"{namespace}/{api_version}/{kind}"
            ),
        )

        log.debug2(
            "Attempting to apply [%s/%s/%s] in %s",
            api_version,
            kind,
            name,
            namespace,
        )
        try:
            return resource_handle.server_side_apply(
                resource_definition,
                name=name,
                namespace=namespace,
                field_manager="oper8",
            ).to_dict()
        except ConflictError:
            log.debug(
                "Overriding field manager conflict for [%s/%s/%s] in %s ",
                api_version,
                kind,
                name,
                namespace,
            )
            return resource_handle.server_side_apply(
                resource_definition,
                name=name,
                namespace=namespace,
                field_manager="oper8",
                force_conflicts=True,
            ).to_dict()

    def _apply(self, resource_definition, method: DeployMethod):
        """Apply a single resource to the cluster

        Args:
            resource_definition:  dict
                The resource manifest to apply

        Returns:
            changed:  bool
                Whether or not the apply resulted in a meaningful change
        """
        changed = False

        res_id = self._get_resource_identifiers(resource_definition)
        api_version = res_id.api_version
        kind = res_id.kind
        name = res_id.name
        namespace = res_id.namespace

        # Get the current resource state
        success, current = self.get_object_current_state(
            kind=kind,
            name=name,
            namespace=namespace,
            api_version=api_version,
        )
        assert_cluster(
            success,
            (
                "Failed to fetch current state for "
                + f"{namespace}/{api_version}/{kind}/{name}"
            ),
        )
        if not current:
            current = {}

        # Determine if there will be a meaningful change (ignoring fields that
        # always change)
        changed = self._manifest_diff(current, resource_definition)

        # If there is meaningful change, apply this instance
        if changed:

            resource_definition = self._retain_kubernetes_annotations(
                current, resource_definition
            )

            req_replace = False
            if method is DeployMethod.DEFAULT:
                req_replace = self._requires_replace(current, resource_definition)

            log.debug2(
                "Attempting to deploy [%s/%s/%s] in %s with %s",
                api_version,
                kind,
                name,
                namespace,
                method,
            )
            # If the resource requires a replace operation then use put. Otherwise use
            # server side apply
            if (
                (req_replace or method is DeployMethod.REPLACE)
                and method != DeployMethod.UPDATE
                and current != {}
            ):
                apply_res = self._replace_resource(
                    resource_definition,
                )
            else:
                try:
                    apply_res = self._apply_resource(resource_definition)
                except UnprocessibleEntityError as err:
                    log.debug3("Caught 422 error: %s", err, exc_info=True)
                    if config.deploy_unprocessable_put_fallback:
                        log.debug("Falling back to PUT on 422: %s", err)
                        apply_res = self._replace_resource(
                            resource_definition,
                        )
                    else:
                        raise

            # Recompute the diff to determine if the apply actually caused a
            # meaningful change. This may have a different result than the check
            # above because the applied manifest does not always result in the
            # resource looking identical (e.g. removing field from applied =
            # manifest does not delete from the resource).
            changed = self._manifest_diff(current, apply_res)

        return changed

    def _disable(self, resource_definition):
        """Disable a single resource to the cluster if it exists

        Args:
            resource_definition:  dict
                The resource manifest to disable

        Returns:
            changed:  bool
                Whether or not the disable resulted in a meaningful change
        """
        changed = False

        # Get the key elements of the resource
        res_id = self._get_resource_identifiers(resource_definition)
        api_version = res_id.api_version
        kind = res_id.kind
        name = res_id.name
        namespace = res_id.namespace

        # Get the resource handle, handling missing kinds as success without
        # change
        log.debug2("Fetching resource [%s/%s]", api_version, kind)
        try:
            # Get a handle to the kind. This may fail with ResourceNotFoundError
            resource_handle = self.client.resources.get(
                api_version=api_version, kind=kind
            )

            # If resource is not namespaced set kubernetes api namespaced to false
            if not namespace:
                resource_handle.namespaced = False

            # Attempt to delete this instance. This ay fail with NotFoundError
            log.debug2(
                "Attempting to delete [%s/%s/%s] from %s",
                api_version,
                kind,
                name,
                namespace,
            )
            resource_handle.delete(name=name, namespace=namespace)
            changed = True

        # If the kind or instance is not found, that's a success without change
        except (ResourceNotFoundError, NotFoundError) as err:
            log.debug2("Valid error caught when disabling [%s/%s]: %s", kind, name, err)

        return changed

    def _set_status(self, resource_definition, status):
        """Disable a single resource to the cluster if it exists

        Args:
            resource_definition:  dict
                A dummy manifest holding the resource identifiers
            status:  dict
                The status to apply

        Returns:
            changed:  bool
                Whether or not the status update resulted in a meaningful change
        """
        changed = False

        # Get the key elements of the resource
        res_id = self._get_resource_identifiers(
            resource_definition, require_api_version=False
        )
        api_version = res_id.api_version
        kind = res_id.kind
        name = res_id.name
        namespace = res_id.namespace
        resource_handle = self.client.resources.get(api_version=api_version, kind=kind)

        # If resource is not namespaced set kubernetes api namespaced to false
        if not namespace:
            resource_handle.namespaced = False

        with self._status_lock:
            # Get the resource itself
            resource = resource_handle.get(name=name, namespace=namespace).to_dict()

            # Get the previous status and compare with the proposed status
            log.debug2(
                "Resource version: %s",
                resource.get("metadata", {}).get("resourceVersion"),
            )
            previous_status = resource.get("status")
            if previous_status == status:
                log.debug("Status has not changed. No update")

            else:
                # Inject the ansible status if enabled
                if self.manage_ansible_status:
                    log.debug2("Injecting ansible status")
                    self._inject_ansible_status(status, previous_status)

                # Overwrite the status
                resource["status"] = status
                resource_handle.status.replace(body=resource).to_dict()
                log.debug2(
                    "Successfully set the status for [%s/%s] in %s",
                    kind,
                    name,
                    namespace,
                )
                changed = True

            return changed
