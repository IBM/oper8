"""
Python-based implementation of the WatchManager
"""

# Standard
from typing import List, Optional, Type
import threading

# First Party
import alog

# Local
from ... import config
from ...controller import Controller
from ...deploy_manager import OpenshiftDeployManager
from ..base import WatchManagerBase
from .filters import get_filters_for_resource_id
from .leader_election import LeadershipManagerBase, get_leader_election_class
from .threads import HeartbeatThread, ReconcileThread, WatchThread
from .threads.watch import create_resource_watch, get_resource_watches
from .utils import ResourceId, WatchRequest

log = alog.use_channel("PYTHW")


class PythonWatchManager(WatchManagerBase):
    """The PythonWatchManager uses the kubernetes watch client to watch
    a particular Controller and execute reconciles. It does the following
    two things

    1. Request a generic watch request for each namespace
    2. Start a reconcile thread to start reconciliation subprocesses
    """

    def __init__(
        self,
        controller_type: Type[Controller],
        deploy_manager: Optional[OpenshiftDeployManager] = None,
        namespace_list: Optional[List[str]] = None,
    ):
        """Initialize the required threads and submit the watch requests
        Args:
            controller_type: Type[Controller]
                The controller to be watched
            deploy_manager: Optional[OpenshiftDeployManager] = None
                An optional DeployManager override
            namespace_list: Optional[List[str]] = []
                A list of namespaces to watch
        """
        super().__init__(controller_type)

        # Handle functional args
        if deploy_manager is None:
            log.debug("Using OpenshiftDeployManager")
            deploy_manager = OpenshiftDeployManager()
        self.deploy_manager = deploy_manager

        # Setup watch namespace
        self.namespace_list = namespace_list or []
        if not namespace_list and config.watch_namespace != "":
            self.namespace_list = config.watch_namespace.split(",")

        # Setup Control variables
        self.shutdown = threading.Event()

        # Setup Threads. These are both singleton instances and will be
        # the same across all PythonWatchManagers
        self.leadership_manager: LeadershipManagerBase = get_leader_election_class()(
            self.deploy_manager
        )
        self.reconcile_thread: ReconcileThread = ReconcileThread(
            deploy_manager=self.deploy_manager,
            leadership_manager=self.leadership_manager,
        )
        self.heartbeat_thread: Optional[HeartbeatThread] = None
        if config.python_watch_manager.heartbeat_file:
            self.heartbeat_thread = HeartbeatThread(
                config.python_watch_manager.heartbeat_file,
                config.python_watch_manager.heartbeat_period,
            )

        # Start thread for each resource watch
        self.controller_watches: List[WatchThread] = []
        if len(self.namespace_list) == 0 or "*" in self.namespace_list:
            self.controller_watches.append(self._add_resource_watch())
        else:
            for namespace in self.namespace_list:
                self.controller_watches.append(self._add_resource_watch(namespace))

    ## Interface ###############################################################

    def watch(self) -> bool:
        """Check for leadership and start all threads

        Returns:
            success:  bool
                True if all threads process are running correctly
        """
        log.info("Starting PythonWatchManager: %s", self)

        if not self.leadership_manager.is_leader():
            log.debug("Acquiring Leadership lock before starting %s", self)
            self.leadership_manager.acquire()

        # If watch has been shutdown then exit before starting threads
        if self.shutdown.is_set():
            return False

        # Start reconcile thread and all watch threads
        self.reconcile_thread.start_thread()
        for watch_thread in self.controller_watches:
            log.debug("Starting watch_thread: %s", watch_thread)
            watch_thread.start_thread()
        if self.heartbeat_thread:
            log.debug("Starting heartbeat_thread")
            self.heartbeat_thread.start_thread()
        return True

    def wait(self):
        """Wait shutdown to be signaled"""
        self.shutdown.wait()

    def stop(self):
        """Stop all threads. This waits for all reconciles
        to finish
        """

        log.info(
            "Stopping PythonWatchManager for %s/%s/%s",
            self.group,
            self.version,
            self.kind,
        )

        # Set shutdown and acquire leadership to clear any deadlocks
        self.shutdown.set()
        self.leadership_manager.acquire(force=True)

        # Stop all threads
        for watch in get_resource_watches():
            watch.stop_thread()
        self.reconcile_thread.stop_thread()
        self.leadership_manager.release()
        if self.heartbeat_thread:
            self.heartbeat_thread.stop_thread()

    ## Helper Functions ###############################################################

    def _add_resource_watch(self, namespace: Optional[str] = None):
        """Request a generic watch request. Optionally for a specific namespace

        Args:
            namespace: Optional[str] = None
                An optional namespace to watch
        """
        log.debug3("Adding %s request for %s", namespace if namespace else "", self)

        # In the global watch manager the controller is both
        # the watched and the requested objects
        resource_id = ResourceId.from_controller(self.controller_type, namespace)
        request = WatchRequest(
            controller_type=self.controller_type,
            watched=resource_id,
            requester=resource_id,
            filters=get_filters_for_resource_id(self.controller_type, resource_id),
        )
        return create_resource_watch(
            request,
            self.reconcile_thread,
            self.deploy_manager,
            self.leadership_manager,
        )
