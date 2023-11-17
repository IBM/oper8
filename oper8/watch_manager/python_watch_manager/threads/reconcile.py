"""
The ReconcileThread is the heart of the PythonWatchManager and controls reconciling
resources and managing any subprocesses
"""
# Standard
from datetime import datetime
from logging.handlers import QueueListener
from multiprocessing.connection import Connection
from time import sleep
from typing import Dict, List, Optional
import multiprocessing
import os
import queue
import threading
import time

# First Party
import alog

# Local
from .... import config
from ....deploy_manager import DeployManagerBase, KubeEventType
from ....exceptions import ConfigError
from ....reconcile import ReconciliationResult
from ..filters import FilterManager
from ..leader_election import LeadershipManagerBase
from ..reconcile_process_entrypoint import create_and_start_entrypoint
from ..utils import (
    JOIN_PROCESS_TIMEOUT,
    SHUTDOWN_RECONCILE_POLL_TIME,
    ReconcileProcess,
    ReconcileRequest,
    ReconcileRequestType,
    Singleton,
    TimerEvent,
    WatchRequest,
    get_logging_handlers,
    parse_time_delta,
)
from .base import ThreadBase
from .timer import TimerThread
from .watch import create_resource_watch

log = alog.use_channel("RCLTHRD")


class ReconcileThread(
    ThreadBase, metaclass=Singleton
):  # pylint: disable=too-many-instance-attributes
    """This class is the core reconciliation class that handles starting subprocesses,
    tracking their status, and handles any results. This thread also kicks of requeue
    requests and requests dependent resource watches"""

    def __init__(
        self,
        deploy_manager: DeployManagerBase = None,
        leadership_manager: LeadershipManagerBase = None,
    ):
        """Initialize the required queues, helper threads, and reconcile tracking. Also
        gather any onetime configuration options

        Args:
            deploy_manager: DeployManagerBase = None
                The deploy manager used throughout the thread
            leadership_manager: LeadershipManagerBase = None
                The leadership_manager for tracking elections
        """
        super().__init__(
            name="reconcile_thread",
            deploy_manager=deploy_manager,
            leadership_manager=leadership_manager,
        )

        # Setup required queues
        self.request_queue = multiprocessing.Queue()
        self.logging_queue = multiprocessing.Queue()

        # Setup helper threads
        self.timer_thread: TimerThread = TimerThread()
        self.log_listener_thread: QueueListener = QueueListener(
            self.logging_queue, *get_logging_handlers(), respect_handler_level=False
        )

        # Setup reconcile, request, and event mappings
        self.running_reconciles: Dict[str, ReconcileProcess] = {}
        self.pending_reconciles: Dict[str, ReconcileRequest] = {}
        self.event_map: Dict[str, TimerEvent] = {}

        # Setup control variables
        self.process_overload = threading.Event()

        # Configure the multiprocessing process spawning context
        context = config.python_watch_manager.process_context
        if context not in multiprocessing.get_all_start_methods():
            raise ConfigError(f"Invalid process_context: '{context}'")

        self.spawn_ctx = multiprocessing.get_context(context)

        # Configure the max number of concurrent reconciles via either config
        # or number of cpus
        if config.python_watch_manager.max_concurrent_reconciles:
            self.max_concurrent_reconciles = (
                config.python_watch_manager.max_concurrent_reconciles
            )
        else:
            self.max_concurrent_reconciles = os.cpu_count()

    def run(self):
        """The reconcile threads control flow is to first wait for
        either a new reconcile request or for a process to end. If its a reconcile request
        the thread checks if one is already running for the resource and if not starts a
        new one. If a reconcile is already running or the thread couldn't start a new one
        the request gets added to the pending reconciles. There can only be one pending
        reconcile per resource. If the reconcile thread received a process end event it
        checks the exit code and handles the result queue.
        """
        while True:  # pylint: disable=too-many-nested-blocks
            if not self.check_preconditions():
                return

            # Wait for a change with the reconcile processes or reconcile queue.
            # Use _reader and Process.sentinel objects, so we can utilize select.select
            listen_objs = [
                *self.running_reconciles.values(),
                self.request_queue._reader,  # pylint: disable=protected-access
            ]
            log.debug3("Waiting on %s", listen_objs)
            ready_objs = multiprocessing.connection.wait(listen_objs)

            # Check preconditions both before and after waiting
            if not self.check_preconditions():
                return

            # For each object that triggered the connection
            for obj in ready_objs:
                log.debug3("Processing object %s with type %s", obj, type(obj))

                # Handle reconcile process end events
                if isinstance(obj, ReconcileProcess):
                    if self._handle_process_end(obj):
                        # If process overload is set than we need to check all resources for
                        # pending reconciles otherwise just check if the completed resource
                        # has a pending request.
                        if self.process_overload.is_set():
                            for uid in list(self.pending_reconciles.keys()):
                                if not self._handle_pending_reconcile(uid):
                                    break
                        else:
                            self._handle_pending_reconcile(obj.uid())

                # Handle all of the events in the queue
                elif isinstance(obj, Connection):
                    self._handle_request_queue()

    ## Class Interface ###################################################

    def start_thread(self):
        """Override start_thread to start helper threads"""
        self.timer_thread.start_thread()
        self.log_listener_thread.start()
        super().start_thread()

    def stop_thread(self):
        """Override stop_thread to ensure reconciles finish correctly"""
        super().stop_thread()

        if not self.is_alive() and not self.running_reconciles:
            log.debug("Reconcile Thread already stopped")
            return

        # Reawaken reconcile thread to stop
        log.debug("Pushing stop reconcile request")
        self.push_request(ReconcileRequest(None, ReconcileRequestType.STOPPED, {}))

        log.debug("Waiting for reconcile thread to finish")
        while self.is_alive():
            time.sleep(0.001)

        # Wait until all reconciles have completed
        log.info("Waiting for Running Reconciles to end")
        while self.running_reconciles:
            log.debug2("Waiting for reconciles %s to end", self.running_reconciles)
            for reconcile_process in list(self.running_reconciles.values()):
                # attempt to join process before trying the next one
                reconcile_process.process.join(JOIN_PROCESS_TIMEOUT)
                if reconcile_process.process.exitcode is not None:
                    log.debug(
                        "Joined reconciles process %s with exitcode: %s for request object %s",
                        reconcile_process.process.pid,
                        reconcile_process.process.exitcode,
                        reconcile_process.request,
                    )
                    self.running_reconciles.pop(reconcile_process.uid())
                    reconcile_process.process.close()

            # Pause for slight delay between checking processes
            sleep(SHUTDOWN_RECONCILE_POLL_TIME)

        # Close the logging queue to indicate no more logging events
        self.logging_queue.close()
        # Skip stopping the listener thread as it can hang on the join, this isn't
        # too bad as the listener thread is daemon anyways
        # self.log_listener_thread.stop()

    ## Public Interface ###################################################

    def push_request(self, request: ReconcileRequest):
        """Push request to reconcile queue

        Args:
            request: ReconcileRequest
                the ReconcileRequest to add to the queue
        """
        log.info(
            "Pushing request '%s' to reconcile queue",
            request,
            extra={"resource": request.resource},
        )
        self.request_queue.put(request)

    ## Event Handlers ###################################################

    def _handle_request_queue(self):
        """The function attempts to start a reconcile for every reconcile requests in the queue.
        If it can't start a reconcile or one is already running then it pushes it to the pending
        queue"""

        # Get all events from the queue
        pending_requests = self._get_all_requests()

        # Start a reconcile for each pending request
        for request in pending_requests:
            if request.type == ReconcileRequestType.STOPPED:
                break

            log.debug3("Got request %s from queue", request)

            # If reconcile is not running then start the process. Otherwise
            # or if starting failed push to the pending reconcile queue
            if request.resource.uid not in self.running_reconciles:
                if not self._start_reconcile_for_request(request):
                    self._push_to_pending_reconcile(request)
            else:
                self._push_to_pending_reconcile(request)

    def _handle_process_end(self, reconcile_process: ReconcileProcess) -> str:
        """Handle a process end event. The function joins the finished process,
        manages any events in the pipe, and creates a requeue/periodic event if
        one is needed.

        Args:
            reconcile_process: ReconcileProcess
                The process that ended

        Returns:
            uid: str
                The uid of the resource that ended
        """
        # Parse process variables
        uid = reconcile_process.uid()
        reconcile_request = reconcile_process.request
        process = reconcile_process.process
        pipe = reconcile_process.pipe

        # Attempt to join the process
        log.debug(
            "Joining process for request %s",
            reconcile_request,
            extra={"resource": reconcile_request.resource},
        )
        process.join(JOIN_PROCESS_TIMEOUT)
        exit_code = process.exitcode

        # If its still then exit and process   will be cleaned up on next iteration
        if exit_code is None:
            log.debug("Process is still alive after join. Skipping cleanup")
            return None

        if exit_code != 0:
            log.warning(
                "Reconcile did not complete successfully: %s",
                reconcile_request,
                extra={"resource": reconcile_request.resource},
            )

        # Remove reconcile from map and release resource lock
        self.running_reconciles.pop(uid)
        self.leadership_manager.release_resource(reconcile_request.resource)

        # Handle any events passed via the process pipe including the reconcile result and
        # close the pipe once done
        reconcile_result = self._handle_process_pipe(pipe)
        process.close()

        # Print reconciliation result
        log.info(
            "Reconcile completed with result %s",
            reconcile_result if reconcile_result else exit_code,
            extra={"resource": reconcile_request.resource},
        )

        # Cancel any existing requeue events
        if uid in self.event_map:
            log.debug2("Marking event as stale: %s", self.event_map[uid])
            self.event_map[uid].cancel()

        # Create a new timer event if one is needed
        event = self._create_timer_event_for_request(
            reconcile_request, reconcile_result
        )
        if event:
            self.event_map[uid] = event

        return uid

    def _handle_process_pipe(self, pipe: Connection) -> ReconciliationResult:
        """Handle any objects in a connection pipe and return the reconciliation result

        Args:
            pipe: Connection
                the pipe to read results from

        Returns:
            reconcile_result: ReconciliationResult
                The result gathered from the pipe
        """
        reconcile_result = None
        while pipe.poll():
            # EOFError is raised when the pipe is closed which is expected after the reconcile
            # process has been joined
            try:
                pipe_obj = pipe.recv()
            except EOFError:
                break

            log.debug3("Received obj %s from process pipe", pipe_obj)

            # Handle any watch requests received
            if isinstance(pipe_obj, WatchRequest):
                self._handle_watch_request(pipe_obj)

            # We only expect one reconciliation result per process
            elif isinstance(pipe_obj, ReconciliationResult):
                reconcile_result = pipe_obj

        # Close the reconcile pipe and release the rest of the process resources
        pipe.close()

        return reconcile_result

    def _handle_watch_request(self, request: WatchRequest):
        """Create a resource watch for a given watch request. This function also
        handles converting controller_info into a valid controller_type

        Args:
            request: WatchRequest
                The requested WatchRequest to be created
        """
        # Parse the controller info into a type
        if request.controller_info and not request.controller_type:
            request.controller_type = request.controller_info.to_class()

        # Parse any filter infos into types
        if request.filters_info:
            request.filters = FilterManager.from_info(request.filters_info)

        create_resource_watch(
            request,
            self,
            self.deploy_manager,
            self.leadership_manager,
        )

    def _create_timer_event_for_request(
        self, request: ReconcileRequest, result: ReconciliationResult = None
    ) -> Optional[TimerEvent]:
        """Enqueue either a requeue or periodic reconcile request for a given
        result.

        Args:
            request: ReconcileRequest
                The original reconcile request that triggered this process
            result: ReconciliationResult = None
                The result of the reconcile

        Returns:
            timer_event: Optional[TimerEvent]
                The timer event if one was created
        """

        # Short circuit if event is not needed, if resource was deleted,
        # or if theres already a pending reconcile
        if (
            not result or not result.requeue
        ) and not config.python_watch_manager.reconcile_period:
            return None

        if result and not result.requeue and request.type == KubeEventType.DELETED:
            return None

        if request.resource.uid in self.pending_reconciles:
            return None

        # Create requeue_time and type based on result/config
        request_type = None
        requeue_time = None
        if result and result.requeue:
            requeue_time = datetime.now() + result.requeue_params.requeue_after
            request_type = ReconcileRequestType.REQUEUED
        elif config.python_watch_manager.reconcile_period:
            requeue_time = datetime.now() + parse_time_delta(
                config.python_watch_manager.reconcile_period
            )
            request_type = ReconcileRequestType.PERIODIC

        future_request = ReconcileRequest(
            request.controller_type, request_type, request.resource
        )
        log.debug3("Pushing requeue request to timer: %s", future_request)

        return self.timer_thread.put_event(
            requeue_time, self.push_request, future_request
        )

    ## Pending Event Helpers ###################################################

    def _handle_pending_reconcile(self, uid: str) -> bool:
        """Start reconcile for pending request if there is one

        Args:
             uid:str
                The uid of the resource being reconciled

        Returns:
            successful_start:bool
                If there was a pending reconcile that got started"""
        # Check if resource has pending request
        if uid in self.running_reconciles or uid not in self.pending_reconciles:
            return False

        # Start reconcile for request
        request = self.pending_reconciles[uid]
        log.debug4("Got request %s from pending reconciles", request)
        if self._start_reconcile_for_request(request):
            self.pending_reconciles.pop(uid)
            return True
        return False

    def _push_to_pending_reconcile(self, request: ReconcileRequest):
        """Push a request to the pending queue if it's newer than the current event

        Args:
            request:  ReconcileRequest
                The request to possibly add to the pending_reconciles
        """
        uid = request.uid()
        # Only update queue if request is newer
        if uid in self.pending_reconciles:
            if request.timestamp > self.pending_reconciles[uid].timestamp:
                log.debug3("Updating reconcile queue with event %s", request)
                self.pending_reconciles[uid] = request
            else:
                log.debug4("Event in queue is newer than event %s", request)
        else:
            log.debug3("Adding event %s to reconcile queue", request)
            self.pending_reconciles[uid] = request

    ## Process functions ##################################################

    def _start_reconcile_for_request(self, request: ReconcileRequest) -> bool:
        """Start a reconciliation process for a given request

        Args:
            request: ReconcileRequest
                The request to attempt to start

        Returns:
            successfully_started: bool
                If a process could be started
        """
        # If thread is supposed to shutdown then don't start process
        if self.should_stop():
            return False

        # Check if there are too many reconciles running
        if len(self.running_reconciles.keys()) >= self.max_concurrent_reconciles:
            log.warning("Unable to start reconcile, max concurrent jobs reached")
            self.process_overload.set()
            return False

        # Attempt to acquire lock on resource. If failed skip starting
        if not self.leadership_manager.acquire_resource(request.resource):
            log.debug("Unable to obtain leadership lock for %s", request)
            return False

        self.process_overload.clear()
        log.info(
            "Starting reconcile for request %s",
            request,
            extra={"resource": request.resource},
        )

        # Create the send and return pipe
        recv_pipe, send_pipe = self.spawn_ctx.Pipe()

        process = self._start_reconcile_process(request, send_pipe)

        # Generate the reconcile process and update map
        reconcile_process = ReconcileProcess(
            process=process, request=request, pipe=recv_pipe
        )

        self.running_reconciles[request.uid()] = reconcile_process
        return True

    def _start_reconcile_process(
        self, request: ReconcileRequest, pipe: Connection
    ) -> multiprocessing.Process:
        """Helper function to generate and start the reconcile process. This
        was largely created to ease the testing and mocking process

         Args:
             request: ReconcileRequest
                The request to start the process with
             pipe: Connection
                The result pipe for this reconcile

        Returns:
            process: multiprocessing.Process
                The started process
        """

        # Create and start the reconcile process
        process = self.spawn_ctx.Process(
            target=create_and_start_entrypoint,
            args=[self.logging_queue, request, pipe],
        )
        process.start()
        log.debug3("Started child process with pid: %s", process.pid)
        return process

    ## Queue Functions ##################################################

    def _get_all_requests(
        self, timeout: Optional[int] = None
    ) -> List[ReconcileRequest]:
        """Get all of the requests from the reconcile queue

        Args:
            timeout:Optional[int]=None
                The timeout to wait for an event. If None it returns immediately

        Returns:
            requests: List[ReconcileRequest]
                The list of requests gathered from the queue
        """
        request_list = []
        while not self.request_queue.empty():
            try:
                request = self.request_queue.get(block=False, timeout=timeout)

                # If there is a stop request then immediately return it
                if request.type == ReconcileRequestType.STOPPED:
                    return [request]
                request_list.append(request)
            except queue.Empty:
                break
        return request_list
