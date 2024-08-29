"""
ReconcileProcessEntrypoint for all PWM reconciles
"""

# Standard
from multiprocessing.connection import Connection
from typing import Type
import copy
import dataclasses
import logging
import multiprocessing
import os
import signal
import sys
import threading

# First Party
import aconfig
import alog

# Local
from ... import config
from ...controller import Controller
from ...deploy_manager import DeployManagerBase, KubeEventType, OpenshiftDeployManager
from ...reconcile import ReconcileManager
from .filters import (
    AndFilter,
    DependentWatchFilter,
    FilterManager,
    get_filters_for_resource_id,
)
from .utils import (
    ClassInfo,
    LogQueueHandler,
    ReconcileRequest,
    ResourceId,
    WatchRequest,
)

log = alog.use_channel("ENTRY")


## ReconcileProcessEntrypoint Deploy Manager


class ReconcileProcessDeployManager(OpenshiftDeployManager):
    """ReconcileProcessEntrypoint deploy manager is a helper deploy manager
    that allows the PWM to insert functionality during a reconcile. This
    is used for things like watching dependent resources and subsystem rollout"""

    def __init__(
        self,
        controller_type: Type[Controller],
        controller_resource: aconfig.Config,
        result_pipe: Connection,
        *args,
        **kwargs,
    ):
        """Initalize the ReconcileProcessEntrypoint DeployManger and gather start-up configurations

        Args:
            controller_type: Type[Controller]
                The Controller being reconciled
            controller_resource: aconfig.Config
                The resource being reconciled
            result_pipe: Connection
                The pipe to send dependent watch requests to
            *args:
                Extendable arguments to pass to to parent
            **kwargs:
                Extendable key word arguments to pass to parent
        """
        # Initialize ReconcileProcessEntrypoint Deploy Manager
        super().__init__(*args, owner_cr=controller_resource, **kwargs)

        # Initialize required variables
        self.requested_watches = set()
        self.result_pipe = result_pipe
        self.controller_type = controller_type

        # Setup Subsystems
        self.subsystems = self._gather_subsystems(controller_type)
        self.reconcile_manager = ReconcileManager(deploy_manager=self)

    # Functional Overrides

    def _apply_resource(self, resource_definition: dict) -> dict:
        """Override apply resource for handling watch_dependent_resources and subsystem rollout"""
        resource = super()._apply_resource(resource_definition)
        resource_id = ResourceId.from_resource(resource)

        # Send watch request if watch_dependent_resources is enabled
        # and/or handle subsystem rollout
        if config.python_watch_manager.watch_dependent_resources:
            log.debug2("Handling dependent resource %s", resource_id)
            self._handle_dependent_resource(resource_id)

        if (
            config.python_watch_manager.subsystem_rollout
            and resource_id.global_id in self.subsystems
        ):
            log.debug2("Rolling out subsystem %s", resource_id.global_id)
            self._handle_subsystem(
                resource, self.subsystems[resource_id.global_id], False
            )

        return resource

    def _replace_resource(self, resource_definition: dict) -> dict:
        """Override replace resource for handling watch_dependent_resources and subsystem rollout"""
        resource = super()._replace_resource(resource_definition)
        resource_id = ResourceId.from_resource(resource)

        # Send watch request if watch_dependent_resources is enabled
        # and/or handle subsystem rollout
        if config.python_watch_manager.watch_dependent_resources:
            log.debug2("Handling dependent resource %s", resource_id)
            self._handle_dependent_resource(resource_id)

        if (
            config.python_watch_manager.subsystem_rollout
            and resource_id.global_id in self.subsystems
        ):
            log.debug2("Rolling out subsystem %s", resource_id.global_id)
            self._handle_subsystem(
                resource, self.subsystems[resource_id.global_id], False
            )

        return resource

    def _disable(self, resource_definition: dict) -> bool:
        """Override disable to insert subsystem logic"""

        changed = super()._disable(resource_definition)
        if not changed:
            return changed

        resource_id = ResourceId.from_resource(resource_definition)

        # If deleted resource is a subsystem then run reconcile with finalizer
        if (
            config.python_watch_manager.subsystem_rollout
            and resource_id.global_id in self.subsystems
        ):
            success, current_state = self.get_object_current_state(
                kind=resource_id.kind,
                name=resource_id.name,
                namespace=resource_id.namespace,
                api_version=resource_id.api_version,
            )
            if not success or not current_state:
                log.warning(
                    "Unable to fetch owner resource %s/%s/%s/%s",
                    resource_id.kind,
                    resource_id.api_version,
                    resource_id.namespace,
                    resource_id.name,
                )
                return changed

            self._handle_subsystem(
                current_state, self.subsystems[resource_id.global_id], True
            )

        return changed

    def _handle_subsystem(self, resource, controller_type, is_finalizer):
        """Handle rolling out a subsystem for a specific controller, resource, and finalizer"""

        # Copy a ref of the current logging format to restore to
        log_formatters = {}
        for handler in logging.getLogger().handlers:
            log_formatters[handler] = handler.formatter

        # Update the current owner
        current_owner = self._owner_cr
        self._owner_cr = resource
        current_controller_type = self.controller_type
        self.controller_type = controller_type

        # Add the new controllers subsystems to the current dictionary
        # this simplifies future look ups
        current_subsystems = self.subsystems
        self.subsystems = (self._gather_subsystems(controller_type),)

        self.reconcile_manager.safe_reconcile(controller_type, resource, is_finalizer)

        # Reset owner_cr, logging, and subsystems
        self._owner_cr = current_owner
        self.controller_type = current_controller_type
        self.subsystems = current_subsystems
        for handler, formatter in log_formatters.items():
            handler.setFormatter(formatter)

    def _handle_dependent_resource(self, watched_id):
        """Handling request a watch for a deployed resource"""
        # Create requester id
        resource_id = ResourceId.from_resource(self._owner_cr)

        # Remove name from watched_id so it captures
        # any resource of that kind with this owner
        watched_id = copy.deepcopy(watched_id)
        watched_id = dataclasses.replace(watched_id, name=None)

        filters = DependentWatchFilter
        if controller_filters := get_filters_for_resource_id(
            self.controller_type, watched_id
        ):
            filters = AndFilter(DependentWatchFilter, controller_filters)
        watch_filters = FilterManager.to_info(filters)

        watch_request = WatchRequest(
            requester=resource_id,
            watched=watched_id,
            # Use controller info to avoid issues between vcs and pickling
            controller_info=ClassInfo.from_type(self.controller_type),
            filters_info=watch_filters,
        )

        # Only send each watch request once
        if watch_request not in self.requested_watches:
            log.debug3(f"Sending watch request {watch_request}")
            self.result_pipe.send(watch_request)
            self.requested_watches.add(watch_request)

    def _gather_subsystems(self, controller_type: Type[Controller]):
        """Gather the list of subsystems for a controller"""
        subsystem_controllers = getattr(controller_type, "pwm_subsystems", [])
        subsystems = {
            ResourceId.from_controller(controller).global_id: controller
            for controller in subsystem_controllers
        }
        log.debug3(f"Gathered subsystems: {subsystems}")
        return subsystems


## EntryPoint
class ReconcileProcessEntrypoint:  # pylint: disable=too-few-public-methods
    """The ReconcileProcessEntrypoint Class is the main start place for a
    reconciliation. It configures some watch manager specific settings like
    multiprocess logging, and signal handling then it hands off control to the
    ReconcileManager"""

    def __init__(
        self,
        controller_type: Type[Controller],
        deploy_manager: DeployManagerBase = None,
    ):
        """Initializer for the entrypoint class

        Args:
            controller_type: Type[Controller]
                The Controller type being reconciled
            deploy_manager: DeployManagerBase = None
                An optional deploy manager override
        """
        self.controller_type = controller_type
        self.deploy_manager = deploy_manager

        # Initialize the reconcile manager in start
        self.reconcile_manager = None

    def start(
        self,
        request: ReconcileRequest,
        result_pipe: Connection,
    ):
        """Main entrypoint for the class

        Args:
            request: ReconcileRequest
                The reconcile request that trigger this reconciliation
            result_pipe: Connection
                The connection to send results back to
        """
        # Parse the request and setup local variables
        log.debug4("Setting up resource")
        resource = request.resource
        resource_id = ResourceId.from_resource(resource)

        # Set a unique thread name for each reconcile
        thread_name = f"entrypoint_{resource_id.get_id()}/{resource_id.name}"
        log.debug4("Setting thread name: %s", thread_name)
        threading.current_thread().name = thread_name

        # Reset signal handlers to default function
        log.debug4("Resetting signals")
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        # Replace stdout and stderr with a null stream as all messages should be passed via
        # the queue and any data in the buffer could cause the process to hang. This can
        # make it difficult to debug subprocesses if they fail before setting up the handler
        log.debug4("Redirecting to /dev/null")
        with open(os.devnull, "w", encoding="utf-8") as null_file:
            sys.stdout = null_file
            sys.stderr = null_file

            log.info(
                "ReconcileProcessEntrypoint for %s and with type: %s",
                self.controller_type,
                request.type,
            )

            # If controller_type has subsystems than set reconciliation to standalone mode.
            # This forces the reconcile to be single threaded but allows for recursive reconciles
            log.debug4("Checking for subsystem rollout")
            if (
                getattr(self.controller_type, "pwm_subsystems", [])
                and config.python_watch_manager.subsystem_rollout
            ):
                config.standalone = True

            # Create a custom deploy manager so we can insert functionality
            deploy_manager = self.deploy_manager
            if not deploy_manager:
                deploy_manager = ReconcileProcessDeployManager(
                    result_pipe=result_pipe,
                    controller_resource=resource.definition,
                    controller_type=self.controller_type,
                )

            # Create a reconciliation manager and start the reconcile
            self.reconcile_manager = ReconcileManager(deploy_manager=deploy_manager)

            finalize = request.type == KubeEventType.DELETED or resource.metadata.get(
                "deletionTimestamp"
            )
            reconcile_result = self.reconcile_manager.safe_reconcile(
                self.controller_type,
                resource.definition,
                finalize,
            )
            # Clear exception as it's not always pickleable
            reconcile_result.exception = None

            # Complete the reconcile by sending the result back up the pipe
            # and explicitly close all remaining descriptors
            log.info("Finished Reconcile for %s", resource_id)
            log.debug3("Sending reconciliation result back to main process")
            result_pipe.send(reconcile_result)
            result_pipe.close()


def create_and_start_entrypoint(
    logging_queue: multiprocessing.Queue,
    request: ReconcileRequest,
    result_pipe: Connection,
    deploy_manager: DeployManagerBase = None,
):
    """Function to create and start an entrypoint while catching any unexpected errors
    Args:
        logging_queue: multiprocessing.Queue
            The queue to send log messages to
        request: ReconcileRequest
            The request that triggered this reconciliation
        result_pipe: Connection
            The pipe to send a result back with
        deploy_manager: DeployManagerBase = None
            An optional DeployManager override
    """
    try:
        # Set the logging library to utilize the multiprocessing logging queue. Do this before
        # any logging messages are sent since that might cause a deadlock
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        handler = LogQueueHandler(logging_queue, request.resource)
        root_logger.addHandler(handler)

        log.debug3("Creating entrypoint")
        entry = ReconcileProcessEntrypoint(
            request.controller_type, deploy_manager=deploy_manager
        )
        log.debug3("Starting entrypoint")
        entry.start(request, result_pipe)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        log.error("Uncaught exception '%s'", exc, exc_info=True)

    # Close the logging queue to ensure all messages are sent before process end
    logging_queue.close()
