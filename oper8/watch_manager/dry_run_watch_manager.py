"""
Dry run implementation of the WatchManager abstraction
"""

# Standard
from functools import partial
from typing import Optional, Type
import logging

# First Party
import alog

# Local
from ..controller import Controller
from ..deploy_manager import DryRunDeployManager
from ..reconcile import ReconcileManager
from .base import WatchManagerBase

log = alog.use_channel("DRWAT")


class DryRunWatchManager(WatchManagerBase):
    """
    The DryRunWatchManager implements the WatchManagerBase interface with using
    a single shared DryRunDeployManager to manage an in-memory representation of
    the cluster.
    """

    reconcile_manager = None

    def __init__(
        self,
        controller_type: Type[Controller],
        deploy_manager: Optional[DryRunDeployManager] = None,
    ):
        """Construct with the type of controller to watch and optionally a
        deploy_manager instance. A deploy_manager will be constructed if none is
        given.

        Args:
            controller_type:  Type[Controller]
                The class for the controller that will be watched
            deploy_manager:  Optional[DryRunWatchManager]
                If given, this deploy_manager will be used. This allows for
                there to be pre-populated resources. Note that it _must_ be a
                DryRunDeployManager (or child class) that supports registering
                watches.
        """
        super().__init__(controller_type)

        # Set up the deploy manager
        self._deploy_manager = deploy_manager or DryRunDeployManager()

        # We lazily initialize the controller instance in watch and _resource in run_reconcile
        self._controller = None
        self._resource = {}

        # We initialize the reconcile_manager instance on first watch creation
        if not self.reconcile_manager:
            self.reconcile_manager = ReconcileManager(
                deploy_manager=self._deploy_manager, reimport_controller=False
            )

    def watch(self) -> bool:
        """Register the watch with the deploy manager"""
        if self._controller is not None:
            log.warning("Cannot watch multiple times!")
            return False

        log.debug("Registering %s with the DeployManager", self.controller_type)

        # Construct controller
        self._controller = self.controller_type()

        # Register watch and finalizers
        api_version = f"{self.group}/{self.version}"
        self._deploy_manager.register_watch(
            api_version=api_version,
            kind=self.kind,
            callback=partial(self.run_reconcile, False),
        )
        if self.controller_type.has_finalizer:
            log.debug("Registering finalizer")
            self._deploy_manager.register_finalizer(
                api_version=api_version,
                kind=self.kind,
                callback=partial(self.run_reconcile, True),
            )

        return True

    def wait(self):
        """There is nothing to do in wait"""

    def stop(self):
        """There is nothing to do in stop"""

    def run_reconcile(self, is_finalizer: bool, resource: dict):
        """Wrapper function to simplify parameter/partial mapping"""
        if not self.reconcile_manager:
            return

        # Only run reconcile if it's a unique resource
        resource_metadata = self._resource.get("metadata", {})
        if (
            self._resource.get("kind") == resource.get("kind")
            and self._resource.get("apiVersion") == resource.get("apiVersion")
            and resource_metadata.get("name")
            == resource.get("metadata", {}).get("name")
            and resource_metadata.get("namespace")
            == resource.get("metadata", {}).get("namespace")
        ):
            return

        # Save the current resource and log handlers then restore it after the reconcile
        # is completed
        log_formatters = {}
        for handler in logging.getLogger().handlers:
            log_formatters[handler] = handler.formatter
        current_resource = self._resource
        self._resource = resource

        self.reconcile_manager.reconcile(self._controller, resource, is_finalizer)
        self._resource = current_resource
        for handler, formatter in log_formatters.items():
            handler.setFormatter(formatter)
