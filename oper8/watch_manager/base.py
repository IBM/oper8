"""
This module holds the base class interface for the various implementations of
WatchManager
"""

# Standard
from typing import Type
import abc

# First Party
import alog

# Local
from ..controller import Controller

log = alog.use_channel("WATCH")


class WatchManagerBase(abc.ABC):
    """A WatchManager is responsible for linking a kubernetes custom resource
    type with a Controller that will execute the reconciliation loop
    """

    # Class-global mapping of all watches managed by this operator
    _ALL_WATCHES = {}

    ## Interface ###############################################################

    def __init__(
        self,
        controller_type: Type[Controller],
    ):
        """Construct with the controller type that will be watched

        Args:
            controller_type:  Type[Controller],
                The Controller instance that will manage this group/version/kind
        """
        self.controller_type = controller_type
        self.group = controller_type.group
        self.version = controller_type.version
        self.kind = controller_type.kind

        # Register this watch instance
        watch_key = str(self)
        assert (
            watch_key not in self._ALL_WATCHES
        ), "Only a single controller may watch a given group/version/kind"
        self._ALL_WATCHES[watch_key] = self

    @abc.abstractmethod
    def watch(self) -> bool:
        """The watch function is responsible for initializing the persistent
        watch and returning whether or not the watch was started successfully.

        Returns:
            success:  bool
                True if the watch was spawned correctly, False otherwise.
        """

    @abc.abstractmethod
    def wait(self):
        """The wait function is responsible for blocking until the managed watch
        has been terminated.
        """

    @abc.abstractmethod
    def stop(self):
        """Terminate this watch if it is currently running"""

    ## Utilities ###############################################################

    @classmethod
    def start_all(cls) -> bool:
        """This utility starts all registered watches

        Returns:
            success:  bool
                True if all watches started succssfully, False otherwise
        """
        started_watches = []
        success = True
        # NOTE: sorting gives deterministic order so that launch failures can be
        #   diagnosed (and tested) more easily. This is not strictly necessary,
        #   but it also doesn't hurt and it is nice to have.
        for _, watch in sorted(cls._ALL_WATCHES.items()):
            if watch.watch():
                log.debug("Successfully started %s", watch)
                started_watches.append(watch)
            else:
                log.warning("Failed to start %s", watch)
                success = False

                # Shut down all successfully started watches
                for started_watch in started_watches:
                    started_watch.stop()

                # Don't start any of the others
                break

        # Wait on all of them to terminate
        for watch in cls._ALL_WATCHES.values():
            watch.wait()

        return success

    @classmethod
    def stop_all(cls):
        """This utility stops all watches"""
        for watch in cls._ALL_WATCHES.values():
            try:
                watch.stop()
                log.debug2("Waiting for %s to terminate", watch)
                watch.wait()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                log.error("Failed to stop watch manager %s", exc, exc_info=True)

    ## Implementation Details ##################################################

    def __str__(self):
        """String representation of this watch"""
        return f"Watch[{self.controller_type}]"
