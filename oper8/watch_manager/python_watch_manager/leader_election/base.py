"""Base classes for leader election implementations"""

# Standard
from typing import Optional
import abc
import threading

# First Party
import alog

# Local
from .... import config
from ....deploy_manager import DeployManagerBase
from ....exceptions import ConfigError
from ....managed_object import ManagedObject
from ..utils import ABCSingletonMeta, parse_time_delta

log = alog.use_channel("LDRELC")


class LeadershipManagerBase(abc.ABC):
    """
    Base class for leader election. Leadership election in the PWM
    is split into two types: global and resource locks. Global locks
    are required to run any reconciliation while resource locks are
    required to reconcile a specific resources. Most child classes
    implement one of these locks.
    """

    def __init__(self, deploy_manager: DeployManagerBase = None):
        """
        Initialize Class

        Args:
            deploy_manager:  DeployManagerBase
                DeployManager used in lock acquisition
        """
        self.deploy_manager = deploy_manager

    ## Lock Interface ####################################################
    @abc.abstractmethod
    def acquire(self, force: bool = False) -> bool:
        """
        Acquire or renew global lock

        Args:
            force:  bool
                Whether to force acquire the lock irregardless of status. Used
                on shutdown

        Returns:
            success:  bool
                True on successful acquisition
        """

    @abc.abstractmethod
    def acquire_resource(self, resource: ManagedObject) -> bool:
        """
        Acquire or renew lock on specific resource

        Args:
            resource:  ManagedObject
                Resource to acquire lock for
        Returns:
            success:  bool
                True on successful acquisition
        """

    @abc.abstractmethod
    def release(self):
        """
        Release global lock
        """

    @abc.abstractmethod
    def release_resource(self, resource: ManagedObject):
        """
        Release lock on specific resource

        Args:
            resource:  ManagedObject
                Resource to release lock for
        """

    @abc.abstractmethod
    def is_leader(self, resource: Optional[ManagedObject] = None):
        """
        Determines if current instance is leader

        Args:
            resource:  Optional[ManagedObject]
                If provided the resource to determine if current instance
                is leader for. If no resource if provided then the global
                lock is checked
        Returns:
            leader:  bool
                True if instance is leader
        """


class ThreadedLeaderManagerBase(LeadershipManagerBase, metaclass=ABCSingletonMeta):
    """
    Base class for threaded leadership election. This base class aids in the
    creation of leadership election classes that require constantly checking
    or updating a resource. Child classes only need to implement renew_or_acquire,
    and it will automatically be looped while lock acquisition is needed
    """

    def __init__(self, deploy_manager: DeployManagerBase):
        """
        Initialize class with events to track leadership and shutdown and
        a lock to ensure renew_or_acquire is only ran once.

        Args:
            deploy_manager: DeployManagerBase
                DeployManager for leader election
        """
        super().__init__(deploy_manager)

        # Events to track status
        self.leader = threading.Event()
        self.shutdown = threading.Event()

        # Lock to ensure multiple acquires aren't running at the same time
        self.run_lock = threading.Lock()

        # Object to track Leadership thread
        self.leadership_thread = threading.Thread(
            name="leadership_thread", target=self.run, daemon=True
        )

        # Calculate threaded poll time:
        poll_time_delta = parse_time_delta(config.python_watch_manager.lock.poll_time)
        if not poll_time_delta:
            log.error(
                "Invalid 'python_watch_manager.lock.poll_time' value: '%s'",
                config.python_watch_manager.lock.poll_time,
            )
            raise ConfigError(
                "Invalid 'python_watch_manager.lock.poll_time' value: "
                f"'{config.python_watch_manager.lock.poll_time}'"
            )
        self.poll_time = poll_time_delta.seconds

    ## Public Interface ####################################################

    def renew_or_acquire(self):
        """
        Renew or acquire leadership lock
        """
        raise NotImplementedError

    def acquire_lock(self):
        """
        Helper function for child classes to acquire leadership lock
        """
        if not self.leader.is_set():
            log.debug2("Acquiring leadership lock")
        # Always set the lock during acquire_lock to avoid concurrency issues
        self.leader.set()

    def release_lock(self):
        """
        Helper function for child classes to release lock
        """
        if self.leader.is_set():
            log.debug2("Releasing leadership lock")
        self.leader.clear()

    ## Lock Interface ####################################################
    def acquire(self, force: bool = False):
        """
        Start/Restart leadership thread or run renew_or_acquire

        Args:
            force:  bool=False
                Whether to force acquire the lock

        Returns:
            success:  bool
                True on successful acquisition
        """
        if force:
            self.leader.set()
            return True

        # ident is set when thread has started
        if not self.leadership_thread.is_alive():
            # Recreate leadership thread if its already exited
            if self.leadership_thread.ident:
                self.leadership_thread = threading.Thread(
                    name="leadership_thread", target=self.run, daemon=True
                )
            log.info(
                "Starting %s: %s", self.__class__.__name__, self.leadership_thread.name
            )
            self.leadership_thread.start()
        else:
            self.run_renew_or_acquire()

        return self.leader.wait()

    def acquire_resource(self, resource: ManagedObject) -> bool:
        """
        Lock in background so acquire_resource just waits for value

        Args:
            resource:  ManagedObject
                Resource that is being locked

        Returns:
            success:  bool
                True on successful acquisition else False
        """
        return self.leader.wait()

    def release(self):
        """
        Release lock and shutdown leader election thread. This thread
        first shuts down the background thread before clearing the lock
        """
        self.shutdown.set()
        self.leadership_thread.join()
        self.leader.clear()

    def release_resource(self, resource: ManagedObject):
        """
        Release resource is not implemented in Threaded classes
        """

    def is_leader(self, resource: Optional[ManagedObject] = None) -> bool:
        """
        Return if leader event has been acquired

        Returns:
            leader: bool
                If instance is current leader
        """
        return self.leader.is_set()

    ## Implementation Details ####################################################

    def run(self):
        """
        Loop to continuously run renew or acquire every so often
        """
        while True:
            if self.shutdown.is_set():
                log.debug("Shutting down %s Thread", self.__class__.__name__)
                return

            self.run_renew_or_acquire()
            self.shutdown.wait(self.poll_time)

    def run_renew_or_acquire(self):
        """
        Run renew_or_acquire safely and with threaded lock
        """
        log.debug2("Running renew or acquire for %s lock", self.__class__.__name__)
        with self.run_lock:
            try:
                self.renew_or_acquire()
            except Exception as err:
                log.warning(
                    "Error detected while acquiring leadership lock", exc_info=True
                )
                raise RuntimeError("Error detected when acquiring lock") from err
