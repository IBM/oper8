"""
Module for the ThreadBase Class
"""

# Standard
import threading

# First Party
import alog

# Local
from ....deploy_manager import DeployManagerBase
from ..leader_election import DryRunLeadershipManager, LeadershipManagerBase

log = alog.use_channel("TRDUTLS")


class ThreadBase(threading.Thread):
    """Base class for all other thread classes. This class handles generic starting, stopping,
    and leadership functions"""

    def __init__(
        self,
        name: str = None,
        daemon: bool = None,
        deploy_manager: DeployManagerBase = None,
        leadership_manager: LeadershipManagerBase = None,
    ):
        """Initialize class and store required instance variables. This function
        is normally overriden by subclasses that pass in static name/daemon variables

        Args:
            name:str=None
                The name of the thread to manager
            daemon:bool=None
                Whether python should wait for this thread to stop before exiting
            deploy_manager: DeployManagerBase = None
                The deploy manager available to this thread during start()
            leadership_manager: LeadershipManagerBase = None
                The leadership_manager for tracking elections
        """
        self.deploy_manager = deploy_manager
        self.leadership_manager = leadership_manager or DryRunLeadershipManager()
        self.shutdown = threading.Event()
        super().__init__(name=name, daemon=daemon)

    ## Abstract Interface ######################################################
    #
    # These functions must be implemented by child classes
    ##
    def run(self):
        """Control loop for the thread. Once this function exits the thread stops"""
        raise NotImplementedError()

    ## Base Class Interface ####################################################
    #
    # These methods MAY be implemented by children, but contain default
    # implementations that are appropriate for simple cases.
    #
    ##

    def start_thread(self):
        """If the thread is not already alive start it"""
        if not self.is_alive():
            log.info("Starting %s: %s", self.__class__.__name__, self.name)
            self.start()

    def stop_thread(self):
        """Set the shutdown event"""
        log.info("Stopping %s: %s", self.__class__.__name__, self.name)
        self.shutdown.set()

    def should_stop(self) -> bool:
        """Helper to determine if a thread should shutdown"""
        return self.shutdown.is_set()

    def check_preconditions(self) -> bool:
        """Helper function to check if the thread should shutdown or reacquire leadership"""
        if self.should_stop():
            return False

        if self.leadership_manager and not self.leadership_manager.is_leader():
            log.debug3("Waiting for leadership")
            self.leadership_manager.acquire()

        return True

    def wait_on_precondition(self, timeout: float) -> bool:
        """Helper function to allow threads to wait for a certain period of time
        only being interrupted for preconditions"""
        self.shutdown.wait(timeout)

        return self.check_preconditions()
