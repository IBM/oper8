"""Implementation of the DryRun LeaderElection"""
# Standard
from typing import Optional

# Local
from ....managed_object import ManagedObject
from .base import LeadershipManagerBase


class DryRunLeadershipManager(LeadershipManagerBase):
    """DryRunLeaderElection class implements an empty leadership
    election manager which always acts as a leader. This is useful
    for dryrun or running without leadership election"""

    def acquire(self, force: bool = False):
        """
        Return true as dryrun is always leader
        """
        return True

    def acquire_resource(self, resource: ManagedObject):
        """
        Return true as dryrun is always leader
        """
        return True

    def release(self):
        """
        NoOp in DryRun as lock is not real
        """

    def release_resource(self, resource: ManagedObject):
        """
        NoOp in DryRun as lock is not real
        """

    def is_leader(self, resource: Optional[ManagedObject] = None):
        """
        DryRunLeadershipManager is always leader
        """
        return True
