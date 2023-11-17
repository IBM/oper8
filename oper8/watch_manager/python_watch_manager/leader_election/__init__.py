"""__init__ file for leadership election classes. Imports all leadership managers
 and defines a generic helper"""
# Standard
from typing import Type

# Local
from .... import config
from .annotation import AnnotationLeadershipManager
from .base import LeadershipManagerBase
from .dry_run import DryRunLeadershipManager
from .lease import LeaderWithLeaseManager
from .life import LeaderForLifeManager


def get_leader_election_class() -> Type[LeadershipManagerBase]:
    """Get the current configured leadership election"""
    if config.python_watch_manager.lock.type == "leader-for-life":
        return LeaderForLifeManager
    if config.python_watch_manager.lock.type == "leader-with-lease":
        return LeaderWithLeaseManager
    if config.python_watch_manager.lock.type == "annotation":
        return AnnotationLeadershipManager
    if config.python_watch_manager.lock.type == "dryrun":
        return DryRunLeadershipManager
    return DryRunLeadershipManager
