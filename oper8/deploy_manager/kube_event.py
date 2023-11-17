"""
Helper module to define shared types related to Kube Events
"""

# Standard
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

# First Party
import alog

# Local
from ..managed_object import ManagedObject

log = alog.use_channel("KUBEWATCH")


class KubeEventType(Enum):
    """Enum for all possible kubernetes event types"""

    DELETED = "DELETED"
    MODIFIED = "MODIFIED"
    ADDED = "ADDED"


@dataclass
class KubeWatchEvent:
    """DataClass containing the type, resource, and timestamp of a
    particular event"""

    type: KubeEventType
    resource: ManagedObject
    timestamp: datetime = field(default_factory=datetime.now)
