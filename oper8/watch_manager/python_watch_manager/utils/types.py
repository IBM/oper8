"""Standard data types used through PWM"""

# Standard
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import cached_property
from multiprocessing.connection import Connection
from typing import Dict, List, NamedTuple, Type, Union
import abc
import importlib
import multiprocessing

# Local
from ....managed_object import ManagedObject

### General Use Data Classes

# Forward Declarations
CONTROLLER_TYPE = "Controller"
FILTER_MANAGER_TYPE = "FilterManager"
FILTER_TYPE = "Filter"
KUBE_EVENT_TYPE_TYPE = "KubeEventType"


@dataclass(eq=True, frozen=True)
class ResourceId:
    """Class containing the information needed to identify a resource"""

    api_version: str
    kind: str
    name: str = None
    namespace: str = None

    # Id properties

    @cached_property
    def global_id(self) -> str:
        """Get the global_id for a resource in the form kind.version.group"""
        group_version = self.api_version.split("/")
        return ".".join([self.kind, *reversed(group_version)])

    @cached_property
    def namespaced_id(self) -> str:
        """Get the namespace specific id for a resource"""
        return f"{self.namespace}.{self.global_id}"

    # Helper Accessor functions
    def get_id(self) -> str:
        """Get the requisite id for a resource"""
        return self.namespaced_id if self.namespace else self.global_id

    def get_named_id(self) -> str:
        """Get a named id for a resouce"""
        return f"{self.name}.{self.get_id()}"

    def get_resource(self) -> dict:
        """Get a resource template from this id"""
        return {
            "kind": self.kind,
            "apiVersion": self.api_version,
            "metadata": {"name": self.name, "namespace": self.namespace},
        }

    # Helper Creation Functions
    @classmethod
    def from_resource(cls, resource: Union[ManagedObject, dict]) -> "ResourceId":
        """Create a resource id from an existing resource"""
        metadata = resource.get("metadata", {})
        return cls(
            api_version=resource.get("apiVersion"),
            kind=resource.get("kind"),
            namespace=metadata.get("namespace"),
            name=metadata.get("name"),
        )

    @classmethod
    def from_owner_ref(cls, owner_ref: dict, namespace: str = None) -> "ResourceId":
        """Create a resource id from an ownerRef"""
        return cls(
            api_version=owner_ref.get("apiVersion"),
            kind=owner_ref.get("kind"),
            namespace=namespace,
            name=owner_ref.get("name"),
        )

    @classmethod
    def from_controller(
        cls, controller: Type[CONTROLLER_TYPE], namespace: str = None
    ) -> "ResourceId":
        """Get a Controller's target as a resource id"""
        return cls(
            api_version=f"{controller.group}/{controller.version}",
            kind=controller.kind,
            namespace=namespace,
        )


### Watch Data Classes


class ClassInfo(NamedTuple):
    """Class containing information describing a class. This is required when passing class
    references between processes which might have different sys paths like when using VCS"""

    moduleName: str
    className: str

    # Generation Utilities
    @classmethod
    def from_type(cls, class_obj: type) -> "ClassInfo":
        """Create a ClassInfo from a class object"""
        return cls(moduleName=class_obj.__module__, className=class_obj.__name__)

    @classmethod
    def from_obj(cls, obj) -> "ClassInfo":
        """Create a ClassInfo from an existing object"""
        return cls.from_type(type(obj))

    # Get the class referenced described by the info
    def to_class(self) -> type:
        """Import and return a ClassInfo's type"""
        module = importlib.import_module(self.moduleName)
        if not module:
            raise ValueError(f"Invalid ControllerInfo Module: {self.moduleName}")

        if not hasattr(module, self.className):
            raise ValueError(
                f"Invalid ControllerInfo: {self.className} not a member of {self.moduleName}"
            )

        return getattr(module, self.className)


@dataclass
class WatchedResource:
    """A class for tracking a resource in the cluster. Every resource that has a
    requested watch will have a corresponding WatchedResource"""

    gvk: ResourceId
    # Each watched resource contains a dict of filters for each
    # corresponding watch request. The key is the named_id of
    # the requester or None for default filters. This aligns with
    # the Controllers pwm_filters attribute
    filters: Dict[str, FILTER_MANAGER_TYPE] = field(default_factory=dict)


@dataclass()
class WatchRequest:
    """A class for requesting a watch of a particular object. It contains information around the
    watched object, who requested the watch, the controller type to be reconciled, and any filters
    to be applied to just this request"""

    watched: ResourceId
    requester: ResourceId

    # Watch request must have either type or info
    controller_type: Type[CONTROLLER_TYPE] = None
    controller_info: ClassInfo = None

    # Don't compare filters when checking equality as we
    # assume they're the same if they have the same controller
    filters: List[Type[FILTER_TYPE]] = field(default_factory=list, compare=False)
    filters_info: List[Type[ClassInfo]] = field(default_factory=list, compare=False)

    def __hash__(self) -> int:
        return hash(
            (
                self.watched,
                self.requester,
                self.controller_type if self.controller_type else self.controller_info,
            )
        )


##  Reconcile Enums


class ReconcileRequestType(Enum):
    """Enum to expand the possible KubeEventTypes to include PythonWatchManager
    specific events"""

    # Used for events that are a requeue of an object
    REQUEUED = "REQUEUED"

    # Used for periodic reconcile events
    PERIODIC = "PERIODIC"

    # Used for when an event is a dependent resource of a controller
    DEPENDENT = "DEPENDENT"

    # Used as a sentinel to alert threads to stop
    STOPPED = "STOPPED"


### Reconcile Classes


@dataclass
class ReconcileRequest:
    """Class to represent one request to the ReconcileThread. This includes
    important information including the current resource and Controller being
    reconciled.
    """

    controller_type: Type[CONTROLLER_TYPE]
    type: Union[ReconcileRequestType, KUBE_EVENT_TYPE_TYPE]
    resource: ManagedObject
    timestamp: datetime = datetime.now()

    def uid(self):
        """Get the uid of the resource being reconciled"""
        return self.resource.uid


@dataclass
class ReconcileProcess:
    """Dataclass to track a running reconcile. This includes the raw process
    object, the result pipe, and the request being reconciled"""

    process: multiprocessing.Process
    request: ReconcileRequest
    pipe: Connection

    def fileno(self):
        """Pass through fileno to process. Sentinel so this object can be
        directly used by multiprocessing.connection.wait"""
        return self.process.sentinel

    def uid(self):
        """Get the uid for the resource being reconciled"""
        return self.request.uid()


### Timer Data Classes
@dataclass(order=True)
class TimerEvent:
    """Class for keeping track of an item in the timer queue. Time is the
    only comparable field to support the TimerThreads priority queue"""

    time: datetime
    action: callable = field(compare=False)
    args: list = field(default_factory=list, compare=False)
    kwargs: dict = field(default_factory=dict, compare=False)
    stale: bool = field(default=False, compare=False)

    def cancel(self):
        """Cancel this event. It will not be executed when read from the
        queue"""
        self.stale = True


## Meta Classes


class Singleton(type):
    """MetaClass to limit a class to only one global instance. When the
    first instance is created it's attached to the Class and the next
    time someone initializes the class the original instance is returned
    """

    def __call__(cls, *args, **kwargs):
        if getattr(cls, "_disable_singleton", False):
            return type.__call__(cls, *args, **kwargs)

        # The _instance is attached to the class itself without looking upwards
        # into any parent classes
        if "_instance" not in cls.__dict__:
            cls._instance = type.__call__(cls, *args, **kwargs)
        return cls._instance


class ABCSingletonMeta(Singleton, abc.ABCMeta):
    """Shared metaclass for ABCMeta and Singleton"""
