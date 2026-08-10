"""
This defines the base class for all DeployManager types.
"""

# Standard
from enum import Enum
from typing import Iterator, List, Optional, Tuple
import abc

# Local
from .kube_event import KubeWatchEvent


class DeployMethod(Enum):
    DEFAULT = "default"
    UPDATE = "update"
    REPLACE = "replace"


class DeployManagerBase(abc.ABC):
    """
    Base class for deploy managers which will be responsible for carrying out
    the actual deploy of an Application/Component.
    """

    @abc.abstractmethod
    def deploy(
        self,
        resource_definitions: List[dict],
        manage_owner_references: bool = True,
        method: DeployMethod = DeployMethod.DEFAULT,
    ) -> Tuple[bool, bool]:
        """The deploy function ensures that the resources defined in the list of
        definitions are deployed in the cluster.

        Args:
            resource_definitions:  list(dict)
                List of resource object dicts to apply to the cluster
            manage_owner_references:  bool
                If true, ownerReferences for the parent CR will be applied to
                the deployed object

        Returns:
            success:  bool
                Whether or not the deploy succeeded
            changed:  bool
                Whether or not the deployment resulted in changes
        """

    @abc.abstractmethod
    def disable(self, resource_definitions: List[dict]) -> Tuple[bool, bool]:
        """The disable function ensures that the resources defined in the list of
        definitions are deleted from the cluster

        Args:
            resource_definitions:  list(dict)
                List of resource object dicts to apply to the cluster

        Returns:
            success:  bool
                Whether or not the delete succeeded
            changed:  bool
                Whether or not the delete resulted in changes
        """

    @abc.abstractmethod
    def get_object_current_state(
        self,
        kind: str,
        name: str,
        namespace: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Tuple[bool, dict]:
        """The get_current_objects function fetches the current state of a given
        object by name

        Args:
            kind:  str
                The kind of the object to fetch
            name:  str
                The full name of the object to fetch
            namespace:  str
                The namespace to search for the object
            api_version:  str
                The api_version of the resource kind to fetch

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            current_state:  dict or None
                The dict representation of the current object's configuration,
                or None if not present
        """

    @abc.abstractmethod
    def watch_objects(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        api_version: Optional[str] = None,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
        resource_version: Optional[str] = None,
    ) -> Iterator[KubeWatchEvent]:
        """The watch_objects function listens for changes in the cluster and returns a
        stream of KubeWatchEvents

        Args:
            kind:  str
                The kind of the object to fetch
            namespace:  str
                The namespace to search for the object
            name:  str
                The name to search for the object
            api_version:  str
                The api_version of the resource kind to fetch
            label_selector:  str
                The label_selector to filter the resources
            field_selector:  str
                The field_selector to filter the resources
            resource_version:  str
                The resource_version the resource must be newer than

        Returns:
            watch_stream: Generator[KubeWatchEvent]
                A stream of KubeWatchEvents generated while watching
        """

    @abc.abstractmethod
    def filter_objects_current_state(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        namespace: Optional[str] = None,
        api_version: Optional[str] = None,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
    ) -> Tuple[bool, List[dict]]:
        """The filter_objects_current_state function fetches a list of objects
        that match either/both the label or field selector
        Args:
            kind:  str
                The kind of the object to fetch
            namespace:  str
                The namespace to search for the object
            api_version:  str
                The api_version of the resource kind to fetch
            label_selector:  str
                The label_selector to filter the resources
            field_selector:  str
                The field_selector to filter the resources

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            current_state:  List[dict]
                A list of  dict representations for the objects configuration,
                or an empty list if no objects match
        """

    @abc.abstractmethod
    def set_status(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        name: str,
        namespace: Optional[str],
        status: dict,
        api_version: Optional[str] = None,
    ) -> Tuple[bool, bool]:
        """Set the status for an object managed by oper8

        Args:
            kind:  str
                The kind of the object ot fetch
            name:  str
                The full name of the object to fetch
            namespace:  Optional[str]
                The namespace to search for the object. If None search cluster wide
            status:  dict
                The status object to set onto the given object
            api_version:  str
                The api_version of the resource to update

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            changed:  bool
                Whether or not the status update resulted in a change
        """
