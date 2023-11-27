"""
Filters are used to limit the amount of events being reconciled by a watch manager
This is based off of the kubernetes controller runtime's "predicates":
https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.15.0/pkg/predicate#Funcs
The default set of filters is derived from operator-sdk's ansible predicates
https://github.com/operator-framework/operator-sdk/blob/50c6ac03746ff4edf582feb9a71d2a7ea6ae6c40/internal/ansible/controller/controller.go#L105
"""

# Standard
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

# First Party
import alog

# Local
from ....deploy_manager import KubeEventType
from ....managed_object import ManagedObject
from ....reconcile import ReconcileManager
from ....status import READY_CONDITION, get_condition
from ....utils import abstractclassproperty
from ..utils import (
    RESERVED_PLATFORM_ANNOTATIONS,
    RESOURCE_VERSION_KEEP_COUNT,
    obj_to_hash,
)

log = alog.use_channel("PWMFLT")


## Default Types


class Filter(ABC):
    """Generic Filter Interface for subclassing. Every subclass should implement a
    `test` function which returns true when a resource should be reconciled. Subclasses
    can optionally implement a `update` method if the filter requires storing some stateful
    information like ResourceVersion or Metadata.

    NOTE: A unique Filter instance is created for each resource
    """

    def __init__(self, resource: ManagedObject):  # noqa: B027
        """Initializer can be used to detect configuration or create instance
        variables. Even though a resource is provided it should not set state until
        update is called

        Args:
            resource: ManagedObject
                This resource can be used by subclass to gather generic information.

        """

    ## Abstract Interface ######################################################
    #
    # These functions must be implemented by child classes
    ##

    @abstractmethod
    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Test whether the resource&event passes the filter. Returns true if
        the filter should be reconciled and return false if it should not be. A filter
        can optionally return None to ignore an event

        Args:
            resource: ManagedObject
                The current resource being checked
            event: KubeEventType
                The event type that triggered this filter

        Returns:
            result: Optional[bool]
                The result of the test.

        """

    ## Base Class Interface ####################################################
    #
    # These methods MAY be implemented by children, but contain default
    # implementations that are appropriate for simple cases.
    #
    ##

    def update(self, resource: ManagedObject):  # noqa: B027
        """Update the instances current state.

        Args:
            resource: ManagedObject
               The current state of the resource
        """

    def update_and_test(self, resource: ManagedObject, event: KubeEventType) -> bool:
        """First test a resource/event against a filter then update the current state

        Args:
            resource: ManagedObject
                The resource being filtered
            event: KubeEventType
                The event to be filtered

        Returns:
            test_result: bool
                The test result
        """
        result = self.test(resource, event)
        if result is not None and not result:
            log.debug3(
                "Failed filter: %s with return val %s",
                self,
                result,
                extra={"resource": resource},
            )
        self.update(resource)
        return result


## Generic Resource filters


class CreationDeletionFilter(Filter):
    """Filter to ensure reconciliation on creation and deletion events"""

    def test(  # pylint: disable=inconsistent-return-statements
        self,
        resource: ManagedObject,
        event: KubeEventType,
    ) -> Optional[bool]:
        """Return true if event is ADDED or DELETED"""

        # Ignore non Added/Deleted Events
        if event not in [KubeEventType.ADDED, KubeEventType.DELETED]:
            return

        return True


class GenerationFilter(Filter):
    """Filter for reconciling on generation changes for resources that support it"""

    def __init__(self, resource: ManagedObject):
        """Set generation instance variable"""
        super().__init__(resource)
        self.generation = None

    def test(  # pylint: disable=inconsistent-return-statements
        self,
        resource: ManagedObject,
        event: KubeEventType,
    ) -> Optional[bool]:
        """Return true if resource generation is different than before"""
        # Only update&test resources with a generation
        if not self.generation:
            return

        # Only test on resource updates
        if event in [KubeEventType.ADDED, KubeEventType.DELETED]:
            return

        # Test if new generation is different
        return self.generation != resource.metadata.get("generation")

    def update(self, resource: ManagedObject):
        """Update the currently observed generation"""
        self.generation = resource.metadata.get("generation")


class NoGenerationFilter(Filter):
    """Filter for reconciling changes to spec on resources that don't support
    the generation field like pods. It does this by hashing the objects excluding
    status and metadata"""

    def __init__(self, resource: ManagedObject):
        """Check if resource supports generation and initialize the hash dict"""
        self.supports_generation = resource.metadata.get("generation") is not None
        self.resource_hashes = {}
        super().__init__(resource)

    def test(  # pylint: disable=inconsistent-return-statements
        self,
        resource: ManagedObject,
        event: KubeEventType,
    ) -> Optional[bool]:
        """Return True if a resources current hash differs from the current"""
        # Don't test resources that support generation or if we don't have hashes yet
        if self.supports_generation or not self.resource_hashes:
            return

        # Only test on resource updates
        if event in [KubeEventType.ADDED, KubeEventType.DELETED]:
            return

        # Check each stored resource hash to see if its
        # changed
        for key, obj_has in self.resource_hashes.items():
            if obj_has != obj_to_hash(resource.get(key)):
                log.debug2("Detected change in %s", key)
                return True

        return False

    def update(self, resource: ManagedObject):
        """Update the observed spec hashes"""
        if self.supports_generation:
            return

        # Get the default hashes for all object keys except metadata
        # and status
        for key, obj in resource.definition.items():
            if key in ["metadata", "status", "kind", "apiVersion"]:
                continue

            self.resource_hashes[key] = obj_to_hash(obj)


class ResourceVersionFilter(Filter):
    """Filter for duplicate resource versions which happens when restarting a
    watch connection"""

    def __init__(self, resource: ManagedObject):
        """Initialize the resource version list"""
        # Use a dequeue instead of a list/set to set a bound on the number
        # of tracked versions
        self.resource_versions = deque([], maxlen=RESOURCE_VERSION_KEEP_COUNT)
        super().__init__(resource)

    def test(  # pylint: disable=inconsistent-return-statements
        self,
        resource: ManagedObject,
        event: KubeEventType,
    ) -> Optional[bool]:
        """Test if the resource's resourceVersion has been seen before"""

        # Don't skip add events as the kubernetes watch can duplicate events
        if event == KubeEventType.DELETED:
            return

        return resource.resource_version not in self.resource_versions

    def update(self, resource: ManagedObject):
        """Add the resources ResourceVersion to the list"""
        self.resource_versions.append(resource.resource_version)


### Annotation Filters


class AnnotationFilter(Filter):
    """Filter resources to reconcile on annotation changes"""

    def __init__(self, resource: ManagedObject):
        """Initialize the annotation hash variable"""
        self.annotations = None
        super().__init__(resource)

    def test(  # pylint: disable=inconsistent-return-statements
        self,
        resource: ManagedObject,
        event: KubeEventType,
    ) -> Optional[bool]:
        """Test if a resource's annotation has changed"""
        # Ignore Added and deleted events
        if event in [KubeEventType.ADDED, KubeEventType.DELETED]:
            return

        return self.annotations != self.get_annotation_hash(resource)

    def update(self, resource: ManagedObject):
        """Update the currently stored annotation"""
        self.annotations = self.get_annotation_hash(resource)

    def get_annotation_hash(self, resource: ManagedObject) -> str:
        """Helper function to get the annotation hash"""
        return obj_to_hash(resource.metadata.get("annotations", {}))


class UserAnnotationFilter(AnnotationFilter):
    """Filter resources to reconcile on user annotation changes. This excludes
    kubernetes and openshift annotations
    """

    def get_annotation_hash(self, resource: ManagedObject) -> str:
        """Overriden function to exclude common platform annotations from
        the annotation hash"""
        output_annotations = {}
        for key, value in resource.metadata.get("annotations", {}).items():
            if self.contains_platform_key(key):
                continue

            output_annotations[key] = value

        return obj_to_hash(output_annotations)

    def contains_platform_key(self, key: str) -> bool:
        """Helper to check if the key contains one of the
        platform annotations"""
        return any(
            reserved_key in key for reserved_key in RESERVED_PLATFORM_ANNOTATIONS
        )


### Oper8 Filters


class PauseFilter(Filter):
    """This filter skips resources that have the oper8 pause annotation"""

    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Test if a resource has the pause annotation"""
        return not ReconcileManager._is_paused(  # pylint: disable=protected-access
            resource
        )


class SubsystemStatusFilter(Filter):
    """Reconcile oper8 controllers when their oper8 status changes

    EXPERIMENTAL: This has passed basic validation but has not been rigorously tested
     in the field
    """

    def __init__(self, resource: ManagedObject):
        """Initialize the currently observed ready condition"""
        self.ready_condition = None
        super().__init__(resource)

    def test(  # pylint: disable=inconsistent-return-statements
        self,
        resource: ManagedObject,
        event: KubeEventType,
    ) -> Optional[bool]:
        """Test if a resources subsystem condition has changed"""
        if event in [KubeEventType.ADDED, KubeEventType.DELETED]:
            return

        return self.ready_condition != get_condition(
            READY_CONDITION, resource.get("status", {})
        ).get("reason")

    def update(self, resource: ManagedObject):
        """Update the currently observed ready condition"""
        self.ready_condition = get_condition(
            READY_CONDITION, resource.get("status", {})
        ).get("reason")


### Dependent Filters


class DependentWatchFilter(Filter):
    """Don't reconcile creation events as we can assume the owner created
    them"""

    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Return False if event is ADDED"""
        return event != KubeEventType.ADDED


### Utility Filters


class LabelFilter(Filter):
    """Filter for resources that match a set of labels"""

    @abstractclassproperty
    def labels(self) -> dict:
        """Subclasses must implement a labels class attribute"""

    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Return true is a resource matches the requested labels"""
        resource_labels = resource.get("metadata", {}).get("labels")
        # Check to make sure every requested label matches
        return all(
            resource_labels.get(label) == value for label, value in self.labels.items()
        )


class DisableFilter(Filter):
    """Filter to disable all reconciles"""

    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Always return False"""
        return False


class EnableFilter(Filter):
    """Filter to run all reconciles"""

    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Always return True"""
        return True
