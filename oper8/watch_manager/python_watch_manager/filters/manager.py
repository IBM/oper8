"""Module contains helpers for processing a group of filters"""
# Standard
from typing import Any, Callable, List, Optional, Tuple, Type, Union
import inspect
import operator

# First Party
import alog

# Local
from ....deploy_manager import KubeEventType
from ....managed_object import ManagedObject
from ..utils import ClassInfo
from .filters import Filter

log = alog.use_channel("PWMFLTMAN")


## Conditional filters


def AndFilter(*args):  # pylint: disable=invalid-name
    """An "And" Filter is just a list of filters"""
    return list(args)


def OrFilter(*args):  # pylint: disable=invalid-name
    """An "Or" Filter is just a tuple of filters"""
    return tuple(args)


class FilterManager(Filter):
    """The FilterManager class helps process conditional filters and groups of filters.
    Filters that in a list are "anded" together while Filters in a tuple are "ored".
    This class also contains helpers to recursively convert between ClassInfo and Filters.
    """

    def __init__(
        self,
        filters: Union[List[Type[Filter]], Tuple[Type[Filter]]],
        resource: ManagedObject,
    ):
        """Initialize all filters in the provided group

        Args:
            filters: Union[List[Type[Filter]], Tuple[Type[Filter]]]
                The filters to manage
            resource: ManagedObject
                The initial resource
        """
        self.filters = self.__recursive_filter_init(filters, resource)

    ### Public Interface

    def update_and_test(
        self, resource: ManagedObject, event: KubeEventType
    ) -> Optional[bool]:
        """Recursively update and test each filter"""
        return self.__recursive_update_and_test(self.filters, resource, event)

    def test(self, resource: ManagedObject, event: KubeEventType) -> Optional[bool]:
        """Recursively test each filter"""
        # test with test_only set to True so nothing is updated
        return self.__recursive_update_and_test(
            self.filters, resource, event, test_only=True
        )

    def update(self, resource: ManagedObject):
        """Update each filter recursively"""
        # Update with update_only set to True so no tests are ran
        self.__recursive_update_and_test(  # pylint: disable=redundant-keyword-arg
            self, self.filters, resource, None, update_only=True
        )

    @classmethod
    def to_info(cls, filters: Type[Filter]) -> Type[ClassInfo]:
        """Helper function to convert from filters to ClassInfos. This is used for pickling and IPC

        Args:
            filters: Type[Filter]
                The filters to convert

        Returns:
            class_info: Type[ClassInfo]
                The class info objects describing the filter
        """
        return cls.__recursive_filter_info(filters)

    @classmethod
    def from_info(cls, info: Type[ClassInfo]) -> Type[Filter]:
        """Helper function to convert from ClassInfos to a filter


        Args:
            class_info: Type[ClassInfo]
                The classinfos to convert back into filters

        Returns:
            filters: Type[Filter]
                The converted filter objects
        """
        return cls.__recursive_filter_info(info)

    ### Private Helper Functions
    @classmethod
    def __recursive_filter_info(
        cls, descriptor: Union[Type[Filter], Type[ClassInfo]]
    ) -> Union[Type[Filter], Type[ClassInfo]]:
        """Recursive helper to convert from filters to class infos and back

        Args:
            descriptor: Union[Type[Filter],Type[ClassInfo]]
                Either the filter or class_info to convert

        Returns:
            type: Union[Type[Filter],Type[ClassInfo]]
                The converted types
        """

        def convert_filter_type(descriptor):
            """Generic function to convert between types"""

            # If we get a filter than we're converting to ClassInfo else
            # we're converting back to Filters
            if inspect.isclass(descriptor) and issubclass(descriptor, Filter):
                return ClassInfo.from_type(descriptor)
            if isinstance(descriptor, ClassInfo):
                return descriptor.to_class()
            # The instance must be a list or a tuple to be processed
            raise ValueError(
                f"Unknown type: {type(descriptor)} {descriptor} passed to convert_filter_type"
            )

        return cls.__recursive_map(descriptor, convert_filter_type)

    def __recursive_filter_init(
        self,
        filters: Union[List[Type[Filter]], Tuple[Type[Filter]], Type[Filter]],
        resource: ManagedObject,
    ) -> Union[List[Filter], Tuple[Filter], Filter]:
        """Helper function to recursively init each filter

        Args:
            filters: Union[List[Type[Filter]], Tuple[Type[Filter]], Type[Filter]]
                The filters to be initialized
            resource: ManagedObject
                The resource to pass to the filters
        Returns:
            filters: Union[List[Filter], Tuple[Filter], Filter]
                The initalized filters
        """

        def init_filter(filter_type: Type[Filter]) -> Filter:
            if not (inspect.isclass(filter_type) and issubclass(filter_type, Filter)):
                raise ValueError(
                    f"Unknown type: {type(filter_type)} passed to init_filter"
                )

            return filter_type(resource)

        return self.__recursive_map(filters, init_filter)

    def __recursive_update_and_test(  # pylint: disable=too-many-arguments, inconsistent-return-statements
        self,
        filters: Union[list, tuple, Filter],
        resource: ManagedObject,
        event: KubeEventType,
        update_only: bool = False,
        test_only: bool = False,
    ) -> Optional[bool]:
        """Helper function to recursively update, test, or both.

        Args:
            filters: Union[list, tuple, Filter]
                The current filters being tested. This is updated when recurring
            resource: ManagedObject
                The current resource being updated/tested
            event: KubeEventType,
                The current event type being updated/tested
            update_only: bool = False
                Whether to only update the filters
            test_only: bool = False,
                Whether to only test the filters

        Returns:
            result: Optional[bool]
                The result of the tests if it was ran
        """
        if update_only and test_only:
            raise ValueError("update_only and test_only can not both be True")

        # Check Initial object types and exit condition
        if isinstance(filters, Filter):
            # If instance is a filter than call either update or update_and_test
            # depending on the failed status
            if update_only:
                filters.update(resource)
                return
            if test_only:
                return filters.test(resource, event)

            return filters.update_and_test(resource, event)

        # If filter list is empty then immediately return success
        if not filters:
            return True

        return_value = None
        operation = operator.and_ if isinstance(filters, list) else operator.or_

        for filter_combo in filters:
            # Recursively processes the filter combo
            result = self.__recursive_update_and_test(
                filter_combo, resource, event, update_only, test_only
            )

            # If return_value has already been set then combine it with the most recent result
            # via the operation
            if result is not None:
                if return_value is not None:
                    return_value = operation(return_value, result)
                else:
                    return_value = result

            # There are two scenarios when filters only need to get updated not tested. The first
            # is when an "and" condition fails or when an "or" succeeds. In both instances we
            # know the end result so testing can be skipped for performance
            if (
                (not update_only and not test_only)
                and result
                and (
                    (operation == operator.and_ and not result)
                    or (operation == operator.or_ and result)
                )
            ):
                update_only = True

        # If no filter cared about the event then don't
        # reconcile
        if return_value is None:
            return False

        return return_value

    @classmethod
    def __recursive_map(
        cls, filters: Union[List[Any], Tuple[Any]], operation: Callable[[Filter], Any]
    ):
        """Helper function to map an operation onto every object in a filter chain

        Args:
            filters: Union[List[Any], Tuple[Any]]
                The filters to map onto
            op: Callable[[Filter],None]
                The function to map onto each filter
        """

        # Directly check tuple to ignore NamedTuples and subclasses
        if not (isinstance(filters, list) or type(filters) is tuple):
            return operation(filters)

        filter_list = []
        for filter_obj in filters:
            filter_list.append(cls.__recursive_map(filter_obj, operation))

        # Ensure the returned iterable is the same type as the original
        return type(filters)(filter_list)
