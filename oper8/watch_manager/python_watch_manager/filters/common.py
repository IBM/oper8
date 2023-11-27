"""Common functions used for interacting with filters including default filter classes"""

# Standard
from functools import lru_cache
from typing import List, Type
import importlib
import inspect

# First Party
import alog

# Local
from .... import config
from ....exceptions import ConfigError
from .filters import (
    AnnotationFilter,
    CreationDeletionFilter,
    EnableFilter,
    Filter,
    GenerationFilter,
    NoGenerationFilter,
    PauseFilter,
    ResourceVersionFilter,
    UserAnnotationFilter,
)
from .manager import AndFilter, OrFilter

log = alog.use_channel("PWMFLTCOM")


### Factory Filter Classes

# Usable Default Filter Classes.
DEFAULT_FILTER_CLASS = AndFilter(
    CreationDeletionFilter,
    GenerationFilter,
    NoGenerationFilter,
    ResourceVersionFilter,
    PauseFilter,
)
ANNOTATION_FILTER_CLASS = OrFilter(DEFAULT_FILTER_CLASS, AnnotationFilter)
USER_ANNOTATION_FILTER_CLASS = OrFilter(DEFAULT_FILTER_CLASS, UserAnnotationFilter)

FILTER_CLASSES = {
    "default": DEFAULT_FILTER_CLASS,
    "annotation": ANNOTATION_FILTER_CLASS,
    "user-annotation": USER_ANNOTATION_FILTER_CLASS,
}


# Forward Declarations
CONTROLLER_TYPE = "Controller"
CONTROLLER_CLASS_TYPE = Type[CONTROLLER_TYPE]
RESOURCE_ID_TYPE = "ResourceId"

### Factory Filter Functions


# Only compute the filters once to avoid reimporting/regathering
@lru_cache(maxsize=1)
def get_configured_filter() -> Filter:
    """Get the default filter that should be applied to every resource

    Returns:
        default_filter: Filter
            The default filter specified in the Config"""

    filter_name = config.python_watch_manager.filter

    # Check for filter in default list or attempt to
    # manually import one
    if filter_name in FILTER_CLASSES:
        filter_obj = FILTER_CLASSES[filter_name]
    elif inspect.isclass(filter_name) and issubclass(filter_name, Filter):
        filter_obj = filter_name
    elif isinstance(filter_name, str):
        filter_obj = import_filter(filter_name)
    # If no filter is provided then always enable
    else:
        filter_obj = EnableFilter

    log.debug2(f"Found filter: {filter_obj}")
    return filter_obj


def get_filters_for_resource_id(
    controller_type: CONTROLLER_CLASS_TYPE, resource_id: RESOURCE_ID_TYPE
) -> List[Filter]:
    """Get the filters for a particular resource_id given a controller_type

    Args:
        controller_type: CONTROLLER_CLASS_TYPE
            The controller type whose filters we're inspecting
        resource_id: "ResourceId"
            The requested resource

    Returns:
        filter_list: List[Filter]
            The list of filters to be applied
    """
    filters = getattr(controller_type, "pwm_filters", [])

    if isinstance(filters, list):
        return_filters = filters

    elif isinstance(filters, dict):
        return_filters = filters.get(resource_id.global_id, [])

    else:
        raise ConfigError(f"Invalid type for PWM filters: {type(filters)}")

    log.debug3(f"Found filters {return_filters} for resource: {resource_id}")
    return return_filters


### Helper Functions


def import_filter(filter_name: str) -> Filter:
    """Import a filter from a string reference

    Args:
        filter_name: str
            Filter name in <module>.<filter> form

    Returns:
        imported_filter: Filter
            The filter that was requested
    """
    module_path, class_name = filter_name.rsplit(".", 1)
    try:
        filter_module = importlib.import_module(module_path)
        filter_obj = getattr(filter_module, class_name)
    except (ImportError, AttributeError) as exc:
        raise ConfigError(
            f"Invalid Filter: {filter_name}. Module or class not found"
        ) from exc

    if (
        inspect.isclass(filter_obj) and not issubclass(filter_obj, Filter)
    ) and not isinstance(filter_obj, (Filter, list, tuple)):
        raise ConfigError(f"{filter_obj} is not a instance of {Filter}")

    return filter_obj
