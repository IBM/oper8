"""
Tests for the FilterManager
"""
# Third Party
import pytest

# First Party
import alog

# Local
from oper8.deploy_manager.kube_event import KubeEventType
from oper8.test_helpers.pwm_helpers import make_managed_object
from oper8.watch_manager.python_watch_manager.filters.filters import (
    CreationDeletionFilter,
    DisableFilter,
    EnableFilter,
    NoGenerationFilter,
)
from oper8.watch_manager.python_watch_manager.filters.manager import (
    AndFilter,
    FilterManager,
    OrFilter,
)

## Helpers #####################################################################


def test_manager_happy_path():
    resource = make_managed_object(generation=None)
    filter = OrFilter(CreationDeletionFilter, NoGenerationFilter)
    fm = FilterManager(filter, resource)

    assert fm.update_and_test(resource, KubeEventType.ADDED)
    updated_resource = make_managed_object(spec={"modified": "value"})
    assert fm.update_and_test(updated_resource, KubeEventType.MODIFIED)
    assert not fm.update_and_test(updated_resource, KubeEventType.MODIFIED)


@pytest.mark.parametrize(
    ["filters", "result"],
    [
        [EnableFilter, True],
        [AndFilter(EnableFilter, DisableFilter), False],
        [OrFilter(EnableFilter, DisableFilter), True],
        [AndFilter(OrFilter(EnableFilter, DisableFilter), EnableFilter), True],
        [AndFilter(OrFilter(DisableFilter, DisableFilter), EnableFilter), False],
    ],
)
def test_manager_conditionals(filters, result):
    resource = make_managed_object()
    fm = FilterManager(filters, resource)
    assert fm.test(resource, KubeEventType.ADDED) == result


def test_manager_to_info():
    filters = OrFilter(CreationDeletionFilter, NoGenerationFilter)

    filter_info = FilterManager.to_info(filters)
    round_tripped_filters = FilterManager.from_info(filter_info)

    assert round_tripped_filters == filters
