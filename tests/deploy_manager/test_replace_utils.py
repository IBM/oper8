"""
Tests for the replace_utils functionality
"""

# Standard

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.deploy_manager.replace_utils import _REPLACE_FUNCS, requires_replace

## Helpers #####################################################################

log = alog.use_channel("TEST")


def sample_object():
    return {
        "original_value": "original",
        "envs": [
            {
                "name": "first",
                "value": "True",
            },
            {
                "name": "second",
                "valueFrom": "False",
            },
        ],
        "dicts_in_lists": [
            {"someDict": {"someValue": "onetwo", "other": "threefour"}},
        ],
        "list": [
            {"name": "container1"},
            {"name": "container2"},
        ],
    }


## Replace functions ##################################################################


@pytest.mark.parametrize(
    ["desired_obj"],
    [
        [
            {
                "envs": [
                    {
                        "name": "first",
                        "valueFrom": "True",
                    },
                    {
                        "name": "second",
                        "valueFrom": "False",
                    },
                ],
            }
        ],
        [
            {
                "envs": [
                    {
                        "name": "first",
                        "value": "True",
                    },
                    {
                        "name": "second",
                        "value": "True",
                    },
                ],
            }
        ],
    ],
)
def test_value_operations(desired_obj):
    """Test that adding a ref to an object with none present adds as expected"""
    current_obj = sample_object()
    assert requires_replace(current_obj, desired_obj)
    # Ensure each replace function is still able to be called
    for func in _REPLACE_FUNCS:
        func(current_obj, desired_obj)


@pytest.mark.parametrize(
    ["desired_obj", "requires"],
    [
        [
            {
                "list": [
                    {"name": "container1"},
                    {"name": "container2"},
                    {"name": "container3"},
                ],
            },
            False,
        ],
        [
            {
                "dicts_in_lists": [
                    {"someDict": {"someValue": "onetwo", "other": "threefour"}},
                ],
            },
            False,
        ],
        [
            {
                "dicts_in_lists": [
                    {"someDict": {"someValue": "onetwo", "other": "threefour"}},
                    {"appendedDict": {"someValue": "onetwo", "other": "threefour"}},
                ],
            },
            False,
        ],
        [
            {
                "list": [
                    {"name": "container1"},
                ],
            },
            True,
        ],
        [
            {
                "list": [
                    {"name": "container1"},
                    {"name": "container_changed"},
                ],
            },
            True,
        ],
        [
            {
                "list": [
                    {"name": "container1"},
                    {"name": "container4"},
                ],
            },
            True,
        ],
    ],
)
def test_list_operations(desired_obj, requires) -> None:
    """Test that adding a ref to an object with none present adds as expected"""
    current_obj = sample_object()
    assert requires_replace(current_obj, desired_obj) == requires
    # Ensure each replace function is still able to be called
    for func in _REPLACE_FUNCS:
        func(current_obj, desired_obj)


## Patch functions ##################################################################


@pytest.mark.parametrize(
    ["desired_obj"],
    [
        [{"new_value": "patched"}],
        [{"added_list": [{"new_value"}]}],
        [{"original_value": "patched"}],
    ],
)
def test_patch_operations(desired_obj):
    current_obj = sample_object()
    assert not requires_replace(current_obj, desired_obj)
