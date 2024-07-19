"""
Tests for the replace_utils functionality
"""

# Standard

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.deploy_manager.replace_utils import requires_replace, REPLACE_FUNCS

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
    for func in REPLACE_FUNCS:
        func(current_obj, desired_obj)


@pytest.mark.parametrize(
    ["desired_obj"],
    [
        [
            {
                "list": [
                    {"name": "container1"},
                    {"name": "container2"},
                    {"name": "container3"},
                ],
            }
        ],
        [
            {
                "list": [
                    {"name": "container1"},
                ],
            }
        ],
        [
            {
                "list": [
                    {"name": "container1"},
                    {"name": "container_changed"},
                ],
            }
        ],
    ],
)
def test_list_operations(desired_obj):
    """Test that adding a ref to an object with none present adds as expected"""
    current_obj = sample_object()
    assert requires_replace(current_obj, desired_obj)
    # Ensure each replace function is still able to be called
    for func in REPLACE_FUNCS:
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
