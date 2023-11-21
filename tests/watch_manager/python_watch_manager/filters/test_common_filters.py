"""
Tests for the common filter functions
"""
# Third Party
import pytest

# First Party
import alog

# Local
from oper8.exceptions import ConfigError
from oper8.watch_manager.python_watch_manager.filters.common import import_filter
from oper8.watch_manager.python_watch_manager.filters.filters import EnableFilter

## Helpers #####################################################################


def test_import_filter():
    assert (
        import_filter(
            "oper8.watch_manager.python_watch_manager.filters.filters.EnableFilter"
        )
        == EnableFilter
    )


def test_import_filter_fail():
    with pytest.raises(ConfigError):
        import_filter(
            "oper8.watch_manager.python_watch_manager.filters.filters.DoesNotExist"
        )
