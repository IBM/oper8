"""
Tests for common PWM utils
"""
# Standard
from datetime import timedelta
from unittest import mock
from uuid import uuid4
import logging

# Third Party
import pytest

# Local
from oper8.test_helpers.helpers import library_config
from oper8.watch_manager.python_watch_manager.utils import common


@pytest.mark.parametrize(
    "test_cfg",
    [
        ("12hr13m14s", timedelta(hours=12, minutes=13, seconds=14)),
        ("0hr0m62s", timedelta(minutes=1, seconds=2)),
        ("0hr0m62.5s", timedelta(minutes=1, seconds=2.5)),
        ("10s", timedelta(seconds=10)),
        ("foobar", None),
        ("0hr0m1.2.3s", None),
    ],
)
def test_parse_time_delta(test_cfg):
    """Make sure that the parser behaves as expected for valid and invalid
    timedeltas
    """
    time_str, result = test_cfg
    assert common.parse_time_delta(time_str) == result


def test_get_operator_namespace_from_file():
    ns_name = "some-namespace"
    other_ns_name = "other-namespace"
    path_mock = mock.MagicMock()
    path_mock.is_file = mock.MagicMock(return_value=True)
    path_mock.read_text = mock.MagicMock(return_value=ns_name)
    with mock.patch("pathlib.Path", return_value=path_mock), library_config(
        python_watch_manager={"lock": {"namespace": other_ns_name}}
    ):
        assert common.get_operator_namespace() == ns_name
        assert path_mock.is_file.called
        assert path_mock.read_text.called


def test_get_operator_namespace_from_config():
    ns_name = "some-namespace"
    other_ns_name = "other-namespace"
    path_mock = mock.MagicMock()
    path_mock.is_file = mock.MagicMock(return_value=False)
    path_mock.read_text = mock.MagicMock(return_value=ns_name)
    with mock.patch("pathlib.Path", return_value=path_mock), library_config(
        python_watch_manager={"lock": {"namespace": other_ns_name}}
    ):
        assert common.get_operator_namespace() == other_ns_name
        assert path_mock.is_file.called
        assert not path_mock.read_text.called


def test_get_logging_handler_adds_stream_handlers():
    """Make sure that get_logging_handlers adds a stream handler by default if
    no other handlers configured
    """
    logger = logging.Logger(str(uuid4()))
    with mock.patch("logging.getLogger", return_value=logger):
        returned_handlers = common.get_logging_handlers()
    assert len(returned_handlers) == 1
    assert isinstance(returned_handlers[0], logging.StreamHandler)


def test_get_logging_handler_does_not_overwrite_other_handlers():
    """Make sure that get_logging_handlers does not change preconfigured
    handlers
    """
    logger = logging.Logger(str(uuid4()))
    logger.addHandler(logging.NullHandler())
    with mock.patch("logging.getLogger", return_value=logger):
        returned_handlers = common.get_logging_handlers()
    assert len(returned_handlers) == 1
    assert isinstance(returned_handlers[0], logging.NullHandler)
