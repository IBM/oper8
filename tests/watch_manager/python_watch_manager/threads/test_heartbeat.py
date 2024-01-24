"""
Tests for the HeartbeatThread
"""
# Standard
from datetime import datetime, timedelta
from unittest import mock

# Local
from oper8.test_helpers.pwm_helpers import (
    MockedHeartbeatThread,
    heartbeat_file,
    read_heartbeat_file,
)

## Helpers #####################################################################


class FailOnceOpen:
    def __init__(self, fail_on: int = 1):
        self.call_num = 0
        self.fail_on = fail_on
        self._real_open = open

    def __call__(self, *args, **kwargs):
        self.call_num += 1
        if self.call_num == self.fail_on:
            print(f"Raising on call {self.call_num}")
            raise OSError("Yikes")
        print(f"Returning from call {self.call_num}")
        return self._real_open(*args, **kwargs)


## Tests #######################################################################


def test_heartbeat_happy_path(heartbeat_file):
    """Make sure the heartbeat initializes correctly"""
    hb = MockedHeartbeatThread(heartbeat_file, "1s")

    # Heartbeat not run until started
    with open(heartbeat_file) as handle:
        assert not handle.read()

    # Start and stop the thread to trigger the first heartbeat only
    hb.start_thread()
    hb.wait_for_beat()
    hb.stop_thread()

    # Make sure the heartbeat is "current"
    assert read_heartbeat_file(heartbeat_file) > (datetime.now() - timedelta(seconds=5))


def test_heartbeat_ongoing(heartbeat_file):
    """Make sure that the heartbeat continues to beat in an ongoing way"""
    hb = MockedHeartbeatThread(heartbeat_file, "1s")

    # Start the thread and read the first one
    hb.start_thread()
    hb.wait_for_beat()
    first_hb = read_heartbeat_file(heartbeat_file)

    # Wait a bit and read again
    hb.wait_for_beat()
    hb.stop_thread()
    later_hb = read_heartbeat_file(heartbeat_file)
    assert later_hb > first_hb


def test_heartbeat_with_exception(heartbeat_file):
    """Make sure that a sporadic failure does not terminate the heartbeat"""
    # Mock so that the third call to open will raise. This correlates with the
    # second heartbeat since we read the file using open after each heartbeat
    with mock.patch("builtins.open", new=FailOnceOpen(3)):
        hb = MockedHeartbeatThread(heartbeat_file, "1s")
        hb.start_thread()

        # The first beat succeeds
        hb.wait_for_beat()
        first_hb = read_heartbeat_file(heartbeat_file)

        # The first beat raises, but doesn't cause any problems
        hb.wait_for_beat()
        second_hb = read_heartbeat_file(heartbeat_file)

        # The third beat succeeds
        hb.wait_for_beat()
        third_hb = read_heartbeat_file(heartbeat_file)
        hb.stop_thread()

        assert first_hb == second_hb
        assert third_hb > first_hb
