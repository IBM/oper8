"""
Tests for the TimerThread
"""
# Standard
from datetime import datetime, timedelta
import time

# Third Party
import pytest

# Local
from oper8.test_helpers.pwm_helpers import MockedTimerThread

## Helpers #####################################################################


class Counter:
    def __init__(self, initial_value=0):
        self.value = initial_value

    def increment(self, value=1):
        self.value += value


@pytest.mark.timeout(5)
def test_timer_thread_happy_path():
    timer = MockedTimerThread()
    timer.start_thread()

    value_tracker = Counter()
    timer.put_event(datetime.now(), value_tracker.increment)
    timer.put_event(datetime.now() + timedelta(seconds=0.1), value_tracker.increment)
    timer.put_event(datetime.now() + timedelta(seconds=0.2), value_tracker.increment, 2)
    timer.put_event(
        datetime.now() + timedelta(seconds=0.3), value_tracker.increment, value=2
    )
    time.sleep(2.5)
    timer.stop_thread()
    assert value_tracker.value == 6


@pytest.mark.timeout(5)
def test_timer_thread_canceled():
    timer = MockedTimerThread()

    value_tracker = Counter()
    timer.put_event(datetime.now(), value_tracker.increment)
    canceled_event = timer.put_event(
        datetime.now() + timedelta(seconds=0.5), value_tracker.increment
    )
    canceled_event.cancel()

    timer.start_thread()
    time.sleep(2)
    timer.stop_thread()
    assert value_tracker.value == 1
