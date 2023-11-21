"""
Tests for the WatchManagerBase base class
"""

# Standard
import threading
import time

# Third Party
import pytest

# Local
from oper8.test_helpers.helpers import DummyController
from oper8.watch_manager.base import WatchManagerBase

## Helpers #####################################################################


class DummyWatchManager(WatchManagerBase):
    def __init__(
        self,
        controller_type,
        watch_success=True,
        stop_wait=0.0,
    ):
        super().__init__(controller_type)
        self.watching = False
        self.watch_success = watch_success
        self.stop_wait = stop_wait

    def watch(self):
        if self.watch_success:
            self.watching = True
            return True
        return False

    def wait(self):
        while self.watching:
            time.sleep(0.05)

    def stop(self):
        if self.stop_wait:
            threading.Thread(target=self._delayed_stop).start()
        else:
            self.watching = False

    def _delayed_stop(self):
        time.sleep(self.stop_wait)
        self.watching = False


class DummyController2(DummyController):
    group = "asdf.qwer"
    version = "v1"
    kind = "Widget"


@pytest.fixture(autouse=True)
def reset_globals():
    """This helper is only used in tests to "reset" the state of the global
    watches dict
    """
    WatchManagerBase._ALL_WATCHES = {}


## Tests #######################################################################


def test_constructor_properties():
    """Test that the base class properties are set on the watch manager"""
    wm = DummyWatchManager(DummyController)
    assert wm.controller_type == DummyController
    assert wm.group == DummyController.group
    assert wm.version == DummyController.version
    assert wm.kind == DummyController.kind


def test_constructor_registrations():
    """Test that all constructed watch managers get registered"""
    wm1 = DummyWatchManager(DummyController)
    wm2 = DummyWatchManager(DummyController2)
    assert len(WatchManagerBase._ALL_WATCHES) == 2
    assert str(wm1) in WatchManagerBase._ALL_WATCHES
    assert str(wm2) in WatchManagerBase._ALL_WATCHES


def test_constructor_no_duplicate_watches():
    """Test that all constructed watch managers get registered"""
    DummyWatchManager(DummyController)
    with pytest.raises(AssertionError):
        DummyWatchManager(DummyController)


def test_start_stop_all_blocking():
    """Test that calling start_all and stop_all with block set to true do indeed
    block and correctly start/stop all managers
    """
    wm1 = DummyWatchManager(DummyController)
    wm2 = DummyWatchManager(DummyController2)

    # Run start_all in a thread so that we can stop it
    thrd = threading.Thread(target=WatchManagerBase.start_all)
    thrd.start()
    time.sleep(0.1)

    # Make sure both watch managers are watching
    assert wm1.watching
    assert wm2.watching

    # Make sure the thread is blocked
    assert thrd.is_alive()

    # Stop all watch managers, and make sure all watch managers are not watching
    WatchManagerBase.stop_all()
    assert not wm1.watching
    assert not wm2.watching


def test_start_all_blocking_failure():
    """Test that calling start_all when one of the managers fails to start
    cleanly shuts down any started managers
    """
    # NOTE: failure is on wm1 because it comes second alphabetically
    wm1 = DummyWatchManager(DummyController, watch_success=False)
    wm2 = DummyWatchManager(DummyController2)

    # Start them and make sure it returns failure
    assert not WatchManagerBase.start_all()
    assert not wm1.watching
    assert not wm2.watching
