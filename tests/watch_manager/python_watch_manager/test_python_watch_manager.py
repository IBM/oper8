"""
Tests for the Python Watch Manager
"""
# Standard
from contextlib import contextmanager
import time

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.test_helpers.helpers import DummyController
from oper8.test_helpers.pwm_helpers import MockedReconcileThread, make_managed_object
from oper8.watch_manager import PythonWatchManager, WatchManagerBase
from oper8.watch_manager.python_watch_manager.threads import watch
from oper8.watch_manager.python_watch_manager.threads.reconcile import ReconcileThread

## Helpers #####################################################################


class DummyPythonWatchManagerController(DummyController):
    version = "v1"


class SecondDummyPythonWatchManagerController(DummyController):
    version = "v2"


@pytest.fixture(autouse=True)
def reset_watch_base():
    yield
    watch.watch_threads = {}
    WatchManagerBase._ALL_WATCHES = {}


@contextmanager
def mock_reconcile_thread(deploy_manager):
    mock_reconcile_thread = MockedReconcileThread(deploy_manager=deploy_manager)
    ReconcileThread._instance = mock_reconcile_thread
    yield
    ReconcileThread._instance = None


@pytest.mark.timeout(8)
def test_python_watch_manager_happy_path():
    dm = DryRunDeployManager()

    with mock_reconcile_thread(dm):
        watch_manager = PythonWatchManager(
            DummyPythonWatchManagerController, deploy_manager=dm
        )
        assert watch_manager.watch()
        assert watch_manager.reconcile_thread.is_alive()

        for controller_watch in watch_manager.controller_watches:
            assert controller_watch.is_alive()

        resource = make_managed_object()
        time.sleep(1)
        dm.deploy([resource.definition])
        time.sleep(2)
        watch_manager.stop()
        assert watch_manager.reconcile_thread.processes_started == 1
        assert not watch_manager.reconcile_thread.is_alive()


@pytest.mark.timeout(8)
def test_python_watch_manager_namespaced():
    dm = DryRunDeployManager()

    with mock_reconcile_thread(dm):
        watch_manager = PythonWatchManager(
            DummyPythonWatchManagerController,
            deploy_manager=dm,
            namespace_list=["test"],
        )
        assert watch_manager.watch()
        assert watch_manager.reconcile_thread.is_alive()

        for controller_watch in watch_manager.controller_watches:
            assert controller_watch.is_alive()
        time.sleep(1)
        resource = make_managed_object()
        dm.deploy([resource.definition])
        time.sleep(2)
        watch_manager.stop()
        assert watch_manager.reconcile_thread.processes_started == 1
        assert not watch_manager.reconcile_thread.is_alive()


@pytest.mark.timeout(8)
def test_python_watch_manager_multiple_deploy():
    dm = DryRunDeployManager()

    with mock_reconcile_thread(dm):
        watch_manager = PythonWatchManager(
            DummyPythonWatchManagerController, deploy_manager=dm
        )
        second_watch_manager = PythonWatchManager(
            SecondDummyPythonWatchManagerController, deploy_manager=dm
        )
        assert watch_manager.watch()
        assert second_watch_manager.watch()
        assert watch_manager.reconcile_thread == second_watch_manager.reconcile_thread
        assert second_watch_manager.reconcile_thread.is_alive()

        for controller_watch in [
            *watch_manager.controller_watches,
            *second_watch_manager.controller_watches,
        ]:
            assert controller_watch.is_alive()

        time.sleep(1)
        resource = make_managed_object()
        dm.deploy([resource.definition])
        resource = make_managed_object(api_version="foo.bar.com/v2")
        dm.deploy([resource.definition])
        time.sleep(2)
        watch_manager.stop()
        assert watch_manager.reconcile_thread.processes_started == 2
        assert not watch_manager.reconcile_thread.is_alive()


def test_python_watch_manager_singleton():
    dm = DryRunDeployManager()
    watch_manager = PythonWatchManager(DummyPythonWatchManagerController, dm)
    second_watch_manager = PythonWatchManager(DummyController, dm)

    assert id(watch_manager.reconcile_thread) == id(
        second_watch_manager.reconcile_thread
    )
