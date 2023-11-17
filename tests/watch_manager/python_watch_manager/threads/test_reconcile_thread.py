"""
Tests for the ReconcileThread
"""
# Standard
from datetime import timedelta
import time

# Third Party
import pytest

# Local
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.deploy_manager.kube_event import KubeEventType
from oper8.reconcile import ReconciliationResult, RequeueParams
from oper8.test_helpers.helpers import library_config
from oper8.test_helpers.pwm_helpers import MockedReconcileThread, make_managed_object
from oper8.watch_manager.python_watch_manager.utils.types import (
    ReconcileRequest,
    ReconcileRequestType,
    ResourceId,
    WatchRequest,
)

## Helpers #####################################################################


@pytest.mark.timeout(5)
def test_reconcile_thread_happy_path():
    dm = DryRunDeployManager()
    result = ReconciliationResult(requeue=False)
    reconcile_thread = MockedReconcileThread(
        deploy_manager=dm, returned_messages=[[result]]
    )

    resource = make_managed_object()
    dm.deploy([resource.definition])
    reconcile_thread.start_thread()

    request = ReconcileRequest(
        controller_type=None, type=KubeEventType.ADDED, resource=resource
    )
    reconcile_thread.push_request(request)

    time.sleep(3)
    reconcile_thread.stop_thread()
    # Make sure the process was started
    assert reconcile_thread.processes_started == 1
    assert reconcile_thread.processes_finished == 1
    # Make sure a periodic timer event was created
    assert reconcile_thread.timer_events.qsize() == 1
    assert (
        reconcile_thread.timer_events.get().args[0].type
        == ReconcileRequestType.PERIODIC
    )


@pytest.mark.timeout(5)
def test_reconcile_requeue():
    dm = DryRunDeployManager()
    requeue_result = ReconciliationResult(
        requeue=True, requeue_params=RequeueParams(requeue_after=timedelta(seconds=1))
    )
    end_result = ReconciliationResult(requeue=False)
    reconcile_thread = MockedReconcileThread(
        deploy_manager=dm,
        subprocess_wait_time=0,
        returned_messages=[[requeue_result], [end_result]],
    )

    resource = make_managed_object()
    dm.deploy([resource.definition])
    reconcile_thread.start_thread()

    request = ReconcileRequest(
        controller_type=None, type=KubeEventType.ADDED, resource=resource
    )
    reconcile_thread.push_request(request)

    time.sleep(3)
    reconcile_thread.stop_thread()
    # Make sure the process was started
    assert reconcile_thread.processes_started == 2
    assert reconcile_thread.processes_finished == 2
    # Make sure a periodic timer event was created
    assert reconcile_thread.timer_events.qsize() == 2
    assert (
        reconcile_thread.timer_events.get().args[0].type
        == ReconcileRequestType.REQUEUED
    )
    assert (
        reconcile_thread.timer_events.get().args[0].type
        == ReconcileRequestType.PERIODIC
    )


@pytest.mark.timeout(5)
def test_reconcile_overflow():
    dm = DryRunDeployManager()
    result = ReconciliationResult(requeue=False)
    with library_config(
        python_watch_manager={"process_context": "fork", "max_concurrent_reconciles": 2}
    ):
        reconcile_thread = MockedReconcileThread(
            deploy_manager=dm, subprocess_wait_time=2, returned_messages=[[result]]
        )

        resource = make_managed_object()
        dm.deploy([resource.definition])
        first_request = ReconcileRequest(
            controller_type=None, type=KubeEventType.ADDED, resource=resource
        )
        resource = make_managed_object(name="second")
        dm.deploy([resource.definition])
        second_request = ReconcileRequest(
            controller_type=None, type=KubeEventType.ADDED, resource=resource
        )
        resource = make_managed_object(name="overflow")
        dm.deploy([resource.definition])
        overflow_request = ReconcileRequest(
            controller_type=None, type=KubeEventType.ADDED, resource=resource
        )
        # Push requests before starting thread to fill queue before it can be
        # emptied
        reconcile_thread.push_request(first_request)
        reconcile_thread.push_request(second_request)
        reconcile_thread.push_request(overflow_request)
        reconcile_thread.start_thread()

        # Stop thread and assert only 2 processes were started
        time.sleep(0.5)
        reconcile_thread.stop_thread()
        assert reconcile_thread.processes_started == 2


@pytest.mark.timeout(8)
def test_reconcile_overflow_handled():
    dm = DryRunDeployManager()
    result = ReconciliationResult(requeue=False)
    with library_config(
        python_watch_manager={"process_context": "fork", "max_concurrent_reconciles": 1}
    ):
        reconcile_thread = MockedReconcileThread(
            deploy_manager=dm, subprocess_wait_time=0, returned_messages=[[result]]
        )

        resource = make_managed_object()
        dm.deploy([resource.definition])
        first_request = ReconcileRequest(
            controller_type=None, type=KubeEventType.ADDED, resource=resource
        )
        resource = make_managed_object(name="second")
        dm.deploy([resource.definition])
        second_request = ReconcileRequest(
            controller_type=None, type=KubeEventType.ADDED, resource=resource
        )
        resource = make_managed_object(name="overflow")
        dm.deploy([resource.definition])
        third_request = ReconcileRequest(
            controller_type=None, type=KubeEventType.ADDED, resource=resource
        )
        # Push requests before starting thread to fill queue before it can be
        # emptied
        reconcile_thread.push_request(first_request)
        reconcile_thread.push_request(second_request)
        reconcile_thread.push_request(third_request)
        reconcile_thread.start_thread()

        # Stop thread and assert that the reconcile thread was able to handle all requests
        time.sleep(5)
        reconcile_thread.stop_thread()
        assert reconcile_thread.processes_started == 3


@pytest.mark.timeout(5)
def test_reconcile_pending():
    dm = DryRunDeployManager()
    result = ReconciliationResult(requeue=False)
    reconcile_thread = MockedReconcileThread(
        deploy_manager=dm,
        subprocess_wait_time=1,
        returned_messages=[[result], [result], [result]],
    )

    resource = make_managed_object()
    dm.deploy([resource.definition])
    add_request = ReconcileRequest(
        controller_type=None, type=KubeEventType.ADDED, resource=resource
    )
    modified_request = ReconcileRequest(
        controller_type=None, type=KubeEventType.MODIFIED, resource=resource
    )
    duplicate_modified_request = ReconcileRequest(
        controller_type=None, type=KubeEventType.MODIFIED, resource=resource
    )
    deleted_requested = ReconcileRequest(
        controller_type=None, type=KubeEventType.DELETED, resource=resource
    )
    # Push requests before starting thread to fill queue
    reconcile_thread.push_request(add_request)
    reconcile_thread.push_request(modified_request)
    reconcile_thread.push_request(duplicate_modified_request)
    reconcile_thread.push_request(deleted_requested)
    reconcile_thread.start_thread()

    # Stop thread and assert that the reconcile thread combined all requests
    time.sleep(3)
    reconcile_thread.stop_thread()
    assert reconcile_thread.processes_started == 2
    assert reconcile_thread.processes_finished == 2


@pytest.mark.timeout(5)
def test_reconcile_watch_request():
    dm = DryRunDeployManager()

    result = ReconciliationResult(requeue=False)
    resource = make_managed_object()
    dm.deploy([resource.definition])

    watch_request = WatchRequest(
        watched=ResourceId.from_resource(resource),
        requester=ResourceId.from_resource(resource),
        controller_type=None,
    )
    reconcile_thread = MockedReconcileThread(
        deploy_manager=dm,
        subprocess_wait_time=0,
        returned_messages=[[result, watch_request]],
    )
    reconcile_thread.start_thread()

    request = ReconcileRequest(
        controller_type=None, type=KubeEventType.ADDED, resource=resource
    )
    # Push requests before starting thread to fill queue
    reconcile_thread.push_request(request)

    # Stop thread and assert that the reconcile thread combined all requests
    time.sleep(2)
    reconcile_thread.stop_thread()
    assert reconcile_thread.processes_started == 1
    assert reconcile_thread.processes_finished == 1
    assert reconcile_thread.watch_threads_created == 1
