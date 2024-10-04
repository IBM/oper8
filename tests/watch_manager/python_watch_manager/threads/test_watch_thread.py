"""
Tests for the WatchThread
"""
# Standard
from unittest.mock import patch
import time

# Third Party
import pytest

# Local
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.deploy_manager.kube_event import KubeEventType
from oper8.test_helpers.helpers import MockDeployManager, library_config
from oper8.test_helpers.pwm_helpers import (
    DisabledLeadershipManager,
    MockedReconcileThread,
    clear_caches,
    make_ownerref,
    make_resource,
)
from oper8.watch_manager.python_watch_manager.filters.filters import DisableFilter
from oper8.watch_manager.python_watch_manager.threads.watch import WatchThread
from oper8.watch_manager.python_watch_manager.utils.types import (
    ReconcileRequestType,
    ResourceId,
    WatchRequest,
)

## Helpers #####################################################################


@pytest.mark.timeout(5)
def test_watch_thread_happy_path():
    dm = DryRunDeployManager()
    watched_object = make_resource(spec={"test": "value"})
    watched_object_id = ResourceId.from_resource(watched_object)

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        watch_thread.start_thread()

        request = WatchRequest(
            # Set watched and requester to the same
            watched=watched_object_id,
            requester=watched_object_id,
        )
        watch_thread.request_watch(request)

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert mocked_reconcile_thread.get_request().type == KubeEventType.ADDED
        assert mocked_reconcile_thread.get_request().type == KubeEventType.MODIFIED


@pytest.mark.timeout(5)
def test_watch_thread_filtered():
    dm = DryRunDeployManager()
    watched_object = make_resource(spec={"test": "value"})
    watched_object_id = ResourceId.from_resource(watched_object)

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": DisableFilter}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        watch_thread.start_thread()

        request = WatchRequest(
            # Set watched and requester to the same
            watched=watched_object_id,
            requester=watched_object_id,
        )
        watch_thread.request_watch(request)

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert mocked_reconcile_thread.requests.empty()


@pytest.mark.timeout(5)
def test_watch_thread_deleted():
    dm = DryRunDeployManager()
    watched_object = make_resource(spec={"test": "value"})
    watched_object_id = ResourceId.from_resource(watched_object)

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        watch_thread.start_thread()

        request = WatchRequest(
            # Set watched and requester to the same
            watched=watched_object_id,
            requester=watched_object_id,
        )
        watch_thread.request_watch(request)

        dm.deploy([watched_object])
        dm.disable([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert mocked_reconcile_thread.get_request().type == KubeEventType.ADDED
        assert mocked_reconcile_thread.get_request().type == KubeEventType.DELETED


@pytest.mark.timeout(5)
def test_watch_thread_owner_watch():
    dm = DryRunDeployManager()
    owner_object = make_resource(
        kind="DifferentKind", name="owner", spec={"test": "value"}
    )
    owner_object_id = ResourceId.from_resource(owner_object)
    watched_object = make_resource(
        spec={"test": "value"}, owner_refs=[make_ownerref(owner_object)]
    )
    watched_object_id = ResourceId.from_resource(watched_object)

    # Deploy owner before watch has started
    dm.deploy([owner_object])

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        request = WatchRequest(
            # Set watched and requester to the same
            watched=watched_object_id,
            requester=owner_object_id,
        )
        watch_thread.request_watch(request)
        watch_thread.start_thread()

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert (
            mocked_reconcile_thread.get_request().type == ReconcileRequestType.DEPENDENT
        )
        assert (
            mocked_reconcile_thread.get_request().type == ReconcileRequestType.DEPENDENT
        )


@pytest.mark.timeout(5)
def test_watch_thread_global_watch():
    dm = DryRunDeployManager()
    owner_object = make_resource(
        kind="DifferentKind", name="owner", spec={"test": "value"}
    )
    watched_object = make_resource(
        spec={"test": "value"}, owner_refs=[make_ownerref(owner_object)]
    )
    watched_object_id = ResourceId.from_resource(watched_object)

    dm.deploy([owner_object])

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        watch_thread.start_thread()

        request = WatchRequest(
            # Set watched and requester to the same
            watched=watched_object_id,
            requester=ResourceId(
                api_version=owner_object.get("apiVersion"),
                kind=owner_object.get("kind"),
            ),
        )
        watch_thread.request_watch(request)

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(3)
        watch_thread.stop_thread()
        assert (
            mocked_reconcile_thread.get_request().type == ReconcileRequestType.DEPENDENT
        )
        assert (
            mocked_reconcile_thread.get_request().type == ReconcileRequestType.DEPENDENT
        )


@pytest.mark.timeout(5)
def test_watch_thread_all_events():
    dm = DryRunDeployManager()
    watched_object = make_resource(spec={"test": "value"})
    request_resource_id = ResourceId(
        api_version=watched_object.get("apiVersion"), kind=watched_object.get("kind")
    )

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        watch_thread.start_thread()

        request = WatchRequest(
            # Set watched and requester to the same
            watched=request_resource_id,
            requester=request_resource_id,
        )
        watch_thread.request_watch(request)

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])
        dm.deploy([make_resource(name="second_obj")])
        dm.disable([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert mocked_reconcile_thread.get_request().type == KubeEventType.ADDED
        assert mocked_reconcile_thread.get_request().type == KubeEventType.MODIFIED
        assert mocked_reconcile_thread.get_request().type == KubeEventType.ADDED
        assert mocked_reconcile_thread.get_request().type == KubeEventType.DELETED


@pytest.mark.timeout(5)
def test_watch_thread_global_watch_two_owners():
    dm = DryRunDeployManager()
    owner_object = make_resource(kind="OwnerKind", name="owner", spec={"test": "value"})
    owner_2_object = make_resource(
        kind="OwnerKind", name="owner2", spec={"test": "value"}
    )
    watched_object = make_resource(
        spec={"test": "value"},
        owner_refs=[make_ownerref(owner_object), make_ownerref(owner_2_object)],
    )
    watched_object_id = ResourceId.from_resource(watched_object)

    dm.deploy([owner_object])
    dm.deploy([owner_2_object])

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        request = WatchRequest(
            # Set watched and requester to the same
            watched=watched_object_id,
            requester=ResourceId(
                api_version=owner_object.get("apiVersion"),
                kind=owner_object.get("kind"),
            ),
        )
        watch_thread.request_watch(request)
        watch_thread.start_thread()

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()

        add_events = [
            mocked_reconcile_thread.get_request(),
            mocked_reconcile_thread.get_request(),
        ]
        assert "owner" in [event.resource.name for event in add_events]
        assert "owner2" in [event.resource.name for event in add_events]
        assert add_events[0].type == ReconcileRequestType.DEPENDENT
        assert add_events[1].type == ReconcileRequestType.DEPENDENT

        modified_events = [
            mocked_reconcile_thread.get_request(),
            mocked_reconcile_thread.get_request(),
        ]

        assert "owner" in [event.resource.name for event in modified_events]
        assert "owner2" in [event.resource.name for event in modified_events]
        assert modified_events[0].type == ReconcileRequestType.DEPENDENT
        assert modified_events[1].type == ReconcileRequestType.DEPENDENT


@pytest.mark.timeout(5)
def test_watch_thread_no_watch():
    dm = DryRunDeployManager()
    watched_object = make_resource(spec={"test": "value"})

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": DisableFilter}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        watch_thread.start_thread()

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert mocked_reconcile_thread.requests.empty()


@pytest.mark.timeout(5)
def test_watch_thread_not_leader():
    dm = DryRunDeployManager()
    watched_object = make_resource(spec={"test": "value"})
    watched_object_id = ResourceId.from_resource(watched_object)

    with library_config(
        python_watch_manager={"process_context": "fork", "filter": None}
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            leadership_manager=DisabledLeadershipManager(),
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )
        request = WatchRequest(watched=watched_object_id, requester=watched_object_id)
        watch_thread.request_watch(request)
        watch_thread.start_thread()

        dm.deploy([watched_object])
        watched_object["spec"] = {"test": "updated"}
        dm.deploy([watched_object])

        time.sleep(1.5)
        watch_thread.stop_thread()
        assert mocked_reconcile_thread.requests.empty()


@pytest.mark.timeout(5)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_watch_thread_invalid_rbac():
    dm = MockDeployManager(watch_raise=True)
    watched_object = make_resource(spec={"test": "value"})
    watched_object_id = ResourceId.from_resource(watched_object)

    with patch(
        "oper8.watch_manager.python_watch_manager.threads.watch.os._exit",
        side_effect=Exception("EndTest"),
    ) as exit_mock, library_config(
        python_watch_manager={
            "process_context": "fork",
            "filter": None,
            "watch_retry_count": 3,
            "watch_retry_delay": "0.1s",
        }
    ):
        mocked_reconcile_thread = MockedReconcileThread()
        watch_thread = WatchThread(
            reconcile_thread=mocked_reconcile_thread,
            kind="Foo",
            api_version="foo.bar.com/v1",
            namespace="test",
            deploy_manager=dm,
        )

        request = WatchRequest(watched=watched_object_id, requester=watched_object_id)
        watch_thread.request_watch(request)
        watch_thread.start_thread()

        # Wait for the retries
        time.sleep(1)

        # Assert we tried to watch 4 times (3 retries plus the initial)
        assert dm.watch_objects.call_count == 4
        assert exit_mock.called
