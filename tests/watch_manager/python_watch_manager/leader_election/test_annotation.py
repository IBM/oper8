"""
Tests for the AnnotationLeadershipManager
"""
# Standard
from datetime import datetime, timedelta
import threading
import time

# Third Party
import pytest

# Local
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.managed_object import ManagedObject
from oper8.test_helpers.helpers import library_config
from oper8.test_helpers.pwm_helpers import make_managed_object
from oper8.watch_manager.python_watch_manager.leader_election.annotation import (
    AnnotationLeadershipManager,
)

## Helpers #####################################################################


@pytest.mark.timeout(5)
def test_annotation_happy_path():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    with library_config(
        pod_name=resource.name,
        python_watch_manager={
            "lock": {
                "name": "test",
                "poll_time": "1s",
                "duration": "10s",
                "namespace": resource.namespace,
            }
        },
    ):
        lm = AnnotationLeadershipManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()

        child_resource = make_managed_object()
        dm.deploy([child_resource.definition])
        assert lm.acquire_resource(child_resource)
        _, updated_object = dm.get_object_current_state(
            child_resource.kind,
            child_resource.name,
            child_resource.namespace,
            child_resource.api_version,
        )
        assert lm.is_leader(ManagedObject(updated_object))
        assert (
            updated_object.get("metadata", {})
            .get("annotations", {})
            .get("oper8.org/lease-name")
            == resource.name
        )

        assert lm.release_resource(ManagedObject(updated_object))
        _, updated_object = dm.get_object_current_state(
            child_resource.kind,
            child_resource.name,
            child_resource.namespace,
            child_resource.api_version,
        )
        assert (
            not updated_object.get("metadata", {})
            .get("annotations", {})
            .get("oper8.org/lease-name")
        )
        assert lm.release()


@pytest.mark.timeout(5)
def test_annotation_already_owner():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    old_time = datetime.now()
    existing_object = make_managed_object(
        annotations={
            "oper8.org/lease-name": resource.name,
            "oper8.org/lease-time": old_time.isoformat(),
        }
    )
    dm.deploy([existing_object.definition])
    with library_config(
        pod_name=resource.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "duration": "10s",
                "namespace": resource.namespace,
            }
        },
    ):
        lm = AnnotationLeadershipManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(existing_object)
        _, updated_object = dm.get_object_current_state(
            existing_object.kind,
            existing_object.name,
            existing_object.namespace,
            existing_object.api_version,
        )
        assert (
            updated_object.get("metadata", {})
            .get("annotations", {})
            .get("oper8.org/lease-time")
            != old_time.isoformat()
        )
        lm.release_resource(existing_object)


@pytest.mark.timeout(5)
def test_annotation_transition():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    old_owner = make_managed_object(kind="Pod", api_version="v1", name="old")
    dm.deploy([old_owner.definition])
    old_time = datetime.now() - timedelta(seconds=30)
    existing_object = make_managed_object(
        annotations={
            "oper8.org/lease-name": old_owner.name,
            "oper8.org/lease-time": old_time.isoformat(),
        }
    )
    dm.deploy([existing_object.definition])
    with library_config(
        pod_name=resource.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "duration": "1s",
                "namespace": resource.namespace,
            }
        },
    ):
        lm = AnnotationLeadershipManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(existing_object)
        _, updated_lease = dm.get_object_current_state(
            existing_object.kind,
            existing_object.name,
            existing_object.namespace,
            existing_object.api_version,
        )
        assert (
            updated_lease.get("metadata", {})
            .get("annotations", {})
            .get("oper8.org/lease-name")
            == resource.name
        )
        lm.release_resource(existing_object)


@pytest.mark.timeout(5)
def test_annotation_not_owner():
    dm = DryRunDeployManager()
    original_owner = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([original_owner.definition])
    old_time = datetime.now()
    existing_object = make_managed_object(
        annotations={
            "oper8.org/lease-name": original_owner.name,
            "oper8.org/lease-time": old_time.isoformat(),
        }
    )
    dm.deploy([existing_object.definition])
    current_pod = make_managed_object(kind="Pod", api_version="v1", name="current")
    dm.deploy([current_pod.definition])
    with library_config(
        pod_name=current_pod.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "namespace": current_pod.namespace,
                "duration": "60s",
            }
        },
    ):
        lm = AnnotationLeadershipManager(deploy_manager=dm)
        assert not lm.acquire_resource(existing_object)
        _, updated_object = dm.get_object_current_state(
            existing_object.kind,
            existing_object.name,
            existing_object.namespace,
            existing_object.api_version,
        )
        assert (
            updated_object.get("metadata", {})
            .get("annotations", {})
            .get("oper8.org/lease-name")
            == original_owner.name
        )
        lm.release_resource(existing_object)


@pytest.mark.timeout(5)
def test_annotation_competition():
    dm = DryRunDeployManager(strict_resource_version=True)
    first_operator = make_managed_object(kind="Pod", api_version="v1", name="first")
    dm.deploy([first_operator.definition])
    with library_config(
        pod_name=first_operator.name,
        python_watch_manager={
            "lock": {
                "name": "test",
                "poll_time": "1s",
                "namespace": first_operator.namespace,
                "duration": "1s",
            }
        },
    ):
        first_manager = AnnotationLeadershipManager(deploy_manager=dm)

    second_operator = make_managed_object(kind="Pod", api_version="v1", name="second")
    dm.deploy([second_operator.definition])
    with library_config(
        pod_name=second_operator.name,
        python_watch_manager={
            "lock": {
                "name": "test",
                "poll_time": "1s",
                "namespace": second_operator.namespace,
                "duration": "1s",
            }
        },
    ):
        second_manager = AnnotationLeadershipManager(deploy_manager=dm)

    target_object = make_managed_object()
    dm.deploy([target_object.definition])

    threading.Thread(
        target=first_manager.acquire_resource, args=[target_object]
    ).start()
    threading.Thread(
        target=second_manager.acquire_resource, args=[target_object]
    ).start()

    time.sleep(1)
    # Assert only one of them gained leadership
    _, updated_object = dm.get_object_current_state(
        target_object.kind,
        target_object.name,
        target_object.namespace,
        target_object.api_version,
    )
    assert updated_object.get("metadata", {}).get("annotations", {}).get(
        "oper8.org/lease-name"
    ) in [first_operator.name, second_operator.name]
