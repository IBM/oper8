"""
Tests for the LeaderWithLeaseManager
"""
# Standard
from datetime import datetime
import time

# Third Party
import pytest

# Local
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.exceptions import ConfigError
from oper8.test_helpers.helpers import configure_logging, library_config
from oper8.test_helpers.pwm_helpers import (
    MockedLeaderWithLeaseManager,
    make_managed_object,
)

## Helpers #####################################################################


@pytest.mark.timeout(5)
def test_lease_happy_path():
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
        lm = MockedLeaderWithLeaseManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(None)
        lm.release()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_lease_already_owner():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    existing_lease = make_managed_object(
        kind="Lease",
        api_version="coordination.k8s.io/v1",
        name="test_lock",
        namespace=resource.namespace,
        spec={
            "holderIdentity": resource.name,
            "renewTime": datetime.utcnow(),
            "leaseDurationSeconds": 3600,
            "leaseTransitions": 1,
        },
    )
    dm.deploy([existing_lease.definition])
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
        lm = MockedLeaderWithLeaseManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(None)
        _, updated_lease = dm.get_object_current_state(
            existing_lease.kind,
            existing_lease.name,
            existing_lease.namespace,
            existing_lease.api_version,
        )
        assert updated_lease.get("spec", {}).get("leaseTransitions") == 1
        lm.release()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_lease_unknown_pod():
    dm = DryRunDeployManager()
    with library_config(
        python_watch_manager={"lock": {"name": "test_lock", "poll_time": "1s"}}
    ):
        with pytest.raises(ConfigError):
            lm = MockedLeaderWithLeaseManager(deploy_manager=dm)
            lm.acquire()


@pytest.mark.timeout(5)
def test_lease_transition():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    old_owner = make_managed_object(kind="Pod", api_version="v1", name="old")
    dm.deploy([old_owner.definition])
    existing_lease = make_managed_object(
        kind="Lease",
        api_version="coordination.k8s.io/v1",
        name="test_lock",
        namespace=old_owner.namespace,
        spec={
            "holderIdentity": old_owner.name,
            "renewTime": "2020-01-01T01:00:00.000000Z",
            "leaseDurationSeconds": 1,
            "leaseTransitions": 1,
        },
    )
    dm.deploy([existing_lease.definition])
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
        lm = MockedLeaderWithLeaseManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(None)
        _, updated_lease = dm.get_object_current_state(
            existing_lease.kind,
            existing_lease.name,
            existing_lease.namespace,
            existing_lease.api_version,
        )
        assert updated_lease.get("spec", {}).get("leaseTransitions") == 2
        lm.release()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_lease_not_owner():
    dm = DryRunDeployManager()
    original_owner = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([original_owner.definition])
    existing_lease = make_managed_object(
        kind="Lease",
        api_version="coordination.k8s.io/v1",
        name="test_lock",
        namespace=original_owner.namespace,
        spec={
            "holderIdentity": original_owner.name,
            "renewTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "leaseDurationSeconds": 3600,
            "leaseTransitions": 1,
        },
    )
    dm.deploy([existing_lease.definition])
    current_pod = make_managed_object(kind="Pod", api_version="v1", name="current")
    dm.deploy([current_pod.definition])
    with library_config(
        pod_name=current_pod.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "namespace": current_pod.namespace,
                "duration": "1s",
            }
        },
    ):
        lm = MockedLeaderWithLeaseManager(deploy_manager=dm)
        lm.run_renew_or_acquire()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_lease_competition():
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
                "duration": "60s",
            }
        },
    ):
        first_manager = MockedLeaderWithLeaseManager(deploy_manager=dm)

    second_operator = make_managed_object(kind="Pod", api_version="v1", name="second")
    dm.deploy([second_operator.definition])
    with library_config(
        pod_name=second_operator.name,
        python_watch_manager={
            "lock": {
                "name": "test",
                "poll_time": "1s",
                "namespace": second_operator.namespace,
                "duration": "60s",
            }
        },
    ):
        second_manager = MockedLeaderWithLeaseManager(deploy_manager=dm)

    first_manager.leadership_thread.start()
    # Add slight delay as DryRunDeployManager doesn't handle resource to old/out of date exceptions
    time.sleep(0.1)
    second_manager.leadership_thread.start()

    time.sleep(1)
    # Assert only one of them gained leadership
    assert first_manager.is_leader() ^ second_manager.is_leader()
    first_manager.release()
    second_manager.release()
