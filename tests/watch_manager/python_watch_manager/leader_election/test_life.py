"""
Tests for the LeaderForLifeManager
"""
# Standard
import time

# Third Party
import pytest

# Local
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.exceptions import ConfigError
from oper8.test_helpers.helpers import library_config
from oper8.test_helpers.pwm_helpers import (
    MockedLeaderForLifeManager,
    make_managed_object,
    make_ownerref,
)

## Helpers #####################################################################


@pytest.mark.timeout(5)
def test_life_happy_path():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    with library_config(
        pod_name=resource.name,
        python_watch_manager={
            "lock": {
                "name": "test",
                "poll_time": "1s",
                "namespace": resource.namespace,
            }
        },
    ):
        lm = MockedLeaderForLifeManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(None)
        lm.release()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_life_already_owner():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([resource.definition])
    existing_cm = make_managed_object(
        kind="ConfigMap",
        api_version="v1",
        name="test_lock",
        namespace=resource.namespace,
        owner_refs=[make_ownerref(resource.definition)],
    )
    dm.deploy([existing_cm.definition])
    with library_config(
        pod_name=resource.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "namespace": resource.namespace,
            }
        },
    ):
        lm = MockedLeaderForLifeManager(deploy_manager=dm)
        assert lm.acquire()
        assert lm.is_leader()
        assert lm.acquire_resource(None)
        lm.release()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_life_unknown_pod():
    dm = DryRunDeployManager()
    resource = make_managed_object(kind="Pod", api_version="v1")
    with library_config(
        pod_name=resource.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "namespace": resource.namespace,
            }
        },
    ):
        with pytest.raises(ConfigError):
            lm = MockedLeaderForLifeManager(deploy_manager=dm)
            lm.acquire()


@pytest.mark.timeout(5)
def test_life_bad_config_map():
    dm = DryRunDeployManager()
    original_owner = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([original_owner.definition])
    current_pod = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([current_pod.definition])
    existing_cm = make_managed_object(
        kind="ConfigMap",
        api_version="v1",
        name="test_lock",
        namespace=original_owner.namespace,
        owner_refs=[
            make_ownerref(original_owner.definition),
            make_ownerref(current_pod.definition),
        ],
    )
    dm.deploy([existing_cm.definition])

    with library_config(
        pod_name=current_pod.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "namespace": current_pod.namespace,
            }
        },
    ):
        lm = MockedLeaderForLifeManager(deploy_manager=dm)
        lm.run_renew_or_acquire()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_life_invalid_config():
    dm = DryRunDeployManager()
    with library_config(
        python_watch_manager={"lock": {"name": "test_lock", "poll_time": "1s"}}
    ):
        with pytest.raises(ConfigError):
            lm = MockedLeaderForLifeManager(deploy_manager=dm)
            lm.acquire()


@pytest.mark.timeout(5)
def test_life_not_owner():
    dm = DryRunDeployManager()
    original_owner = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([original_owner.definition])
    existing_cm = make_managed_object(
        kind="ConfigMap",
        api_version="v1",
        name="test_lock",
        namespace=original_owner.namespace,
        owner_refs=[make_ownerref(original_owner.definition)],
    )
    dm.deploy([existing_cm.definition])
    current_pod = make_managed_object(kind="Pod", api_version="v1")
    dm.deploy([current_pod.definition])
    with library_config(
        pod_name=current_pod.name,
        python_watch_manager={
            "lock": {
                "name": "test_lock",
                "poll_time": "1s",
                "namespace": current_pod.namespace,
            }
        },
    ):
        lm = MockedLeaderForLifeManager(deploy_manager=dm)
        lm.run_renew_or_acquire()
        assert not lm.is_leader()


@pytest.mark.timeout(5)
def test_life_competition():
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
            }
        },
    ):
        first_manager = MockedLeaderForLifeManager(deploy_manager=dm)

    second_operator = make_managed_object(kind="Pod", api_version="v1", name="second")
    dm.deploy([second_operator.definition])
    with library_config(
        pod_name=second_operator.name,
        python_watch_manager={
            "lock": {
                "name": "test",
                "poll_time": "1s",
                "namespace": second_operator.namespace,
            }
        },
    ):
        second_manager = MockedLeaderForLifeManager(deploy_manager=dm)

    first_manager.leadership_thread.start()
    # Add slight delay as DryRunDeployManager doesn't handle resource to old/out of date exceptions
    time.sleep(0.1)
    second_manager.leadership_thread.start()
    time.sleep(1)
    # Assert only one of them gained leadership
    assert first_manager.is_leader() ^ second_manager.is_leader()
    first_manager.release()
    second_manager.release()
