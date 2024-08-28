"""
Tests for the OpenshiftDeployManager
"""
# Standard
from contextlib import contextmanager
from queue import Queue
from threading import Event, Thread
from unittest import mock
import time

# Third Party
from openshift.dynamic.exceptions import UnprocessibleEntityError
import pytest

# First Party
import alog

# Local
from oper8 import config
from oper8 import status as oper8_status
from oper8.deploy_manager.base import DeployMethod
from oper8.deploy_manager.kube_event import KubeEventType
from oper8.deploy_manager.openshift_deploy_manager import OpenshiftDeployManager
from oper8.deploy_manager.owner_references import _make_owner_reference
from oper8.test_helpers.helpers import (
    SOME_OTHER_NAMESPACE,
    TEST_NAMESPACE,
    MockedOpenshiftDeployManager,
    configure_logging,
    library_config,
)
from oper8.test_helpers.kub_mock import MockKubClient, mock_kub_client_constructor
from oper8.utils import merge_configs

## Helpers #####################################################################

configure_logging()
log = alog.use_channel("TEST")


def setup_testable_manager(manage_ansible_status=False, owner_cr=None, *args, **kwargs):
    """Set up a testable deploy manager with a mock api client"""
    dm = MockedOpenshiftDeployManager(
        manage_ansible_status=manage_ansible_status,
        owner_cr=owner_cr,
        *args,
        **kwargs,
    )
    dm.client.resources.invalidate_cache()
    return dm


def make_obj_states(cluster_state, name_override=None):
    """Make a list of object state dicts for each object in the cluster state"""
    states = []
    for namespace, ns_entries in cluster_state.items():
        for kind, kind_entries in ns_entries.items():
            for api_version, version_entries in kind_entries.items():
                for name, state in version_entries.items():
                    if callable(state):
                        state = state("GET", namespace, kind, api_version, name)
                    if isinstance(state, tuple):
                        state = state[0]
                    states.append(
                        merge_configs(
                            state,
                            {
                                "apiVersion": api_version,
                                "kind": kind,
                                "metadata": {
                                    "name": name_override or name,
                                    "namespace": namespace,
                                },
                            },
                        )
                    )
    return states


def get_ansbile_conditions(status):
    return [
        cond
        for cond in status.get("conditions", [])
        if cond.get("type") == OpenshiftDeployManager._ANSIBLE_COND_TYPE
    ]


@contextmanager
def gather_watch_events(dm, *args, expected_events=None, **kwargs):
    """Helper function to pipe watch events from a seperate thread into a queue"""

    # Wait until the watch has started before returning. This prevents
    # duplicate events
    startup = Event()
    add_watch_queue = dm.client.client._add_watch_queue

    def startup_watch(*args, **kwargs):
        log.debug2("Setting startup event")
        startup.set()
        return add_watch_queue(*args, **kwargs)

    dm.client.client._add_watch_queue = mock.Mock(side_effect=startup_watch)

    # Setup watch queue for consuming events
    watch_queue = Queue()

    def watch_events():
        log.debug3("Watch Thread started")
        num_events = 0
        for event in dm.watch_objects(*args, **kwargs):
            log.debug2("Received event %s", event)
            watch_queue.put(event)

            # if we've gotten the expected number of events
            # exit
            num_events += 1
            if num_events >= expected_events:
                log.debug2("Exiting watch thread")
                return

    #  Start a thread for watching events in the background
    watch_thread = Thread(target=watch_events, daemon=True)
    watch_thread.start()

    # Wait for watch before starting
    log.debug2("Waiting for watch to start")
    while not startup.is_set():
        pass

    log.debug2("Watch started")
    yield watch_queue

    # If the number of expected events was given assert that the thread exited
    if expected_events:
        watch_thread.join(1)
        assert not watch_thread.is_alive(), "Extra events in watch queue"


## Tests #######################################################################


############
## deploy ##
############


def test_deploy_new_resource():
    """Make sure that deploying a new instance of an existing resource type to
    the cluster works
    """
    start_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager(cluster_state=start_cluster_state)
    success, changed = dm.deploy(make_obj_states(end_cluster_state))
    assert success
    assert changed


def test_deploy_update_resource_change():
    """Make sure that deploying a change to an existing resource instance in the
    cluster works when there is a valid change
    """
    start_cluster_state = {
        "test": {"Foo": {"foo.bar.com/v1": {"bar": {"wing": "bat"}}}}
    }
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {"wing": "bar"}}}}}
    dm = setup_testable_manager(cluster_state=start_cluster_state)

    # Deploy it and make sure it changes
    success, changed = dm.deploy(make_obj_states(end_cluster_state))
    assert success
    assert changed

    # Look it back up and make sure the changes took
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is not None
    assert content.get("wing") == "bar"


def test_deploy_update_resource_no_change():
    """Make sure that deploying a change to an existing resource instance in the
    cluster works when there is no change
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {"wing": "bat"}}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    success, changed = dm.deploy(make_obj_states(cluster_state))
    assert success
    assert not changed


def test_deploy_method_resource():
    """Make sure that deploying a new instance of an existing resource type to
    the cluster works
    """
    start_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    replace_apply_resource = {
        "kind": "Foo",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {"name": "bar", "namespace": "test"},
        "spec": {"some": "key_1"},
    }
    end_apply_resource = {
        "kind": "Foo",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {"name": "bar", "namespace": "test"},
        "spec": {"some": "key_2"},
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state)
    dm._requires_replace = lambda *args, **kwargs: True

    replace_called = []

    def track_replace(resource_definition: dict):
        replace_called.append(resource_definition)
        return MockedOpenshiftDeployManager._replace_resource(dm, resource_definition)

    dm._replace_resource = track_replace

    apply_called = []

    def track_apply(resource_definition):
        apply_called.append(resource_definition)
        return MockedOpenshiftDeployManager._apply_resource(dm, resource_definition)

    dm._apply_resource = track_apply

    # Use apply instead of replace when first deploying
    success, changed = dm.deploy(
        resource_definitions=make_obj_states(end_cluster_state),
        method=DeployMethod.REPLACE,
    )
    assert success
    assert changed
    assert len(replace_called) == 0 and len(apply_called) == 1

    success, changed = dm.deploy(
        resource_definitions=[replace_apply_resource],
        method=DeployMethod.REPLACE,
    )
    assert success
    assert changed
    assert len(replace_called) == 1 and len(apply_called) == 1

    success, changed = dm.deploy(
        resource_definitions=[end_apply_resource], method=DeployMethod.UPDATE
    )
    assert success
    assert changed
    assert len(replace_called) == 1 and len(apply_called) == 2


def test_deploy_method_update_resource():
    """Make sure that deploying a new instance of an existing resource type to
    the cluster works
    """
    start_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager(cluster_state=start_cluster_state)
    success, changed = dm.deploy(
        make_obj_states(end_cluster_state), DeployMethod.UPDATE
    )
    assert success
    assert changed


def test_deploy_unknown_resource_type():
    """Make sure that trying to deploy an instance of an unknown resource type
    results in a handled error
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {"wing": "bat"}}}}}
    dm = setup_testable_manager(cluster_state={})
    success, changed = dm.deploy(make_obj_states(cluster_state))
    assert not success
    assert not changed


def test_deploy_conflict_persist():
    """Make sure that trying to deploy and encountering a persistent 409
    conflict error results in a failed deployment
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": ({}, 409)}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    success, changed = dm.deploy(make_obj_states(cluster_state))
    assert not success
    assert not changed


def test_deploy_conflict_retry_resolve_count():
    """Make sure that trying to deploy and encountering a 409 conflict error the
    first time which resolves on retry results in a successful deployment
    """

    class ConflictOncePatch:
        def __init__(self):
            self._count = 0

        def __call__(self, method, namespace, kind, api_version, name):
            log.debug(
                "Handling call with method [%s] for [%s/%s/%s/%s]",
                method,
                namespace,
                api_version,
                kind,
                name,
            )
            res = {
                "metadata": {
                    "resourceVersion": self._count,
                }
            }
            if method == "PATCH":
                if self._count < 1:
                    log.debug("Returning 409")
                    res = ({}, 409)
                self._count += 1
            return res

    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": ConflictOncePatch()}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {"some": "content"}}}}}
    success, changed = dm.deploy(make_obj_states(cluster_state))
    assert success
    assert changed


@pytest.mark.parametrize("enable_fallback", [True, False])
def test_deploy_conflict_unprocessible_retry_resolve_count(enable_fallback):
    """Make sure that trying to deploy and encountering a 409 conflict error the
    first time, then encountering a 422 falls back to using PUT and resolves
    """

    class ConflictUnprocessiblePatch:
        def __init__(self):
            self._count = 0

        def __call__(self, method, namespace, kind, api_version, name):
            log.debug(
                "Handling call with method [%s] for [%s/%s/%s/%s]",
                method,
                namespace,
                api_version,
                kind,
                name,
            )
            res = {
                "metadata": {
                    "resourceVersion": self._count,
                }
            }
            if method == "PATCH":
                if self._count < 1:
                    log.debug("Returning 409")
                    res = ({}, 409)
                else:
                    log.debug("Returning 422")
                    res = ({}, 422)
                self._count += 1
            return res

    with library_config(deploy_unprocessable_put_fallback=enable_fallback):
        cluster_state = {
            "test": {"Foo": {"foo.bar.com/v1": {"bar": ConflictUnprocessiblePatch()}}}
        }
        dm = setup_testable_manager(cluster_state=cluster_state)
        cluster_state = {
            "test": {"Foo": {"foo.bar.com/v1": {"bar": {"some": "content"}}}}
        }
        assert dm.deploy(make_obj_states(cluster_state))[0] is enable_fallback


def test_deploy_conflict_retry_resolve_time():
    """Make sure that trying to deploy and encountering a 409 conflict error for
    a period of time eventually results in a successful deploy
    """

    class ConflictTimePatch:
        def __init__(self):
            self._first = None

        def __call__(self, method, namespace, kind, api_version, name):
            log.debug(
                "Handling call with method [%s] for [%s/%s/%s/%s]",
                method,
                namespace,
                api_version,
                kind,
                name,
            )
            res = {
                "metadata": {
                    "resourceVersion": 1234,
                }
            }
            if method == "PATCH":
                if self._first is None:
                    self._first = time.time()
                if time.time() - self._first < config.retry_backoff_base_seconds * 2.5:
                    log.debug("Returning 409")
                    res = ({}, 409)
            return res

    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": ConflictTimePatch()}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {"some": "content"}}}}}
    success, changed = dm.deploy(make_obj_states(cluster_state))
    assert success
    assert changed


def test_deploy_forbidden():
    """Make sure that a forbidden error when deploying results in a handled
    error
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": ({}, 403)}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    success, changed = dm.deploy(make_obj_states(cluster_state))
    assert not success
    assert not changed


def test_deploy_no_cached():
    """Make sure that when an object is deployed, a subsequent call to
    get_object_current_state will pull from the cluster to get the updated state
    """
    start_cluster_state = {
        "test": {"Foo": {"foo.bar.com/v1": {"bar": {"wing": "bat"}}}}
    }
    end_cluster_state = {
        "test": {"Foo": {"foo.bar.com/v1": {"bar": {"wom": "bar", "wing": "bar"}}}}
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state)

    # Fetch it to pre-populate the cache
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is not None
    assert "wom" not in content
    assert content.get("wing") == "bat"

    # Deploy it and make sure it changes
    success, changed = dm.deploy(make_obj_states(end_cluster_state))
    assert success
    assert changed

    # Look it back up and make sure the changes took
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is not None
    assert content.get("wom") == "bar"
    assert content.get("wing") == "bar"


def test_deploy_multiple_resources_ok():
    """Make sure that deploying multiple valid resources at a time succeeds"""
    start_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}, "baz": {}}}}}
    dm = setup_testable_manager(cluster_state=start_cluster_state)
    success, changed = dm.deploy(make_obj_states(end_cluster_state))
    assert success and changed
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success and content is not None
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="baz"
    )
    assert success and content is not None


def test_deploy_multiple_resources_second_bad_content():
    """Make sure that deploying multiple resources where the first is valid and
    the second is malformed successfully applies the first one, but fails on the
    second one
    """

    def patch_unprocessable(method, namespace, kind, api_version, name):
        if method == "PATCH":
            return ({}, 422)
        else:
            return {}

    start_cluster_state = {
        "test": {"Foo": {"foo.bar.com/v1": {"baz": patch_unprocessable}}}
    }
    deploy_cluster_state = {
        "test": {
            "Foo": {
                "foo.bar.com/v1": {
                    "bar": {},
                    "baz": {"something": "different"},
                }
            }
        }
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state)
    success, changed = dm.deploy(make_obj_states(deploy_cluster_state))
    assert not success
    assert changed

    # Make sure "bar" actually got deployed
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success and content is not None


def test_deploy_multiple_resources_first_bad_content():
    """Make sure that deploying multiple resources where the first is invalid
    and the second would be valid does not deploy the second after the first
    fails
    """

    def patch_unprocessable(method, namespace, kind, api_version, name):
        if method == "PATCH":
            return ({}, 422)
        else:
            return {}

    start_cluster_state = {
        "test": {"Foo": {"foo.bar.com/v1": {"bar": patch_unprocessable}}}
    }
    deploy_cluster_state = {
        "test": {
            "Foo": {
                "foo.bar.com/v1": {
                    "bar": {"something": "different"},
                    "baz": {},
                }
            }
        }
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state)
    success, changed = dm.deploy(make_obj_states(deploy_cluster_state))
    assert not success
    assert not changed

    # Make sure "baz" did not get deployed
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="baz"
    )
    assert success and content is None


def test_deploy_empty_list():
    """Make sure that an empty list of resources is a no-op"""
    dm = setup_testable_manager()
    success, changed = dm.deploy([])
    assert success
    assert not changed


def test_deploy_add_owner_reference():
    """Make sure that deploying a resource with an owner CR adds the owner
    reference given the resource and owner CR are in the same namespace
    """
    start_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    owner_cr = {
        "kind": "Owner",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {"name": "owner", "namespace": "test", "uid": "unique"},
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state, owner_cr=owner_cr)
    success, changed = dm.deploy(make_obj_states(end_cluster_state))
    assert success
    assert changed
    success, content = dm.get_object_current_state(
        kind="Foo",
        api_version="foo.bar.com/v1",
        namespace="test",
        name="bar",
    )
    assert success
    assert content["metadata"]["ownerReferences"] == [_make_owner_reference(owner_cr)]


def test_deploy_dont_add_owner_reference_if_manage_owner_references_disabled():
    """Make sure that deploying a resource with an owner CR does not add the
    owner reference when manage_owner_references is False
    """
    start_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    owner_cr = {
        "kind": "Owner",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {"name": "owner", "namespace": "test", "uid": "unique"},
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state, owner_cr=owner_cr)
    success, changed = dm.deploy(
        make_obj_states(end_cluster_state), manage_owner_references=False
    )
    assert success
    assert changed
    success, content = dm.get_object_current_state(
        kind="Foo",
        api_version="foo.bar.com/v1",
        namespace="test",
        name="bar",
    )
    assert success
    assert "ownerReferences" not in content["metadata"]


def test_deploy_cluster_wide_resource():
    """Make sure that deploying a new instance of an existing resource type to
    the cluster works
    """
    start_cluster_state = {"": {"Foo": {"foo.bar.com/v1": {}}}}
    dm = setup_testable_manager(cluster_state=start_cluster_state)

    obj = {"apiVersion": "foo.bar.com/v1", "kind": "Foo", "metadata": {"name": "bar"}}
    success, changed = dm.deploy([obj])

    assert success
    assert changed


def test_deploy_dont_add_owner_reference_if_resource_and_owner_cr_not_in_same_namespace():
    """Make sure that deploying a resource with an owner CR does not add the owner
    reference if the resource and owner CR are in a different namespace
    """
    start_cluster_state = {SOME_OTHER_NAMESPACE: {"Foo": {"foo.bar.com/v1": {}}}}
    end_cluster_state = {SOME_OTHER_NAMESPACE: {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    owner_cr = {
        "kind": "Owner",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {"name": "owner", "namespace": TEST_NAMESPACE, "uid": "unique"},
    }
    dm = setup_testable_manager(cluster_state=start_cluster_state, owner_cr=owner_cr)
    success, changed = dm.deploy(make_obj_states(end_cluster_state))
    assert success
    assert changed
    success, content = dm.get_object_current_state(
        kind="Foo",
        api_version="foo.bar.com/v1",
        namespace=SOME_OTHER_NAMESPACE,
        name="bar",
    )
    assert success
    assert content["metadata"]["ownerReferences"] == []


#############
## disable ##
#############


def test_disable_remove_present_resource():
    """Make sure that disabling a resource which is in the cluster correctly
    deletes it
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    success, changed = dm.disable(make_obj_states(cluster_state))
    assert success
    assert changed


def test_disable_remove_object_not_present():
    """Make sure that disabling a resource which is in the cluster correctly
    indicates that no change was made
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    success, changed = dm.disable(make_obj_states(cluster_state, name_override="baz"))
    assert success
    assert not changed


def test_disable_remove_kind_not_present():
    """Make sure that disabling a kind which is unknown to the cluster indicates
    success (the resource doesn't exist at the end)
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager()
    success, changed = dm.disable(make_obj_states(cluster_state))
    assert success
    assert not changed


def test_disable_remove_object_non_namespaced():
    """Make sure that disabling a resource which is in the cluster correctly
    indicates that no change was made
    """
    cluster_state = {None: {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)

    obj = make_obj_states(cluster_state)[0]
    del obj["metadata"]["namespace"]
    success, changed = dm.disable([obj])
    assert success
    assert not changed


def test_disable_remove_forbidden():
    """Make sure that a forbidden error is handled when attempting to remove a
    resource from the cluster
    """

    def delete_forbidden(method, namespace, kind, api_version, name):
        if method == "DELETE":
            return ({}, 403)
        else:
            return {}

    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": delete_forbidden}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)
    success, changed = dm.disable(make_obj_states(cluster_state))
    assert not success
    assert not changed


def test_disable_invalid_resource_list():
    """Make sure that passing an invalid type shows non-success"""
    dm = setup_testable_manager()
    with pytest.raises(AssertionError):
        dm.disable("not a list")


def test_disable_clear_cache():
    """Make sure that for an object that has been cached, a disable will cause
    it to not hit cache on a subsequent get_object_current_state
    """
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    dm = setup_testable_manager(cluster_state=cluster_state)

    # First get the object to populate the cache
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is not None

    # Disable it
    success, changed = dm.disable(make_obj_states(cluster_state))
    assert success
    assert changed

    # Look it up again and make sure it's not found
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is None


##############################
## get_object_current_state ##
##############################


def test_get_object_current_state_object_present():
    """Make sure that get_object_current_state returns success and a valid
    object when the object is present in the cluster
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is not None


def test_get_object_current_state_resource_not_present():
    """Make sure that get_object_current_state returns success and None when the
    resource type is not present in the cluster
    """
    dm = setup_testable_manager(cluster_state={})
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar"
    )
    assert success
    assert content is None


def test_get_object_current_state_object_not_present():
    """Make sure that get_object_current_state returns success and None when the
    object is not present in the cluster
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="baz"
    )
    assert success
    assert content is None


def test_get_object_current_state_object_not_unique():
    """Make sure that when a given resource type is given which has multiple
    api_versions available and the api_version is not given as an arg, the
    ResourceNotUniqueError is handled as if the object is not found.
    """
    dm = setup_testable_manager(
        cluster_state={
            "test": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )
    success, content = dm.get_object_current_state(
        kind="Foo",
        namespace="test",
        name="bar",
    )
    assert success
    assert content is None


def test_get_object_current_state_object_not_unique_with_version():
    """Make sure that when a given resource type is given which has multiple
    api_versions available and the api_version is given as an arg, the
    object is found and returned.
    """
    dm = setup_testable_manager(
        cluster_state={
            "test": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar", api_version="v1"
    )
    assert success
    assert content is not None


def test_multiple_version_error():
    """Make sure that deploying a new instance of an existing resource type to
    the cluster works
    """
    dm = setup_testable_manager(
        cluster_state={
            "default": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )

    # Assert no object is returned
    success, obj = dm.get_object_current_state(
        kind="Foo",
        namespace="",
        api_version="",
        name="",
    )

    assert success
    assert not obj

    success, obj = dm.get_object_current_state(
        kind="Foo",
        namespace="default",
        api_version="foo.bar.com/v1",
        name="bar",
    )

    assert success
    assert obj


def test_get_object_current_state_forbidden():
    """Make sure that if a 403 (forbidden) is given, it's reported as
    non-successful
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": ({}, 403)}}}}
    )
    success, content = dm.get_object_current_state(
        kind="Foo", namespace="test", name="bar", api_version="v1"
    )
    assert not success
    assert content is None


def test_get_object_current_state_object_not_namespaced():
    """Make sure that when a given resource type is given which has multiple
    api_versions available and the api_version is not given as an arg, the
    ResourceNotUniqueError is handled as if the object is not found.
    """
    dm = setup_testable_manager(
        cluster_state={
            "": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )
    success, content = dm.get_object_current_state(
        kind="Foo", name="bar", api_version="v1beta1"
    )
    assert success
    assert content


##################################
## filter_objects_current_state ##
##################################


def test_filter_objects_current_state_object_present():
    """Make sure that filter_objects_current_state returns success and a valid
    object when the object is present in the cluster
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    success, content = dm.filter_objects_current_state(kind="Foo", namespace="test")
    assert success
    assert len(content) != 0


def test_filter_objects_current_state_resource_not_present():
    """Make sure that filter_objects_current_state returns success and [] when the
    resource type is not present in the cluster
    """
    dm = setup_testable_manager(cluster_state={})
    success, content = dm.filter_objects_current_state(kind="Foo", namespace="test")
    assert success
    assert len(content) == 0


def test_filter_objects_current_state_object_label_selector():
    """Make sure that filter_objects_current_state returns success and [obj] when an
    object matches the label selector
    """
    dm = setup_testable_manager(
        cluster_state={
            "test": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {"metadata": {"labels": {"app": "bar"}}}}
                }
            }
        }
    )
    success, content = dm.filter_objects_current_state(
        kind="Foo", namespace="test", label_selector="app=bar"
    )
    assert success
    assert len(content) == 1


def test_filter_objects_current_state_object_field_selector():
    """Make sure that filter_objects_current_state returns success and [obj] when an
    object matches the field selector
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {"spec": {"a": 1}}}}}}
    )
    success, content = dm.filter_objects_current_state(
        kind="Foo", namespace="test", field_selector="spec.a==1"
    )
    assert success
    assert len(content) == 1


def test_filter_objects_current_state_object_not_unique():
    """Make sure that when a given resource type is given which has multiple
    api_versions available and the api_version is not given as an arg, the
    ResourceNotUniqueError is handled as if the object is not found.
    """
    dm = setup_testable_manager(
        cluster_state={
            "test": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )
    success, content = dm.filter_objects_current_state(kind="Foo", namespace="test")
    assert success
    assert len(content) == 0


def test_filter_objects_current_state_object_not_unique_with_version():
    """Make sure that when a given resource type is given which has multiple
    api_versions available and the api_version is given as an arg, the
    object is found and returned.
    """
    dm = setup_testable_manager(
        cluster_state={
            "test": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )
    success, content = dm.filter_objects_current_state(
        kind="Foo", namespace="test", api_version="v1"
    )
    assert success
    assert len(content) != 0


def test_multiple_version_error():
    """Make sure that deploying a new instance of an existing resource type to
    the cluster works
    """
    dm = setup_testable_manager(
        cluster_state={
            "default": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )

    # Assert no object is returned
    success, obj = dm.filter_objects_current_state(
        kind="Foo",
        namespace="",
        api_version="",
    )

    assert success
    assert len(obj) == 0

    success, obj = dm.filter_objects_current_state(
        kind="Foo",
        namespace="default",
        api_version="foo.bar.com/v1",
    )

    assert success
    assert len(obj) == 1


def test_filter_objects_current_state_forbidden():
    """Make sure that if a 403 (forbidden) is given, it's reported as
    non-successful
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": ({}, 403)}}}}
    )
    success, content = dm.filter_objects_current_state(
        kind="Foo", namespace="test", api_version="v1"
    )
    assert not success
    assert len(content) == 0


def test_filter_objects_current_state_object_not_namespaced():
    """Make sure that when a given resource type is given which has multiple
    api_versions available and the api_version is not given as an arg, the
    ResourceNotUniqueError is handled as if the object is not found.
    """
    dm = setup_testable_manager(
        cluster_state={
            "": {
                "Foo": {
                    "foo.bar.com/v1": {"bar": {}},
                    "foo.bar.com/v1beta1": {"bar": {}},
                }
            }
        }
    )
    success, content = dm.filter_objects_current_state(
        kind="Foo",
        api_version="v1beta1",
    )
    assert success
    assert len(content) != 0


###################
## watch_objects ##
###################


@pytest.mark.timeout(5)
def test_watch_objects():
    dm = setup_testable_manager(cluster_state={"test": {"Foo": {"foo.bar.com/v1": {}}}})
    initial_objects = make_obj_states(
        {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )

    with gather_watch_events(
        dm,
        expected_events=3,
        kind="Foo",
        api_version="foo.bar.com/v1",
        namespace="test",
    ) as watch_queue:

        # Create, Update, and delete resource
        dm.deploy(initial_objects)
        dm.deploy(
            make_obj_states(
                {"test": {"Foo": {"foo.bar.com/v1": {"bar": {"spec": "updated"}}}}}
            )
        )
        dm.disable(initial_objects)

        # Assert each event is captured
        assert watch_queue.get().type == KubeEventType.ADDED
        assert watch_queue.get().type == KubeEventType.MODIFIED
        assert watch_queue.get().type == KubeEventType.DELETED


@pytest.mark.timeout(5)
def test_watch_objects_resourced():
    dm = setup_testable_manager(cluster_state={"test": {"Foo": {"foo.bar.com/v1": {}}}})
    initial_objects = make_obj_states(
        {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    dm.deploy(initial_objects)

    with gather_watch_events(
        dm,
        expected_events=2,
        kind="Foo",
        api_version="foo.bar.com/v1",
        namespace="test",
        name="bar",
    ) as watch_queue:

        # Create and Delete objects while updating a nonwatched resource. Ensure only
        # the create and delete events are captured
        dm.deploy(
            make_obj_states(
                {
                    "test": {
                        "Foo": {"foo.bar.com/v1": {"different": {"spec": "updated"}}}
                    }
                }
            )
        )
        dm.disable(initial_objects)
        # Assert only events related to our obj get returned
        assert watch_queue.get().type == KubeEventType.ADDED
        assert watch_queue.get().type == KubeEventType.DELETED


################
## set_status ##
################


def test_set_status_happy_path():
    """Make sure that setting a simple status can round-trip safely"""
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    log.debug("Setting Status")
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    assert success
    assert changed


def test_set_status_conflict_retry_success():
    """Make sure that if a conflict occurs on the first update, a retry is
    attempted
    """

    # Small callable that will return a conflict the first time, then return
    # success
    class OnePutConflict:
        def __init__(self):
            self.first_call = True

        def __call__(self, method, namespace, kind, api_version, name):
            if method == "PUT" and self.first_call:
                log.debug("First call. Returning 409")
                self.first_call = False
                return ({}, 409)
            else:
                return {"metadata": {"resourceVersion": 123}}

    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": OnePutConflict()}}}}
    )
    log.debug("Setting Status")
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    assert success
    assert changed


def test_set_status_conflict_retry_failure():
    """Make sure that if a conflict occurs on a retry, we do not retry a third
    time
    """

    # Small callable that will return a conflict the first time, then return
    # success
    class TwoPutConflicts:
        def __init__(self):
            self.put_call_num = 0

        def __call__(self, method, namespace, kind, api_version, name):
            if method == "PUT":
                if self.put_call_num < 2:
                    log.debug("Call %d. Returning 409", self.put_call_num)
                    return ({}, 409)
                self.put_call_num += 1
            return {}

    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": TwoPutConflicts()}}}}
    )
    log.debug("Setting Status")
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    not success
    not changed


def test_set_status_update_cache():
    """Make sure that when set_status is called, a subsequent
    get_object_current_state on the same object will return the updated status
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )

    # Fetch current state and make sure status is not there
    success, content = dm.get_object_current_state(
        kind="Foo", name="bar", namespace="test"
    )
    assert success
    assert content is not None
    assert "status" not in content

    # Set the status
    log.debug("Setting Status")
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    assert success
    assert changed

    # Re-fetch and make sure the status is present
    success, content = dm.get_object_current_state(
        kind="Foo", name="bar", namespace="test"
    )
    assert success
    assert content is not None
    assert "status" in content
    assert content["status"].get("foo") == "bar"


def test_set_status_update_no_change():
    """Make sure that when set_status is called, if the new status is the same
    as the current status, the return indicates no change
    """
    status = {"foo": "bar"}
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {"status": status}}}}}
    )
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status=status,
    )
    assert success
    assert not changed


def test_set_status_resource_kind_missing():
    """Make sure that when set_status is called and the resource kind does not
    exist, the function handles it and returns non-success
    """
    dm = setup_testable_manager(cluster_state={})
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    assert not success
    assert not changed


def test_set_status_resource_object_missing():
    """Make sure that when set_status is called and the named object does not
    exist, the function handles it and returns non-success
    """
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    success, changed = dm.set_status(
        kind="Foo",
        name="baz",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    assert not success
    assert not changed


def test_set_status_put_forbidden():
    """Make sure that when set_status is called and the PUT method returns a
    403, the error is handled and returns non-success
    """

    # Small callable that will return 403 only on PUT
    def put_forbidden(method, namespace, kind, api_version, name):
        if method == "PUT":
            return ({}, 403)
        else:
            return {}

    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": put_forbidden}}}}
    )
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status={"foo": "bar"},
    )
    assert not success
    assert not changed


def test_set_status_no_api_version():
    """Make sure that setting a status without api_version works as expected"""
    dm = setup_testable_manager(
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    )
    log.debug("Setting Status")
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version=None,
        status={"foo": "bar"},
    )
    assert success
    assert changed


#########################################
## set_status w/ manage_ansible_status ##
#########################################


def test_set_status_ansible_status_not_ready():
    """Make sure that when ansible status management is enabled and the status
    is not ready, the correct ansbile status is injected.
    """
    dm = setup_testable_manager(
        manage_ansible_status=True,
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}},
    )
    status = oper8_status.make_application_status(
        ready_reason=oper8_status.ReadyReason.INITIALIZING
    )
    log.debug3("Input status: %s", status)
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status=status,
    )
    assert success
    assert changed  # Status updated
    success, content = dm.get_object_current_state(
        kind="Foo", name="bar", namespace="test"
    )
    assert success
    cluster_status = content["status"]
    log.debug3("Updated status: %s", cluster_status)
    ansible_conditions = get_ansbile_conditions(cluster_status)
    assert len(ansible_conditions) == 1
    assert (
        ansible_conditions[0].get("ansibleResult")
        == OpenshiftDeployManager._ANSIBLE_COND_RES_UNREADY
    )


def test_set_status_ansible_status_ready():
    """Make sure that when ansible status management is enabled and the status
    is ready, the correct ansbile status is injected.
    """
    dm = setup_testable_manager(
        manage_ansible_status=True,
        cluster_state={"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}},
    )
    status = oper8_status.make_application_status(
        ready_reason=oper8_status.ReadyReason.STABLE,
        updating_reason=oper8_status.UpdatingReason.STABLE,
        version="1.0.0",
    )
    log.debug3("Input status: %s", status)
    success, changed = dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status=status,
    )
    assert success
    assert changed  # Status updated
    success, content = dm.get_object_current_state(
        kind="Foo", name="bar", namespace="test"
    )
    assert success
    cluster_status = content["status"]
    log.debug3("Updated status: %s", cluster_status)
    ansible_conditions = get_ansbile_conditions(cluster_status)
    assert len(ansible_conditions) == 1
    assert (
        ansible_conditions[0].get("ansibleResult")
        == OpenshiftDeployManager._ANSIBLE_COND_RES_READY
    )


def test_set_status_ansible_status_transition():
    """Make sure that when ansible status management is enabled and the status
    is ready when the previous status was not, the transition time is updated.
    """
    version = "1.0.0"
    init_status = oper8_status.make_application_status(
        ready_reason=oper8_status.ReadyReason.INITIALIZING, version=version
    )
    ready_status = oper8_status.make_application_status(
        ready_reason=oper8_status.ReadyReason.STABLE,
        updating_reason=oper8_status.UpdatingReason.STABLE,
        version=version,
    )
    dm = setup_testable_manager(
        manage_ansible_status=True,
        cluster_state={
            "test": {"Foo": {"foo.bar.com/v1": {"bar": {"status": init_status}}}}
        },
    )

    dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status=ready_status,
    )
    cluster_status = dm.get_object_current_state(
        kind="Foo", name="bar", namespace="test"
    )[1]["status"]
    log.debug3("Updated status: %s", cluster_status)
    ansible_conditions = get_ansbile_conditions(cluster_status)
    assert len(ansible_conditions) == 1
    assert (
        ansible_conditions[0].get("ansibleResult")
        == OpenshiftDeployManager._ANSIBLE_COND_RES_READY
    )

    # Make sure timestamp taken from ready status
    assert (
        ansible_conditions[0].get("lastTransitionTime")
        == ready_status["conditions"][0][oper8_status.TIMESTAMP_KEY]
    )


def test_set_status_ansible_status_no_transition():
    """Make sure that when ansible status management is enabled and the status
    is ready when the previous status was also ready, the transition time is
    not updated.
    """
    init_status = oper8_status.make_application_status(
        ready_reason=oper8_status.ReadyReason.INITIALIZING
    )
    error_status = oper8_status.make_application_status(
        ready_reason=oper8_status.ReadyReason.CONFIG_ERROR
    )
    dm = setup_testable_manager(
        manage_ansible_status=True,
        cluster_state={
            "test": {"Foo": {"foo.bar.com/v1": {"bar": {"status": init_status}}}}
        },
    )

    dm.set_status(
        kind="Foo",
        name="bar",
        namespace="test",
        api_version="foo.bar.com/v1",
        status=error_status,
    )
    cluster_status = dm.get_object_current_state(
        kind="Foo", name="bar", namespace="test"
    )[1]["status"]
    log.debug3("Updated status: %s", cluster_status)
    ansible_conditions = get_ansbile_conditions(cluster_status)
    assert len(ansible_conditions) == 1
    assert (
        ansible_conditions[0].get("ansibleResult")
        == OpenshiftDeployManager._ANSIBLE_COND_RES_UNREADY
    )

    # Make sure timestamp taken from first init status
    assert (
        ansible_conditions[0].get("lastTransitionTime")
        == init_status["conditions"][0][oper8_status.TIMESTAMP_KEY]
    )


###########
## Other ##
###########


def test_default_construct_client():
    """Make sure that if no client is given at construction, the client is
    automatically created when needed.
    """
    dm = OpenshiftDeployManager()
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    with mock_kub_client_constructor():
        dm.deploy(make_obj_states(cluster_state))


def test_in_cluster_config():
    """Make sure that if the in-cluster config loads, the client is created
    when needed."""
    dm = OpenshiftDeployManager()
    cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": {}}}}}
    with mock.patch("kubernetes.config.load_incluster_config", return_value=None):
        mock_client = MockKubClient()
        with mock.patch("kubernetes.client.ApiClient", return_value=mock_client):
            dm.deploy(make_obj_states(cluster_state))


def test_422_fallback_to_put_ok():
    """Make sure that if enabled, deploy can fall back to using PUT instead of
    PATCH on a 422 error and that the PUT can succeed if valid.
    """
    with library_config(deploy_unprocessable_put_fallback=True):

        def fail_patch_only(method, namespace, kind, api_version, name):
            if method == "PATCH":
                return ({}, 422)
            return {
                "apiVersion": api_version,
                "kind": kind,
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                },
            }

        cluster_state = {"test": {"Foo": {"foo.bar.com/v1": {"bar": fail_patch_only}}}}
        dm = setup_testable_manager(cluster_state=cluster_state)
        success, _ = dm.deploy(
            [
                {
                    "apiVersion": "foo.bar.com/v1",
                    "kind": "Foo",
                    "metadata": {"name": "bar", "namespace": "test"},
                    "key": "val",
                }
            ]
        )
        assert success


def test_422_fallback_to_put_invalid():
    """Make sure that if enabled, deploy can fall back to using PUT instead of
    PATCH on a 422 error and that the PUT can still produce a 422 if it is truly
    invalid.
    """
    with library_config(deploy_unprocessable_put_fallback=True):

        def fail_put_patch_only(method, namespace, kind, api_version, name):
            if method in ["PATCH", "PUT"]:
                return ({}, 422)
            return {
                "apiVersion": api_version,
                "kind": kind,
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                },
            }

        cluster_state = {
            "test": {"Foo": {"foo.bar.com/v1": {"bar": fail_put_patch_only}}}
        }
        dm = setup_testable_manager(cluster_state=cluster_state)
        success, _ = dm.deploy(
            [
                {
                    "apiVersion": "foo.bar.com/v1",
                    "kind": "Foo",
                    "metadata": {"name": "bar", "namespace": "test"},
                    "key": "val",
                }
            ]
        )
        assert not success
