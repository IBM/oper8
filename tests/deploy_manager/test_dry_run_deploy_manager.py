"""Tests for the DryRunDeployManager

NOTE: The majority of the functionality is thoroughly exercised by all of the
    other unit tests, so the tests here only test elements that are particularly
    delicate and/or not covered elsewhere.
"""
# Standard
from threading import Timer
from unittest.mock import Mock

# Third Party
import pytest

# First Party
import aconfig

# Local
from oper8.deploy_manager import DryRunDeployManager, KubeEventType, KubeWatchEvent
from oper8.deploy_manager.owner_references import _make_owner_reference
from oper8.test_helpers.helpers import SOME_OTHER_NAMESPACE, TEST_NAMESPACE, setup_cr

## Helpers #####################################################################


def make_obj(
    api_version, kind, name="foobar", namespace=SOME_OTHER_NAMESPACE, spec={"a": 1}
):
    return aconfig.Config(
        {
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "app": "foobar",
                    "run": "frontend",
                },
            },
            "spec": spec,
        },
        override_env_vars=False,
    )


## Tests #######################################################################


def test_watches_triggered():
    """Test that registered watches are triggered when a resource of the right
    GVK is deployed
    """
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    mock1 = Mock()
    mock2 = Mock()
    dm.register_watch(api_version, kind, mock1)
    dm.register_watch(api_version, kind, mock2)
    dm.deploy([obj])

    mock1.assert_called_once_with(obj)
    mock2.assert_called_once_with(obj)


def test_watches_not_triggered_other_resource():
    """Test that a registered watch is not triggered for an object with a
    different GVK
    """
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    mock = Mock()
    dm.register_watch("foo.bar/v2", kind, mock)
    dm.deploy([obj])
    assert not mock.called


def test_finalizer_triggered():
    """Test that registered finalizers are triggered when a resource of the
    right GVK is deleted
    """
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    mock1 = Mock()
    mock2 = Mock()
    dm.register_finalizer(api_version, kind, mock1)
    dm.register_finalizer(api_version, kind, mock2)
    dm.deploy([obj])
    assert not mock1.called
    assert not mock2.called
    dm.disable([obj])
    assert mock1.call_count == 1
    assert mock2.call_count == 1


def test_finalizer_not_triggered_not_deleted():
    """Test that a registered finalizer is not triggered when a resource of the
    right GVK is disabled, but was not present in the cluster
    """
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    mock = Mock()
    dm.register_finalizer(api_version, kind, mock)
    dm.disable([obj])
    assert not mock.called


def test_finalizer_not_triggered_other_resource():
    """Test that a registered finalizer is not triggered when a resource of the
    right GVK is disabled, but was not present in the cluster
    """
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    mock = Mock()
    dm.register_finalizer("foo.bar/v2", kind, mock)
    dm.deploy([obj])
    assert not mock.called
    dm.disable([obj])
    assert not mock.called


def test_watch_key_none():
    """Make sure the _watch_key function can handle None arguments. This is used
    at times in unit tests and should be supported
    """
    dm = DryRunDeployManager()
    assert isinstance(dm._watch_key(None, None), str)


@pytest.mark.timeout(5)
def test_watch_objects():
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    dm.deploy([obj])

    deploy_timer_event = Timer(
        interval=0.5,
        function=dm.deploy,
        args=[[make_obj(api_version, kind, name="barfoo")]],
    )
    update_timer_event = Timer(
        interval=1,
        function=dm.deploy,
        args=[[make_obj(api_version, kind, name="barfoo", spec={"modified": 1})]],
    )
    disable_timer_event = Timer(
        interval=1.5, function=dm.disable, args=[[make_obj(api_version, kind)]]
    )
    deploy_timer_event.start()
    update_timer_event.start()
    disable_timer_event.start()

    captured_events = []
    for event in dm.watch_objects(
        kind, api_version, namespace=SOME_OTHER_NAMESPACE, timeout=3
    ):
        captured_events.append(event)

    assert len(captured_events) == 4
    assert captured_events[0].type == KubeEventType.ADDED
    assert captured_events[0].resource.get("metadata").get("name") == "foobar"
    assert captured_events[1].type == KubeEventType.ADDED
    assert captured_events[1].resource.get("metadata").get("name") == "barfoo"
    assert captured_events[2].type == KubeEventType.MODIFIED
    assert captured_events[2].resource.get("metadata").get("name") == "barfoo"
    assert captured_events[3].type == KubeEventType.DELETED
    assert captured_events[3].resource.get("metadata").get("name") == "foobar"


@pytest.mark.timeout(5)
def test_watch_objects_namespaced():
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    name = "foobar"
    obj = make_obj(api_version, kind, name=name)
    dm.deploy([obj])

    update_timer_event = Timer(
        interval=0.5,
        function=dm.deploy,
        args=[[make_obj(api_version, kind, spec={"updated": 2})]],
    )
    other_update_timer_event = Timer(
        interval=0.5,
        function=dm.deploy,
        args=[[make_obj(api_version, kind, name="othername", spec={"updated": 2})]],
    )
    disable_timer_event = Timer(
        interval=1.5, function=dm.disable, args=[[make_obj(api_version, kind)]]
    )
    other_update_timer_event.start()
    update_timer_event.start()
    disable_timer_event.start()

    captured_events = []
    for event in dm.watch_objects(
        kind, api_version, namespace=SOME_OTHER_NAMESPACE, name=name, timeout=3
    ):
        captured_events.append(event)

    assert len(captured_events) == 3
    assert captured_events[0].type == KubeEventType.ADDED
    assert captured_events[0].resource.get("metadata").get("name") == name
    assert captured_events[1].type == KubeEventType.MODIFIED
    assert captured_events[1].resource.get("metadata").get("name") == name
    assert captured_events[2].type == KubeEventType.DELETED
    assert captured_events[2].resource.get("metadata").get("name") == name


def test_pre_deploy_resources():
    """Make sure that the _watches member gets initialized before deploy is
    called when a deploy manager is initialized with pre-existing resources
    """
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    DryRunDeployManager(resources=[obj])


def test_no_watch_set_status():
    """Make sure that calling set_status doesn't trigger a watch"""
    dm = DryRunDeployManager()
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)

    def die(*_, **__):
        raise RuntimeError()

    dm.register_watch(api_version, kind, die)
    dm.set_status(
        kind=obj["kind"],
        name=obj["metadata"]["name"],
        namespace=obj["metadata"]["namespace"],
        api_version=obj["apiVersion"],
        status={"foo": "bar"},
    )


def test_deploy_add_owner_reference():
    """Make sure that deploying a resource with an owner CR adds the owner
    reference given the resource and owner CR are in the same namespace
    """
    owner_cr = setup_cr(namespace=TEST_NAMESPACE)
    dm = DryRunDeployManager(owner_cr=owner_cr)
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind, namespace=TEST_NAMESPACE)
    success, changed = dm.deploy([obj])
    assert success
    assert changed
    success, content = dm.get_object_current_state(
        kind=kind,
        api_version=api_version,
        namespace=obj.metadata.namespace,
        name=obj.metadata.name,
    )
    assert success
    assert content["metadata"]["ownerReferences"] == [_make_owner_reference(owner_cr)]


def test_deploy_dont_add_owner_reference_if_manage_owner_references_disabled():
    """Make sure that deploying a resource with an owner CR does not add the
    owner reference when manage_owner_references is set to False
    """
    owner_cr = setup_cr(namespace=TEST_NAMESPACE)
    dm = DryRunDeployManager(owner_cr=owner_cr)
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind)
    success, changed = dm.deploy([obj], manage_owner_references=False)
    assert success
    assert changed
    success, content = dm.get_object_current_state(
        kind=kind,
        api_version=api_version,
        namespace=obj.metadata.namespace,
        name=obj.metadata.name,
    )
    assert success
    assert "ownerReferences" not in content["metadata"]


def test_deploy_dont_add_owner_reference_if_resource_and_owner_cr_not_in_same_namespace():
    """Make sure that deploying a resource with an owner CR does not add the owner
    reference if the resource and owner CR are in a different namespace
    """
    owner_cr = setup_cr(namespace=TEST_NAMESPACE)
    dm = DryRunDeployManager(owner_cr=owner_cr)
    api_version = "foo.bar/v1"
    kind = "Foo"
    obj = make_obj(api_version, kind, namespace=SOME_OTHER_NAMESPACE)
    success, changed = dm.deploy([obj])
    assert success
    assert changed
    success, content = dm.get_object_current_state(
        kind=kind,
        api_version=api_version,
        namespace=obj.metadata.namespace,
        name=obj.metadata.name,
    )
    assert success
    assert content["metadata"]["ownerReferences"] == []


def test_deploy_cluster_resource():
    """Make sure that deploying a resource without a namespace does not fail"""
    # Create deploy manager
    owner_cr = setup_cr()
    dm = DryRunDeployManager(owner_cr=owner_cr)
    api_version = "foo.bar/v1"
    kind = "Foo"

    # Create object and delete namespace
    obj = make_obj(api_version, kind)
    del obj["metadata"]["namespace"]

    # Deploy object
    success, changed = dm.deploy([obj], manage_owner_references=False)
    assert success
    assert changed

    success, content = dm.get_object_current_state(
        kind=kind,
        api_version=api_version,
        namespace=None,
        name=obj.metadata.name,
    )

    # Validate object was deployed
    assert success
    assert content["metadata"]["name"] == obj["metadata"]["name"]


@pytest.mark.parametrize(
    ["resources", "label_selector", "field_selector", "obj_count"],
    [
        # Label Selector Tests
        [[make_obj("foo/v1", "Foo", name="first")], "app==foobar", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app==wrong", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "app!=wrong", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app!=foobar", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "app=foobar", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app=wrong", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "app in foobar", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app in (foobar)", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app in (foobar,wrong)", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app in (wrong)", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "app in ()", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "app notin foobar", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "app notin (foobar)", None, 0],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            "app notin (foobar,wrong)",
            None,
            0,
        ],
        [[make_obj("foo/v1", "Foo", name="first")], "app notin (wrong)", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app notin ()", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "app", None, 1],
        [[make_obj("foo/v1", "Foo", name="first")], "wrong", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "!app", None, 0],
        [[make_obj("foo/v1", "Foo", name="first")], "!wrong", None, 1],
        # Label Selector MultiResult Tests
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            "app==foobar",
            None,
            2,
        ],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            "app in foobar",
            None,
            2,
        ],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            "app not in foobar",
            None,
            0,
        ],
        # Label Selector with Multiple Label Selectors
        [[make_obj("foo/v1", "Foo", name="first")], "app,app==foobar", None, 1],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            "app in (foobar),run=frontend",
            None,
            1,
        ],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            "app in (foobar),run!=backend,owner=prod",
            None,
            0,
        ],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            "app,app in (foobar,barfoo),run,run notin backend",
            None,
            1,
        ],
        # Field Selector Tests
        # no need to test so many permutations as they're covered in label selectors Tests
        [[make_obj("foo/v1", "Foo", name="first")], None, "metadata.name==first", 1],
        [[make_obj("foo/v1", "Foo", name="first")], None, "metadata.name==second", 0],
        [[make_obj("foo/v1", "Foo", name="first")], None, "spec.a!=1", 0],
        [[make_obj("foo/v1", "Foo", name="first")], None, "spec.a=1", 1],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            None,
            "metadata.labels.app in (foobar)",
            1,
        ],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            None,
            "metadata.labels.app in (wrong)",
            0,
        ],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            None,
            "metadata.labels.app notin (wrong)",
            1,
        ],
        [[make_obj("foo/v1", "Foo", name="first")], None, "!spec.b", 1],
        [[make_obj("foo/v1", "Foo", name="first")], None, "spec.a", 1],
        # Field Selector Multi Tests
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            None,
            "metadata.name = first",
            1,
        ],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            None,
            "metadata.name in (first,second)",
            2,
        ],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            None,
            "metadata.namespace notin (default)",
            2,
        ],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            None,
            "spec.a",
            2,
        ],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            None,
            "spec.a=4",
            0,
        ],
        # Label And Field Selector Combined
        [[make_obj("foo/v1", "Foo", name="first")], "app=foobar", "spec.a=1", 1],
        [
            [make_obj("foo/v1", "Foo", name="first")],
            "app in (foobar,barfoo)",
            "spec.a=2",
            0,
        ],
        [[make_obj("foo/v1", "Foo", name="first")], "app in (wrong)", "spec.a=1", 0],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            "app=foobar",
            "metadata.name==first",
            1,
        ],
        # Blank Field and Label Selectors match all
        [[make_obj("foo/v1", "Foo", name="first")], None, None, 1],
        [
            [
                make_obj("foo/v1", "Foo", name="first"),
                make_obj("foo/v1", "Foo", name="second"),
            ],
            None,
            None,
            2,
        ],
    ],
)
def test_filter_objects_current_state(
    resources, label_selector, field_selector, obj_count
):
    """Make sure that all types of field selectors and label selectors work as expected"""
    # Create deploy manager
    owner_cr = setup_cr()
    dm = DryRunDeployManager(owner_cr=owner_cr)
    api_version = resources[0].get("apiVersion")
    namespace = resources[0].get("metadata").get("namespace")
    kind = resources[0].get("kind")

    # Deploy object
    command_success, changed = dm.deploy(resources, manage_owner_references=False)
    assert command_success
    assert changed

    # Filter Object
    command_success, content = dm.filter_objects_current_state(
        kind,
        api_version=api_version,
        namespace=namespace,
        label_selector=label_selector,
        field_selector=field_selector,
    )

    assert command_success
    assert len(content) == obj_count


def test_filter_object_state_incorrect_api_version():
    """Make sure that deploying a resource without a namespace does not fail"""
    # Create deploy manager
    owner_cr = setup_cr()
    dm = DryRunDeployManager(owner_cr=owner_cr)
    api_version = "wrong"
    namespace = "default"
    kind = "Foo"

    resources = [make_obj("right", "Foo", namespace=namespace, name="first")]

    # Deploy object
    command_success, changed = dm.deploy(resources, manage_owner_references=False)
    assert command_success
    assert changed

    # Filter Object
    command_success, content = dm.filter_objects_current_state(
        kind,
        api_version=api_version,
        namespace=namespace,
        label_selector=None,
        field_selector=None,
    )

    assert command_success
    assert len(content) == 0
