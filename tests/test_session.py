"""
Tests for all functionality of the Session object
"""

# Third Party
import pytest

# First Party
import aconfig

# Local
from oper8.patch import STRATEGIC_MERGE_PATCH
from oper8.session import MAX_NAME_LEN, Session
from oper8.status import make_application_status
from oper8.test_helpers.helpers import (
    DummyNodeComponent,
    MockDeployManager,
    make_patch,
    setup_cr,
)

## Helpers #####################################################################


def make_component_class(comp_name):
    class DerivedComponent(DummyNodeComponent):
        name = comp_name

    return DerivedComponent


def make_api_obj(
    kind="Foo",
    api_version="foo.bar/v1",
    name="foo",
    namespace="testit",
):
    return aconfig.Config(
        {
            "kind": kind,
            "apiVersion": api_version,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"app": "test", "run": name},
            },
        },
        override_env_vars=False,
    )


## Tests #######################################################################

###############
## Prperties ##
###############


def test_constructed_properties():
    """Make sure all properties derived from the constructor args are populated
    correctly
    """
    rec_id = "1ab"
    cr = setup_cr()
    cfg = aconfig.Config({"foo": "bar"}, override_env_vars=False)
    dm = MockDeployManager()
    patches = [make_patch(STRATEGIC_MERGE_PATCH, {})]
    session = Session(rec_id, cr, cfg, dm, patches)
    assert session.id == rec_id
    assert session.cr_manifest == cr
    assert session.config == cfg
    assert session.deploy_manager == dm
    assert session.temporary_patches == patches


def test_cr_properties():
    """Make sure all properties derived from the CR manifest are populated
    correctly
    """
    version = "develop.1.2.3"
    namespace = "wingbat"
    name = "wombat"
    api_version = "critters.bats/v23"
    kind = "Critter"
    spec = {"key": "value"}
    cr = setup_cr(
        api_version=api_version,
        kind=kind,
        namespace=namespace,
        name=name,
        version=version,
        spec=spec,
    )
    session = Session("1ab", cr, {}, MockDeployManager())
    assert session.version == version
    assert session.namespace == namespace
    assert session.name == name
    assert session.kind == kind
    assert session.api_version == api_version
    assert session.spec == cr.spec
    assert session.metadata == cr.metadata


@pytest.mark.parametrize(
    "field",
    ["kind", "apiVersion", "metadata", "metadata.name", "metadata.namespace"],
)
def test_missing_cr_required_fields(field):
    """Make sure that required fields missing from the CR correctly raise
    validation errors
    """
    cr = setup_cr()
    field_parts = field.split(".")
    dct = cr
    while len(field_parts) > 1:
        dct = dct[field_parts[0]]
        field_parts = field_parts[1:]
    del dct[field_parts[0]]
    with pytest.raises(AssertionError):
        session = Session("1ab", cr, {}, MockDeployManager())


@pytest.mark.parametrize(
    "field,expected",
    [("spec", aconfig.Config({})), ("spec.version", None)],
)
def test_missing_cr_optional_fields(field, expected):
    """Make sure that optional fields in the CR that are accessed via properties
    do not raise errors
    """
    cr = setup_cr()
    field_parts = field.split(".")
    dct = cr
    while len(field_parts) > 1:
        dct = dct[field_parts[0]]
        field_parts = field_parts[1:]
    del dct[field_parts[0]]
    session = Session("1ab", cr, {}, MockDeployManager())
    assert getattr(session, field.split(".")[-1]) == expected


def test_current_version():
    """Make sure that retrieving the current_version works when it's present
    in the deploy manager
    """
    # Make sure that current_version is not set when it hasn't been deployed
    cr = setup_cr()
    dm = MockDeployManager()
    session = Session("1ab", cr, {}, dm)
    assert session.current_version is None

    # Make sure that current_version is set when it's been deployed before
    current_version = "some-version"
    cr.status = make_application_status(version=current_version)
    dm.deploy([cr])
    session = Session("2cd", cr, {}, dm)
    assert session.current_version == current_version


def test_dict_conversion():
    """Make sure that a CR as a raw dict is converted (for that 100% cov!)"""
    Session("1ab", dict(setup_cr()), {}, MockDeployManager())


######################
## State Management ##
######################


def test_add_component_ok():
    """Make sure that add_component successfully adds enabled and disabled
    components
    """
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    enabled_comp = make_component_class("enabled")(session)
    disabled_comp = make_component_class("disabled")(session, disabled=True)
    assert session.get_components() == [enabled_comp]
    assert session.get_components(disabled=True) == [disabled_comp]


def test_add_component_no_duplicate():
    """Make sure that the same component cannot be added multiple times"""
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    CompType = make_component_class("comp")
    CompType(session)
    with pytest.raises(ValueError):
        CompType(session)


def test_add_component_dependency_ok():
    """Make sure that adding a dependency from one component to another works as
    expected
    """
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    comp1 = make_component_class("one")(session)
    comp2 = make_component_class("two")(session)
    session.add_component_dependency(comp1, comp2)
    assert session.get_component_dependencies(comp1.name) == [(comp2, None)]


def test_add_component_dependency_verify_function():
    """Make sure that adding a dependency with a verify function stores the
    verify function successfully
    """
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    comp1 = make_component_class("one")(session)
    comp2 = make_component_class("two")(session)
    verify_fn = lambda *_, **__: True
    session.add_component_dependency(comp1, comp2, verify_function=verify_fn)
    dep_list = session.get_component_dependencies(comp1.name)
    assert len(dep_list) == 1
    assert dep_list[0][0] == comp2
    assert dep_list[0][1].func == verify_fn


def test_add_component_dependency_by_name():
    """Make sure that adding a dependency can be done by name"""
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    comp1 = make_component_class("one")(session)
    comp2 = make_component_class("two")(session)
    session.add_component_dependency(comp1.name, comp2.name)
    assert session.get_component_dependencies(comp1.name) == [(comp2, None)]


def test_add_component_dependency_no_unknown_component():
    """Make sure that a dependency can't be added for an unknown component"""
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    comp2 = make_component_class("two")(session)
    with pytest.raises(ValueError):
        session.add_component_dependency("one", comp2)


def test_add_component_dependency_no_unknown_upstream():
    """Make sure that a dependency can't be added for an unknown upstream"""
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    comp1 = make_component_class("one")(session)
    with pytest.raises(ValueError):
        session.add_component_dependency(comp1, "two")


def test_add_component_dependency_no_disabled_component():
    """Make sure that a dependency can't be added for a disabled component"""
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    enabled_comp = make_component_class("enabled")(session)
    disabled_comp = make_component_class("disabled")(session, disabled=True)
    with pytest.raises(ValueError):
        session.add_component_dependency(disabled_comp, enabled_comp)


def test_add_component_dependency_no_disabled_upstream():
    """Make sure that a dependency can't be added for a disabled upstream"""
    session = Session("1ab", setup_cr(), {}, MockDeployManager())
    enabled_comp = make_component_class("enabled")(session)
    disabled_comp = make_component_class("disabled")(session, disabled=True)
    with pytest.raises(ValueError):
        session.add_component_dependency(enabled_comp, disabled_comp)


###############
## Utilities ##
###############


def test_get_scoped_name():
    """Make sure that get_scoped_name correctly scopes and truncates"""
    name = "some-name"
    dm = MockDeployManager()
    session = Session("1ab", setup_cr(name=name), {}, dm)

    # Test a name shorter than the max length
    assert session.get_scoped_name("foo") == f"{name}-foo"

    # Test a name longer than the max length
    scoped_name = session.get_scoped_name(
        "".join((str(i % 10) for i in range(MAX_NAME_LEN)))
    )
    assert len(scoped_name) == MAX_NAME_LEN
    assert scoped_name.startswith(name)


def test_get_truncated_name():
    """Make sure that get_truncated_name correctly (only) truncates"""
    name = "some-name"
    dm = MockDeployManager()
    session = Session("1ab", setup_cr(name=name), {}, dm)

    # Test a name shorter than the max length
    assert session.get_truncated_name("foo") == "foo"

    # Test a name longer than the max length
    long_name = "".join((str(i % 10) for i in range(MAX_NAME_LEN * 2)))
    trunc_name = session.get_truncated_name(long_name)
    assert len(trunc_name) == MAX_NAME_LEN
    assert not trunc_name.startswith(name)

    # Test that truncation makes names unique when the instance name is longer
    # than the max
    session = Session("1ab", setup_cr(name=long_name), {}, dm)
    assert session.get_truncated_name("foo") != session.get_truncated_name("bar")


def test_get_object_current_state():
    """Make sure that get_object_current_state correctly proxies to the deploy
    manager with the current namespace
    """
    dm = MockDeployManager()
    session = Session("1ab", setup_cr(), {}, dm)
    obj1 = make_api_obj(namespace=session.namespace)
    obj2 = make_api_obj(namespace="different-namespace", name="other-name")
    dm.deploy([obj1, obj2])

    # Make sure it fetches correctly with api_version in the same namespace
    success, obj1_state = session.get_object_current_state(
        kind=obj1.kind,
        api_version=obj1.apiVersion,
        name=obj1.metadata.name,
    )
    assert success
    assert obj1_state == obj1

    # Make sure it fetches correctly without api_version in the same namespace
    success, obj1_state = session.get_object_current_state(
        kind=obj1.kind,
        name=obj1.metadata.name,
    )
    assert success
    assert obj1_state == obj1

    # Make sure it doesn't find objects in different namespaces
    success, obj2_state = session.get_object_current_state(
        kind=obj2.kind,
        api_version=obj2.apiVersion,
        name=obj2.metadata.name,
    )
    assert success
    assert obj2_state is None


def test_filter_objects_current_state():
    """Make sure that filter_object_current_state correctly proxies to the deploy
    manager with the current namespace
    """
    dm = MockDeployManager()
    session = Session("1ab", setup_cr(), {}, dm)
    obj1 = make_api_obj(namespace=session.namespace, name="first")
    obj2 = make_api_obj(namespace=session.namespace, name="second")
    obj3 = make_api_obj(namespace="different-namespace", name="other-name")
    dm.deploy([obj1, obj2, obj3])

    # Make sure it fetches correctly in the same namespace
    success, obj_list = session.filter_objects_current_state(
        kind=obj1.kind,
        api_version=obj1.apiVersion,
    )
    assert success
    assert len(obj_list) == 2
    assert obj1 in obj_list and obj2 in obj_list

    # Make sure it fetches correctly filters labels
    success, obj_list = session.filter_objects_current_state(
        kind=obj1.kind, label_selector="run=first"
    )
    assert success
    assert len(obj_list) == 1
    assert obj_list[0] == obj1

    # Make sure it fetches correctly filters fields
    success, obj_list = session.filter_objects_current_state(
        kind=obj1.kind, field_selector="metadata.name!=first"
    )
    assert success
    assert len(obj_list) == 1
    assert obj_list[0] == obj2

    # Make sure it doesn't find objects in different namespaces
    success, obj_list = session.filter_objects_current_state(
        kind=obj2.kind, field_selector="metadata.name=other-name"
    )
    assert success
    assert len(obj_list) == 0
