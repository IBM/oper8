"""
Tests for functions in oper8.utils
"""

# Standard
import contextlib
import datetime
import io

# Third Party
import pytest

# Local
from oper8 import constants, utils
from oper8.dag import ResourceNode
from oper8.test_helpers.helpers import library_config, setup_cr, setup_session

## Helpers #####################################################################


def make_session_annotations(annotations):
    return setup_session(full_cr=setup_cr(metadata={"annotations": annotations}))


## get_passthrough_annotations #################################################


def test_get_passthrough_annotations_all_present():
    """Make sure that all expected annotations do get passed through"""
    annotations = {k: "foobar" for k in constants.PASSTHROUGH_ANNOTATIONS}
    passthroughs = utils.get_passthrough_annotations(
        make_session_annotations(annotations)
    )
    assert annotations == passthroughs

    # Since it's particularly important, explicitly make sure that the
    # config_defaults annotation is included
    assert constants.CONFIG_DEFAULTS_ANNOTATION_NAME in passthroughs


def test_get_passthrough_annotations_some_present():
    """Make sure that only the oper8 annotations present are passed through and
    new ones are not added
    """
    annotations = {
        constants.PAUSE_ANNOTATION_NAME: "False",
        constants.LOG_DEFAULT_LEVEL_NAME: "debug",
        constants.LOG_FILTERS_NAME: "FOO:off,BAR:debug4",
    }
    assert (
        utils.get_passthrough_annotations(make_session_annotations(annotations))
        == annotations
    )


def test_get_passthrough_annotations_ignore_others():
    """Make sure that non-oper8 annotations are not passed through"""
    annotations = {
        "other": "annotation",
        constants.PAUSE_ANNOTATION_NAME: "False",
    }
    passthroughs = utils.get_passthrough_annotations(
        make_session_annotations(annotations)
    )
    assert "other" not in passthroughs


def test_get_passthrough_annotations_no_temporary_patches():
    """Make sure that the TemporaryPatch annotation is NOT passed through"""
    annotations = {
        constants.TEMPORARY_PATCHES_ANNOTATION_NAME: "foobar",
    }
    passthroughs = utils.get_passthrough_annotations(
        make_session_annotations(annotations)
    )
    assert constants.TEMPORARY_PATCHES_ANNOTATION_NAME not in passthroughs


## nested_set / nested_get #####################################################


def test_nested_set_get():
    """Test nested_set and nested_get"""

    # Happy Path
    d = {}
    utils.nested_set(d, "foo.bar", 1)
    assert utils.nested_get(d, "foo.bar") == 1
    assert "foo" in d
    assert "bar" in d["foo"]
    assert d["foo"]["bar"] == 1

    # Bad intermediate key
    with pytest.raises(TypeError):
        utils.nested_set({"foo": 1}, "foo.bar", 1)
    with pytest.raises(TypeError):
        utils.nested_get({"foo": 1}, "foo.bar")

    # Get intermediate missing
    assert utils.nested_get({}, "foo.bar") is None
    assert utils.nested_get({}, "foo.bar", "default") == "default"
    assert utils.nested_get({"foo": {}}, "foo.bar", "default") == "default"


## classproperty decorators ####################################################


def test_classproperty():
    """Make sure that an attribute defined with @classproperty can be accessed
    without an instance of the class
    """

    class Foo:
        @utils.classproperty
        def bar(cls):
            return 1

    assert Foo.bar == 1


def test_abstractclassproperty():
    """Make sure that an attribute defined with @abstractclassproperty cannot be
    accessed without an implementation, but can be with a valid @classproperty
    implementation or a class member value
    """

    class FooBase:
        @utils.abstractclassproperty
        def bar(cls):
            """This will raise"""

        def __init__(self):
            self.bar

    class FooChildOne(FooBase):
        @utils.classproperty
        def bar(cls):
            return 1

    class FooChildTwo(FooBase):
        bar = 2

    class FooChildThree(FooBase):
        pass

    setattr(FooChildThree, "bar", 3)

    with pytest.raises(NotImplementedError):
        FooBase()

    assert FooChildOne.bar == 1
    assert FooChildTwo.bar == 2
    assert FooChildThree.bar == 3

    # Make sure help can be called on an abstractclassproperty without raising
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        help(FooBase.bar)
    buf.seek(0)
    help_str = buf.read()
    assert help_str


## sanitize_for_serialization #################################################


def get_generic_openapi_resource_type(provided_map=None):
    class OpenApiResource:
        attribute_map = provided_map

        @property
        def property() -> str:
            return "classproperty"

        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    return OpenApiResource


def test_sanitize_for_serialization_types():
    """Make sure that the internal resource name is formatted correctly.
    This one is also pretty basic...
    """
    output_dict = (
        utils.sanitize_for_serialization(
            {
                "kind": "Foo",
                "metadata": {"name": "test"},
                "spec": {
                    "none": None,
                    "should_be_empty": {
                        "null": None,
                    },
                    "list": ["listitem"],
                    "tuple": ("tupleitem",),
                    "date": datetime.datetime(2020, 1, 1),
                    "resourceNode": ResourceNode(
                        name="test", manifest={"metadata": {"name": "test"}}
                    ),
                    "classProperty": get_generic_openapi_resource_type().property,
                    "openapiType": get_generic_openapi_resource_type(
                        {
                            "api_version": "apiVersion",
                            "kind": "kind",
                            "metadata": "metadata",
                            "spec": "spec",
                            "status": "status",
                        }
                    )(
                        api_version="v1",
                        kind="Test",
                        spec={"container": []},
                        metadata={"name": "test"},
                        status=get_generic_openapi_resource_type(
                            {"reconciled_version": "reconciledVersion"}
                        )(reconciled_version=1),
                    ),
                },
            },
        ),
    )
    assert output_dict[0] == {
        "kind": "Foo",
        "metadata": {"name": "test"},
        "spec": {
            "date": "2020-01-01T00:00:00",
            "list": ["listitem"],
            "classProperty": "classproperty",
            "openapiType": {
                "apiVersion": "v1",
                "kind": "Test",
                "metadata": {"name": "test"},
                "spec": {"container": []},
                "status": {"reconciledVersion": 1},
            },
            "resourceNode": {"metadata": {"name": "test"}},
            "should_be_empty": {},
            "tuple": ("tupleitem",),
        },
    }


## Manifest Version #################################################


def test_get_manifest_version():
    cr = setup_cr(version="1.2.3")
    assert utils.get_manifest_version(cr) == "1.2.3"

    with library_config(vcs={"version_override": "2.0.0"}):
        assert utils.get_manifest_version(cr) == "2.0.0"


## Finalizers #################################################


@pytest.mark.parametrize(
    ["cr", "finalizer", "expected_finalizers"],
    [
        [setup_cr(), "testfinalizer", ["testfinalizer"]],
        [
            setup_cr(metadata={"finalizers": ["otherfinalizer"]}),
            "testfinalizer",
            ["testfinalizer", "otherfinalizer"],
        ],
        [
            setup_cr(metadata={"finalizers": ["testfinalizer"]}),
            "testfinalizer",
            ["testfinalizer"],
        ],
    ],
)
def test_add_finalizer(cr, finalizer, expected_finalizers):
    session = setup_session(full_cr=cr)

    utils.add_finalizer(session, finalizer)

    assert set(session.finalizers) == set(expected_finalizers)

    success, obj = session.get_object_current_state(
        session.kind, session.name, session.api_version
    )
    assert success
    assert set(obj["metadata"]["finalizers"]) == set(expected_finalizers)


@pytest.mark.parametrize(
    ["cr", "finalizer", "expected_finalizers"],
    [
        [setup_cr(), "testfinalizer", []],
        [setup_cr(metadata={"finalizers": ["testfinalizer"]}), "testfinalizer", []],
        [
            setup_cr(metadata={"finalizers": ["testfinalizer", "otherfinalizer"]}),
            "testfinalizer",
            ["otherfinalizer"],
        ],
    ],
)
def test_remove_finalizer(cr, finalizer, expected_finalizers):
    session = setup_session(full_cr=cr)

    utils.remove_finalizer(session, finalizer)

    assert set(session.finalizers) == set(expected_finalizers)

    success, obj = session.get_object_current_state(
        session.kind, session.name, session.api_version
    )
    assert success
    assert set(obj["metadata"]["finalizers"]) == set(expected_finalizers)


def test_remove_finalizer_not_found():
    """Make sure that when the resource has already been removed from the
    cluster, there is no error on remove_finalizer
    """
    session = setup_session(full_cr=setup_cr(), deploy_initial_cr=False)
    utils.remove_finalizer(session, "testfinalizer")
    success, obj = session.get_object_current_state(
        session.kind, session.name, session.api_version
    )
    assert success
    assert not obj
