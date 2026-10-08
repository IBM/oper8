"""
Test the patching semantics
"""

# Standard
import copy

# Third Party
import pytest

# First Party
import aconfig
import alog

# Local
from oper8.exceptions import ConfigError
from oper8.patch import JSON_PATCH_6902, STRATEGIC_MERGE_PATCH, apply_patches
from oper8.test_helpers.helpers import configure_logging, make_patch

configure_logging()

log = alog.use_channel("TEST")

## Helpers #####################################################################


def sample_foo(base):
    base.setdefault("kind", "Foo")
    base.setdefault("apiVersion", "v1")
    base.setdefault("metadata", {}).setdefault("name", "foo")
    return base


def sample_pod(containers=None):
    return {
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {"name": "foo"},
        "spec": {"containers": containers or []},
    }


def pod_psm_body(containers=None, object_name="foo.pod"):
    out = {}
    inner = out
    for part in object_name.split("."):
        inner = inner.setdefault(part, {})
    inner.setdefault("spec", {})["containers"] = containers or []
    return out


## Tests #######################################################################

###################
## patchJson6902 ##
###################


def test_js6902_apply_patch():
    """Test that a simple patchJson6902 patch applies cleanly"""
    obj = sample_foo({"key": "value", "nested": {"key": "nested_value"}})
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "blob": [
                    {"op": "replace", "path": "/key", "value": "replaced"},
                    {
                        "op": "replace",
                        "path": "/nested/key",
                        "value": "nested_replaced",
                    },
                ]
            }
        },
    )
    res = apply_patches("foo.blob", obj, [patch])
    assert res["key"] == "replaced"
    assert res["nested"]["key"] == "nested_replaced"


def test_js6902_non_list():
    """Test that a non-list js6902 patch raises an error"""
    obj = sample_foo({"key": "value", "nested": {"key": "nested_value"}})
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "blob": {"op": "replace", "path": "/key", "value": "replaced"},
            }
        },
    )
    with pytest.raises(ValueError):
        apply_patches("foo.blob", obj, [patch])


#########################
## patchStrategicMerge ##
#########################

## Happy Path ##


def test_psm_apply_patch():
    """Test that a simple patchStrategicMerge patch applies cleanly"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "Always"},
                {"name": "bar", "image": "bar"},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert len(res["spec"]["containers"]) == 2
    assert res["spec"]["containers"][0] == {
        "name": "foo",
        "image": "foo",
        "restartPolicy": "Always",
    }
    assert res["spec"]["containers"][1] == {"name": "bar", "image": "bar"}


def test_psm_update_key():
    """Test that a key can be updated in an object"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "OnFailure"},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert len(res["spec"]["containers"]) == 1
    assert res["spec"]["containers"][0] == {
        "name": "foo",
        "image": "foo",
        "restartPolicy": "OnFailure",
    }


def test_psm_delete_key():
    """Test that a key can be deleted from an object"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": None},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert len(res["spec"]["containers"]) == 1
    assert res["spec"]["containers"][0] == {"name": "foo", "image": "foo"}


def test_psm_delete_dict_list_item():
    """Test that an element can be deleted from a list of dicts"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "$patch": "delete"},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert len(res["spec"]["containers"]) == 0


def test_psm_delete_primitive_list_items():
    """Test that an element can be deleted from a list of primitives"""
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"foo": {"$deleteFromPrimitiveList/key": [2, 4]}}},
    )
    res = apply_patches("foo.foo", obj, [patch])
    assert res["key"] == [1, 3]


def test_psm_list_element_replace():
    """Test that an element in a list can be replaced without merging"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    replaced_foo = {
        "name": "foo",
        "$patch": "replace",
        "image": "foo:latest",
        "foo": "bar",
    }
    patch = make_patch(STRATEGIC_MERGE_PATCH, pod_psm_body([replaced_foo]))
    res = apply_patches("foo.pod", obj, [patch])
    assert len(res["spec"]["containers"]) == 1
    del replaced_foo["$patch"]
    assert res["spec"]["containers"][0] == replaced_foo


def test_psm_list_no_merge_key_replace():
    """Test that for a list with no merge key, it is direclty replaced"""
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = make_patch(STRATEGIC_MERGE_PATCH, {"foo": {"foo": {"key": [2, 4]}}})
    res = apply_patches("foo.foo", obj, [patch])
    assert res["key"] == [2, 4]


## Error Cases ##


def test_psm_bad_directive():
    """Test that an unknown directive throws an error"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH, pod_psm_body([{"name": "foo", "$patch": "flipflop"}])
    )
    with pytest.raises(ValueError):
        apply_patches("foo.pod", obj, [patch])


def test_psm_missing_merge_key_patch():
    """Test that an element in a patch list that is missing the merge key
    causes an error
    """
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(STRATEGIC_MERGE_PATCH, pod_psm_body([{"foo": "bar"}]))
    with pytest.raises(ValueError):
        apply_patches("foo.pod", obj, [patch])


def test_psm_missing_merge_key_current():
    """Test that an element in a current list that is missing the merge key
    causes an error
    """
    obj = sample_pod([{"image": "foo"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH, pod_psm_body([{"name": "foo", "foo": "bar"}])
    )
    with pytest.raises(ValueError):
        apply_patches("foo.pod", obj, [patch])


def test_psm_primitive_delete_not_there():
    """Test that a request to delete a primitive from a list causes an error if
    the element is not present
    """
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {"foo": {"foo": {"$deleteFromPrimitiveList/keyNotThere": [2, 4]}}},
    )
    with pytest.raises(ValueError):
        apply_patches("foo.foo", obj, [patch])


def test_psm_primitive_delete_patch_not_list():
    """Test that a request to delete a primitive from a list where the patch
    value is not a list causes an error
    """
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = make_patch(
        STRATEGIC_MERGE_PATCH, {"foo": {"foo": {"$deleteFromPrimitiveList/key": 2}}}
    )
    with pytest.raises(ValueError):
        apply_patches("foo.foo", obj, [patch])


def test_psm_primitive_delete_current_not_list():
    """Test that a request to delete a primitive from a list where the current
    value is not a list causes an error
    """
    obj = sample_foo({"key": {"a": 1, "b": 2}})
    patch = make_patch(
        STRATEGIC_MERGE_PATCH, {"foo": {"foo": {"$deleteFromPrimitiveList/key": ["a"]}}}
    )
    with pytest.raises(ValueError):
        apply_patches("foo.foo", obj, [patch])


def test_psm_primitive_delete_element_not_found():
    """Test that a request to delete a primitive from a list which doesn't
    contain the element raises an error
    """
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = make_patch(
        STRATEGIC_MERGE_PATCH, {"foo": {"foo": {"$deleteFromPrimitiveList/key": [5]}}}
    )
    with pytest.raises(ValueError):
        apply_patches("foo.foo", obj, [patch])


def test_psm_delete_element_not_found():
    """Test that a `$patch: delete` directive to delete an element from a list
    that isn't found raises an error
    """
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        STRATEGIC_MERGE_PATCH, pod_psm_body([{"name": "bar", "$patch": "delete"}])
    )
    with pytest.raises(ValueError):
        apply_patches("foo.pod", obj, [patch])


#############
## general ##
#############


def test_bad_patch_type():
    """Test that a bad patch type raises an error"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    with pytest.raises(ValueError):
        apply_patches(
            "foo.pod", obj, [make_patch("BAD_TYPE", pod_psm_body([{"name": "foo"}]))]
        )


def test_mixed_patch_types():
    """Test that multiple patches of different types can be applied to the same
    object
    """
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch1 = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "add", "path": "/spec/imagePullPolicy", "value": "Always"}
                ]
            }
        },
    )
    patch2 = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "Always"},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch1, patch2])
    expected = obj
    expected["spec"]["imagePullPolicy"] = "Always"
    expected["spec"]["containers"][0]["restartPolicy"] = "Always"
    assert res == expected


def test_patch_order():
    """Test that an ordered list of patches is applied in the correct order"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch1 = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "Always", "image": "image1"},
            ]
        ),
    )
    patch2 = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "OnFailure"},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch1, patch2])
    expected = obj
    expected["spec"]["containers"][0]["restartPolicy"] = "OnFailure"
    expected["spec"]["containers"][0]["image"] = "image1"
    assert res == expected


def test_only_applicable_patches():
    """Test that only the patches for the given object are applied, and all
    other patches are ignored
    """
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch1 = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "Always", "image": "image1"},
            ],
            object_name="bar.pod",
        ),
    )
    patch2 = make_patch(
        STRATEGIC_MERGE_PATCH,
        pod_psm_body(
            [
                {"name": "foo", "restartPolicy": "OnFailure"},
            ]
        ),
    )
    res = apply_patches("foo.pod", obj, [patch1, patch2])
    expected = obj
    expected["spec"]["containers"][0]["restartPolicy"] = "OnFailure"
    assert res == expected
    # Just to make it clear
    assert res["spec"]["containers"][0]["image"] == "foo"


#########################
## Identity Protection ##
#########################


@pytest.mark.parametrize(
    "path",
    [
        "/metadata/namespace",
        "/metadata/name",
        "/kind",
        "/apiVersion",
    ],
)
def test_json6902_identity_field_changes_raise(path):
    """JSON6902 patches changing identity fields must raise ConfigError"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "replace", "path": path, "value": "changed"},
                ]
            }
        },
    )
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])


def test_json6902_replace_metadata_raises():
    """JSON6902 replace on /metadata with a dict raises ConfigError"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "replace", "path": "/metadata", "value": {"labels": {}}},
                ]
            }
        },
    )
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])


def test_json6902_move_from_metadata_name_raises():
    """JSON6902 move with from=/metadata/name raises ConfigError"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "move", "path": "/metadata/name", "from": "/metadata/name"},
                ]
            }
        },
    )
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])


def test_json6902_root_replace_raises():
    """JSON6902 root (empty path) replace raises ConfigError"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "replace", "path": "", "value": {"kind": "Test"}},
                ]
            }
        },
    )
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])


def test_json6902_test_op_on_namespace_allowed():
    """JSON6902 test operations on protected fields must be allowed (read-only)"""
    obj = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo", "namespace": "test-ns"},
        "spec": {"containers": []},
    }
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "test", "path": "/metadata/namespace", "value": "test-ns"},
                ]
            }
        },
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert res["metadata"]["namespace"] == "test-ns"


def test_json6902_copy_from_name_to_labels_allowed():
    """JSON6902 copy operations from /metadata/name to /metadata/labels/name must work"""
    obj = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo", "namespace": "test", "labels": {}},
        "spec": {"containers": []},
    }
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {
                        "op": "copy",
                        "path": "/metadata/labels/name",
                        "from": "/metadata/name",
                    },
                ]
            }
        },
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert res["metadata"]["labels"]["name"] == "foo"


def test_json6902_labels_annotations_allowed():
    """JSON6902 operations on /metadata/labels and /metadata/annotations must work"""
    obj = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "foo",
            "namespace": "test",
            "labels": {},
            "annotations": {},
        },
        "spec": {"containers": []},
    }
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "add", "path": "/metadata/labels/foo", "value": "bar"},
                    {"op": "add", "path": "/metadata/annotations/test", "value": "val"},
                ]
            }
        },
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert res["metadata"]["labels"]["foo"] == "bar"
    assert res["metadata"]["annotations"]["test"] == "val"


def test_identity_protection_preserves_input_not_mutated():
    """Verify the original input resource_definition dict is not mutated in raising case"""
    obj = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "foo", "namespace": "test"},
        "spec": {"containers": []},
    }
    original = copy.deepcopy(obj)
    patch = make_patch(
        JSON_PATCH_6902,
        {
            "foo": {
                "pod": [
                    {"op": "replace", "path": "/metadata/namespace", "value": "new-ns"},
                ]
            }
        },
    )
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])
    # Verify input was not mutated
    assert obj == original


@pytest.mark.parametrize(
    "operation",
    [
        {"op": "remove", "path": "/metadata/namespace"},
        {"op": "add", "path": "/metadata/namespace", "value": "other"},
        {"op": "remove", "path": "/metadata"},
        {"op": "add", "path": "/metadata/name/foo", "value": "bar"},
        {"op": "move", "from": "/metadata", "path": "/spec/metadata"},
        {"op": "copy", "from": "/spec", "path": "/metadata"},
    ],
)
def test_json6902_other_identity_operations_raise(operation):
    """Make sure that add/remove/move/copy operations that would touch the
    protected identity fields are rejected
    """
    obj = sample_pod()
    obj["metadata"]["namespace"] = "test"
    patch = make_patch(JSON_PATCH_6902, {"foo": {"pod": [operation]}})
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])


@pytest.mark.parametrize(
    "body",
    [
        {"metadata": {"namespace": "other"}},
        {"metadata": {"namespace": None}},
        {"metadata": {"name": "other"}},
        {"metadata": None},
        {"kind": "ConfigMap"},
        {"apiVersion": "v2"},
    ],
)
def test_psm_identity_field_changes_raise(body):
    """Strategic merge patches changing identity fields must raise ConfigError"""
    obj = sample_pod()
    obj["metadata"]["namespace"] = "test"
    original = copy.deepcopy(obj)
    patch = make_patch(STRATEGIC_MERGE_PATCH, {"foo": {"pod": body}})
    with pytest.raises(ConfigError):
        apply_patches("foo.pod", obj, [patch])
    assert obj == original


def test_psm_non_identity_metadata_allowed():
    """Strategic merge patches that modify metadata without changing the
    identity of the resource are allowed
    """
    obj = sample_pod()
    obj["metadata"]["namespace"] = "test"
    patch = make_patch(
        STRATEGIC_MERGE_PATCH,
        {
            "foo": {
                "pod": {
                    "metadata": {"namespace": "test", "labels": {"foo": "bar"}},
                    "spec": {"restartPolicy": "Never"},
                }
            }
        },
    )
    res = apply_patches("foo.pod", obj, [patch])
    assert res["metadata"]["labels"] == {"foo": "bar"}
    assert res["metadata"]["namespace"] == "test"
    assert res["spec"]["restartPolicy"] == "Never"


def test_identity_check_only_matching_patches():
    """Make sure that a patch for a different resource does not trigger the
    identity check for an unrelated resource
    """
    obj = sample_pod()
    patch = make_patch(
        JSON_PATCH_6902,
        {"bar": {"pod": [{"op": "replace", "path": "/spec", "value": {}}]}},
    )
    assert apply_patches("foo.pod", obj, [patch]) == obj
