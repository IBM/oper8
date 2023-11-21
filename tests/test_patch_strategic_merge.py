"""
Test the patch_strategic_merge implementation
"""

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.patch_strategic_merge import patch_strategic_merge
from oper8.test_helpers.helpers import configure_logging

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


def pod_psm_body(containers=None):
    out = {}
    out.setdefault("spec", {})["containers"] = containers or []
    return out


## Tests #######################################################################

################
## Happy Path ##
################


def test_apply_patch():
    """Test that a simple patchStrategicMerge patch applies cleanly"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = pod_psm_body(
        [
            {"name": "foo", "restartPolicy": "Always"},
            {"name": "bar", "image": "bar"},
        ]
    )
    res = patch_strategic_merge(obj, patch)
    assert len(res["spec"]["containers"]) == 2
    assert res["spec"]["containers"][0] == {
        "name": "foo",
        "image": "foo",
        "restartPolicy": "Always",
    }
    assert res["spec"]["containers"][1] == {"name": "bar", "image": "bar"}


def test_update_key():
    """Test that a key can be updated in an object"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = pod_psm_body(
        [
            {"name": "foo", "restartPolicy": "OnFailure"},
        ]
    )
    res = patch_strategic_merge(obj, patch)
    assert len(res["spec"]["containers"]) == 1
    assert res["spec"]["containers"][0] == {
        "name": "foo",
        "image": "foo",
        "restartPolicy": "OnFailure",
    }


def test_delete_key():
    """Test that a key can be deleted from an object"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = pod_psm_body(
        [
            {"name": "foo", "restartPolicy": None},
        ]
    )
    res = patch_strategic_merge(obj, patch)
    assert len(res["spec"]["containers"]) == 1
    assert res["spec"]["containers"][0] == {"name": "foo", "image": "foo"}


def test_delete_dict_list_item():
    """Test that an element can be deleted from a list of dicts"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = pod_psm_body(
        [
            {"name": "foo", "$patch": "delete"},
        ]
    )
    res = patch_strategic_merge(obj, patch)
    assert len(res["spec"]["containers"]) == 0


def test_add_dict_list_item():
    """Test that an element can be added to a list of dicts"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    patch = pod_psm_body(
        [
            {"name": "bar", "restartPolicy": "Maybe"},
        ]
    )
    res = patch_strategic_merge(obj, patch)
    assert len(res["spec"]["containers"]) == 2


def test_delete_primitive_list_items():
    """Test that an element can be deleted from a list of primitives"""
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = {"$deleteFromPrimitiveList/key": [2, 4]}
    res = patch_strategic_merge(obj, patch)
    assert res["key"] == [1, 3]


def test_list_element_replace():
    """Test that an element in a list can be replaced without merging"""
    obj = sample_pod([{"name": "foo", "image": "foo", "restartPolicy": "Always"}])
    replaced_foo = {
        "name": "foo",
        "$patch": "replace",
        "image": "foo:latest",
        "foo": "bar",
    }
    patch = pod_psm_body([replaced_foo])
    res = patch_strategic_merge(obj, patch)
    assert len(res["spec"]["containers"]) == 1
    del replaced_foo["$patch"]
    assert res["spec"]["containers"][0] == replaced_foo


def test_list_no_merge_key_replace():
    """Test that for a list with no merge key, it is directly replaced"""
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = {"key": [2, 4]}
    res = patch_strategic_merge(obj, patch)
    assert res["key"] == [2, 4]


def test_list_custom_merge_patch_keys():
    """Test that list merging is performed correctly when a custom set of
    merge_patch_keys is given
    """
    obj = sample_foo(
        {
            "key": [
                {"name": "one", "nestedKey": "foo", "baz": "bat"},
                {"name": "two", "nestedKey": "bar", "baz": "bat"},
            ]
        }
    )
    patch = {"key": [{"name": "two", "nestedKey": "OVERRIDE"}]}
    res = patch_strategic_merge(obj, patch, merge_patch_keys={"Foo.key": "name"})
    assert res["key"] == [
        {"name": "one", "nestedKey": "foo", "baz": "bat"},
        {"name": "two", "nestedKey": "OVERRIDE", "baz": "bat"},
    ]


#################
## Error Cases ##
#################


def test_bad_directive():
    """Test that an unknown directive throws an error"""
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = pod_psm_body([{"name": "foo", "$patch": "flipflop"}])
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_missing_merge_key_patch():
    """Test that an element in a patch list that is missing the merge key
    causes an error
    """
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = pod_psm_body([{"foo": "bar"}])
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_missing_merge_key_current():
    """Test that an element in a current list that is missing the merge key
    causes an error
    """
    obj = sample_pod([{"image": "foo"}])
    patch = pod_psm_body([{"name": "foo", "foo": "bar"}])
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_primitive_delete_not_there():
    """Test that a request to delete a primitive from a list causes an error if
    the element is not present
    """
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = {"$deleteFromPrimitiveList/keyNotThere": [2, 4]}
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_primitive_delete_patch_not_list():
    """Test that a request to delete a primitive from a list where the patch
    value is not a list causes an error
    """
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = {"$deleteFromPrimitiveList/key": 2}
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_primitive_delete_current_not_list():
    """Test that a request to delete a primitive from a list where the current
    value is not a list causes an error
    """
    obj = sample_foo({"key": {"a": 1, "b": 2}})
    patch = {"$deleteFromPrimitiveList/key": ["a"]}
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_primitive_delete_element_not_found():
    """Test that a request to delete a primitive from a list which doesn't
    contain the element raises an error
    """
    obj = sample_foo({"key": [1, 2, 3, 4]})
    patch = {"$deleteFromPrimitiveList/key": [5]}
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)


def test_delete_element_not_found():
    """Test that a `$patch: delete` directive to delete an element from a list
    that isn't found raises an error
    """
    obj = sample_pod([{"name": "foo", "image": "foo"}])
    patch = pod_psm_body([{"name": "bar", "$patch": "delete"}])
    with pytest.raises(ValueError):
        patch_strategic_merge(obj, patch)
