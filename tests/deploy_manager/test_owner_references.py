"""
Tests for the update_owner_references functionality
"""

# Standard
import copy

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.deploy_manager.owner_references import (
    _make_owner_reference,
    update_owner_references,
)
from oper8.exceptions import ClusterError
from oper8.test_helpers.helpers import (
    SOME_OTHER_NAMESPACE,
    TEST_NAMESPACE,
    MockDeployManager,
)

## Helpers #####################################################################

log = alog.use_channel("TEST")

SAMPLE_OWNER = {
    "kind": "Owner",
    "apiVersion": "foo.bar.com/v1",
    "metadata": {
        "name": "owner",
        "namespace": TEST_NAMESPACE,
        "uid": "12345",
    },
}


def sample_object(namespace=TEST_NAMESPACE):
    return {
        "kind": "Child",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {
            "name": "child",
            "namespace": namespace,
            "uid": "54321",
        },
    }


## Happy Path ##################################################################


def test_add_new_owner_ref():
    """Test that adding a ref to an object with none present adds as expected"""
    dm = MockDeployManager()
    obj = sample_object()
    update_owner_references(dm, SAMPLE_OWNER, obj)
    assert "ownerReferences" in obj["metadata"]
    assert obj["metadata"]["ownerReferences"] == [_make_owner_reference(SAMPLE_OWNER)]


def test_do_not_add_new_owner_ref_if_namespace_mismatch():
    """Test that a ref to an object with none present is only added if owner and object are in same namespace"""
    dm = MockDeployManager()
    obj = sample_object(namespace=SOME_OTHER_NAMESPACE)
    update_owner_references(dm, SAMPLE_OWNER, obj)
    assert "ownerReferences" in obj["metadata"]
    assert obj["metadata"]["ownerReferences"] == []


def test_no_duplicate():
    """Test that an object with an existing ref for the owner does not
    duplicate the existing ref
    """
    dm = MockDeployManager()
    obj = sample_object()
    obj["metadata"]["ownerReferences"] = [_make_owner_reference(SAMPLE_OWNER)]
    update_owner_references(dm, SAMPLE_OWNER, obj)
    assert "ownerReferences" in obj["metadata"]
    assert obj["metadata"]["ownerReferences"] == [_make_owner_reference(SAMPLE_OWNER)]


def test_external_preserved():
    """Test that an object with an existing ref for a different owner adds the
    new reference without removing the old one
    """
    external_owner = {
        "kind": "Owner",
        "apiVersion": "foo.bar.com/v1",
        "metadata": {
            "name": "other-owner",
            "namespace": "test",
            "uid": "67890",
        },
    }
    external_ref = _make_owner_reference(external_owner)
    obj = sample_object()
    cluster_content_obj = copy.deepcopy(obj)
    cluster_content_obj["metadata"]["ownerReferences"] = [external_ref]
    dm = MockDeployManager(resources=[cluster_content_obj])
    update_owner_references(dm, SAMPLE_OWNER, obj)
    assert "ownerReferences" in obj["metadata"]
    assert obj["metadata"]["ownerReferences"] == [
        external_ref,
        _make_owner_reference(SAMPLE_OWNER),
    ]


## Happy Path ##################################################################


def test_owner_missing_keys():
    """Test that when the owner is missing required keys, an assertion is hit"""
    dm = MockDeployManager()
    for key in ["kind", "apiVersion", "metadata.name", "metadata.namespace"]:
        log.debug("Trying without [%s]", key)
        bad_owner = copy.deepcopy(SAMPLE_OWNER)
        parts = key.split(".")
        dct = bad_owner
        for part in parts[:-1]:
            dct = dct[part]
        del dct[parts[-1]]
        with pytest.raises(AssertionError):
            update_owner_references(dm, bad_owner, sample_object())


def test_child_missing_keys():
    """Test that when the child is missing required keys, an assertion is hit"""
    dm = MockDeployManager()
    for key in ["kind", "apiVersion", "metadata.name", "metadata.namespace"]:
        log.debug("Trying without [%s]", key)
        bad_child = sample_object()
        parts = key.split(".")
        dct = bad_child
        for part in parts[:-1]:
            dct = dct[part]
        del dct[parts[-1]]
        with pytest.raises(AssertionError):
            update_owner_references(dm, SAMPLE_OWNER, bad_child)


def test_lookup_cluster_error():
    """Test that when the deploy manager fails to look up the object, a
    ClusterError is raised
    """
    dm = MockDeployManager(get_state_fail=True)
    obj = sample_object()
    with pytest.raises(ClusterError):
        update_owner_references(dm, SAMPLE_OWNER, obj)


## _make_owner_reference #######################################################


def test_make_owner_reference_required_keys():
    """Make sure the shape of the owner reference looks right"""
    ref = _make_owner_reference(SAMPLE_OWNER)
    for key, source in {
        "apiVersion": "apiVersion",
        "kind": "kind",
        "name": "metadata.name",
        "uid": "metadata.uid",
    }.items():
        parts = source.split(".")
        dct = SAMPLE_OWNER
        for part in parts[:-1]:
            dct = dct[part]
        exp_val = dct[parts[-1]]
        assert key in ref
        assert ref[key] == exp_val


def test_make_owner_block_owner_deletion():
    """Make sure that blockOwnerDeletion is set"""
    ref = _make_owner_reference(SAMPLE_OWNER)
    assert "blockOwnerDeletion" in ref
    assert ref["blockOwnerDeletion"]


def test_make_owner_not_controller():
    """Make sure that controller is not set"""
    ref = _make_owner_reference(SAMPLE_OWNER)
    assert "controller" not in ref
