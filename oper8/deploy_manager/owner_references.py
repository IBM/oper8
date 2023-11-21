"""
This module holds common functionality that the DeployManager implementations
can use to manage ownerReferences on deployed resources
"""

# First Party
import alog

# Local
from ..exceptions import assert_cluster
from .base import DeployManagerBase

log = alog.use_channel("OWNRF")


def update_owner_references(
    deploy_manager: DeployManagerBase,
    owner_cr: dict,
    child_obj: dict,
):
    """Fetch current ownerReferences and merge a reference for this CR into
    the child object
    """

    # Validate the shape of the owner CR and the chid object
    _validate_object_struct(owner_cr)
    _validate_object_struct(child_obj)

    # Fetch the current state of this object
    kind = child_obj["kind"]
    api_version = child_obj["apiVersion"]
    name = child_obj["metadata"]["name"]
    namespace = child_obj["metadata"]["namespace"]
    uid = child_obj["metadata"].get("uid")

    success, content = deploy_manager.get_object_current_state(
        kind=kind, name=name, api_version=api_version, namespace=namespace
    )
    assert_cluster(
        success, f"Failed to fetch current state of {api_version}.{kind}/{name}"
    )

    # Get the current ownerReferences
    owner_refs = []
    if content is not None:
        owner_refs = content.get("metadata", {}).get("ownerReferences", [])
        log.debug3("Current owner refs: %s", owner_refs)

    # If the current CR is not represented and current CR is in the same
    # namespace as the child object, add it
    current_uid = owner_cr["metadata"]["uid"]
    log.debug3("Current CR UID: %s", current_uid)
    current_namespace = owner_cr["metadata"]["namespace"]
    log.debug3("Current CR namespace: %s", current_namespace)

    if current_uid == uid:
        log.debug2("Owner is same as child; Not adding owner ref")
        return

    if (namespace == current_namespace) and (
        current_uid not in [ref["uid"] for ref in owner_refs]
    ):
        log.debug2(
            "Adding current CR owner reference for %s.%s/%s",
            api_version,
            kind,
            name,
        )
        owner_refs.append(_make_owner_reference(owner_cr))

    # Add the ownerReferences to the object that will be applied to the
    # cluster
    log.debug4("Final owner refs: %s", owner_refs)
    child_obj["metadata"]["ownerReferences"] = owner_refs


## Implementation Details ######################################################


def _validate_object_struct(obj: dict):
    """Ensure that the required portions of an object are present (kind,
    apiVerison, metadata.namespace, metadata.name)
    """
    assert "kind" in obj, "Got object without 'kind'"
    assert "apiVersion" in obj, "Got object without 'apiVersion'"
    metadata = obj.get("metadata")
    assert isinstance(metadata, dict), "Got object with non-dict 'metadata'"
    assert "name" in metadata, "Got object without 'metadata.name'"
    assert "namespace" in metadata, "Got object without 'metadata.namespace'"


def _make_owner_reference(owner_cr: dict) -> dict:
    """Make an owner reference for the given CR instance

    Error Semantics: This function makes a best-effort and does not validate the
    content of the owner_cr, so the resulting ownerReference may contain None
    entries.

    Args:
        owner_cr:  dict
            The full CR manifest for the owning resource

    Returns:
        owner_reference:  dict
            The dict entry for the `metadata.ownerReferences` entry of the owned
            object
    """
    # NOTE: We explicitly don't set controller: True here. If two
    #   oper8-managed resources reference the resource, only one can have
    #   controller set to True. According to StackOverflow, this field is
    #   only used for adoption and not garbage collection.
    # CITE: https://stackoverflow.com/a/65825463
    metadata = owner_cr.get("metadata", {})
    return {
        "apiVersion": owner_cr.get("apiVersion"),
        "kind": owner_cr.get("kind"),
        "name": metadata.get("name"),
        "uid": metadata.get("uid"),
        # The parent will not be deleted until this object completes its
        # deletion
        "blockOwnerDeletion": True,
    }
