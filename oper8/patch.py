"""
This module holds shared semantics for patching resources using temporary_patch
"""

# Standard
from typing import List, Optional, Tuple
import copy

# Third Party
from jsonpatch import JsonPatch

# First Party
import alog

# Local
from .exceptions import ConfigError
from .patch_strategic_merge import patch_strategic_merge

log = alog.use_channel("PATCH")

## Public Interface ############################################################

STRATEGIC_MERGE_PATCH = "patchStrategicMerge"
JSON_PATCH_6902 = "patchJson6902"

# JSON pointers to the fields that define the identity of a resource. Patches
# are not allowed to modify these since doing so would allow a patch to
# redirect the operator to write a different resource (e.g. in a different
# namespace) than the one that was rendered.
PROTECTED_IDENTITY_FIELDS = (
    "/apiVersion",
    "/kind",
    "/metadata/name",
    "/metadata/namespace",
)


def apply_patches(
    internal_name: str,
    resource_definition: dict,
    temporary_patches: List[dict],
):
    """Apply all temporary patches to the given resource from the given list.
    The patches are applied in-place.

    Args:
        internal_name:  str
            The name given to the internal node of the object. This is used to
            identify which patches apply to this object.
        resource_definition:  dict
            The dict representation of the object to patch
        temporary_patches:  List[dict]
            The list of temporary patches that apply to this rollout

    Returns:
        patched_definition:  dict
            The dict representation of the object with patches applied
    """
    log.debug2(
        "Looking for patches for %s/%s (%s)",
        resource_definition.get("kind"),
        resource_definition.get("metadata", {}).get("name"),
        internal_name,
    )
    resource_definition = copy.deepcopy(resource_definition)
    identity = _get_identity(resource_definition)
    for patch_content in temporary_patches:
        log.debug4("Checking patch: << %s >>", patch_content)

        # Look to see if this patch contains a match for the internal name
        internal_name_parts = internal_name.split(".")
        internal_name_parts.reverse()
        patch = patch_content.spec.patch
        log.debug4("Full patch section: %s", patch)
        while internal_name_parts and isinstance(patch, dict):
            patch_level = internal_name_parts.pop()
            log.debug4("Getting patch level [%s]", patch_level)
            patch = patch.get(patch_level, {})
            log.debug4("Patch level: %s", patch)
        log.debug4("Checking patch: %s", patch)

        # If the patch matches, apply the right merge
        if patch and not internal_name_parts:
            log.debug3("Found matching patch: %s", patch_content.metadata.name)

            # Dispatch the right patch type
            if patch_content.spec.patchType == STRATEGIC_MERGE_PATCH:
                resource_definition = _apply_patch_strategic_merge(
                    resource_definition, patch
                )
            elif patch_content.spec.patchType == JSON_PATCH_6902:
                resource_definition = _apply_json_patch(resource_definition, patch)
            else:
                raise ValueError(
                    f"Unsupported patch type [{patch_content.spec.patchType}]"
                )

            # Make sure the patch did not modify the identity of the resource
            _verify_identity(
                identity,
                resource_definition,
                internal_name,
                patch_content.get("metadata", {}).get("name"),
            )
    return resource_definition


## Identity Protection #########################################################


def _get_identity(resource_definition: dict) -> Tuple[Optional[str], ...]:
    """Get the values of the protected identity fields for the given resource
    in the same order as PROTECTED_IDENTITY_FIELDS
    """
    metadata = resource_definition.get("metadata") or {}
    return (
        resource_definition.get("apiVersion"),
        resource_definition.get("kind"),
        metadata.get("name"),
        metadata.get("namespace"),
    )


def _verify_identity(
    identity: Tuple[Optional[str], ...],
    resource_definition: dict,
    internal_name: str,
    patch_name: Optional[str],
):
    """Verify that the identity of the patched resource matches the identity
    of the resource before patching

    Args:
        identity:  Tuple[Optional[str], ...]
            The identity of the resource before patching
        resource_definition:  dict
            The dict representation of the patched resource
        internal_name:  str
            The internal name of the resource being patched
        patch_name:  Optional[str]
            The name of the patch that was applied

    Raises:
        ConfigError: If any of the identity fields were changed by the patch
    """
    patched_identity = _get_identity(resource_definition)
    changed_fields = [
        field
        for field, before, after in zip(
            PROTECTED_IDENTITY_FIELDS, identity, patched_identity
        )
        if before != after
    ]
    if changed_fields:
        msg = (
            f"Temporary patch [{patch_name}] for [{internal_name}] attempted to "
            f"modify protected identity fields {changed_fields}"
        )
        log.error(msg)
        raise ConfigError(msg)


def _is_protected_pointer(pointer: str) -> bool:
    """Determine whether the given JSON pointer refers to a protected identity
    field, an ancestor of one (including the document root), or a descendant
    of one
    """
    return any(
        pointer == protected
        or protected.startswith(f"{pointer}/")
        or pointer.startswith(f"{protected}/")
        for protected in PROTECTED_IDENTITY_FIELDS
    )


## JSON Patch 6902 #############################################################


def _apply_json_patch(
    resource_definition: dict,
    patch: dict,
) -> dict:
    """Apply a Json Patch based on JSON Patch (rfc 6902)"""

    if not isinstance(patch, list):
        raise ValueError("Invalid JSON 6902 patch. Must be a list of operations.")
    _validate_json_patch(patch)
    return JsonPatch(patch).apply(resource_definition)


def _validate_json_patch(patch: list):
    """Validate that none of the operations in the JSON patch modify any of
    the protected identity fields. The `test` operation is read-only and is
    therefore allowed on any path. The `from` pointer is only checked for
    `move` operations since `copy` does not modify its source.
    """
    for operation in patch:
        if not isinstance(operation, dict):
            raise ValueError(
                f"Invalid JSON 6902 patch operation. Must be a dict: {operation}"
            )
        op = operation.get("op")
        if op == "test":
            continue
        pointers = [operation.get("path")]
        if op == "move":
            pointers.append(operation.get("from"))
        for pointer in pointers:
            if not isinstance(pointer, str):
                raise ValueError(
                    f"Invalid JSON 6902 patch operation. Pointers must be strings: {operation}"
                )
            if _is_protected_pointer(pointer):
                msg = (
                    "JSON 6902 patch operations may not modify protected identity "
                    f"fields: {operation}"
                )
                log.error(msg)
                raise ConfigError(msg)


## Strategic Merge Patch #######################################################


def _apply_patch_strategic_merge(
    resource_definition: dict,
    patch: dict,
) -> dict:
    """Apply a Strategic Merge Patch based on JSON Merge Patch (rfc 7386)"""
    return patch_strategic_merge(resource_definition, patch)
