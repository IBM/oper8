"""
This module holds shared semantics for patching resources using temporary_patch
"""

# Standard
from typing import List
import copy

# Third Party
from jsonpatch import JsonPatch

# First Party
import alog

# Local
from .patch_strategic_merge import patch_strategic_merge

log = alog.use_channel("PATCH")

## Public Interface ############################################################

STRATEGIC_MERGE_PATCH = "patchStrategicMerge"
JSON_PATCH_6902 = "patchJson6902"


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
    return resource_definition


## JSON Patch 6902 #############################################################


def _apply_json_patch(
    resource_definition: dict,
    patch: dict,
) -> dict:
    """Apply a Json Patch based on JSON Patch (rfc 6902)"""

    if not isinstance(patch, list):
        raise ValueError("Invalid JSON 6902 patch. Must be a list of operations.")
    return JsonPatch(patch).apply(resource_definition)


## Strategic Merge Patch #######################################################


def _apply_patch_strategic_merge(
    resource_definition: dict,
    patch: dict,
) -> dict:
    """Apply a Strategic Merge Patch based on JSON Merge Patch (rfc 7386)"""
    return patch_strategic_merge(resource_definition, patch)
