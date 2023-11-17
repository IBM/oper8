"""
This module implements Patch Strategic Merge following the semantics in:

* kustomize: https://kubectl.docs.kubernetes.io/references/kustomize/glossary/#patchstrategicmerge
* kubernetes: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md
"""  # pylint: disable=line-too-long


# Standard
from collections import OrderedDict
from typing import Dict
import copy

# Third Party
from openshift.dynamic.apply import STRATEGIC_MERGE_PATCH_KEYS

# First Party
import alog

log = alog.use_channel("PATCH")

## Public ######################################################################


def patch_strategic_merge(
    resource_definition: dict,
    patch: dict,
    merge_patch_keys: Dict[str, str] = None,
) -> dict:
    """Apply a Strategic Merge Patch based on JSON Merge Patch (rfc 7386)

    Args:
        resource_definition:  dict
            The dict representation of the kubernetes resource
        patch:  dict
            The formatted patch to apply
        merge_patch_keys:  Dict[str, str]
            The mapping from paths to merge keys used to perform merge semantics
            for list elements

    Returns:
        patched_resource_definition:  dict
            The patched version of the resource_definition
    """
    if merge_patch_keys is None:
        merge_patch_keys = STRATEGIC_MERGE_PATCH_KEYS
    return _strategic_merge(
        current=copy.deepcopy(resource_definition),
        desired=copy.deepcopy(patch),
        position=resource_definition.get("kind"),
        merge_patch_keys=merge_patch_keys,
    )


## Implementation ##############################################################


_DIRECTIVE_KEY = "$patch"
_DIRECTIVE_REPLACE = "replace"
_DIRECTIVE_MERGE = "merge"
_DIRECTIVE_DELETE = "delete"
_DIRECTIVE_DELETE_FROM_PRIMITIVE_LIST = "$deleteFromPrimitiveList/"


def _strategic_merge(  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    current: dict,
    desired: dict,
    position: str,
    merge_patch_keys: Dict[str, str],
) -> dict:
    """Recursive implementation of Patch Strategic Merge."""

    # If they are dicts, recurse on keys
    if isinstance(desired, dict) and isinstance(current, dict):
        log.debug4("Performing dict merge at [%s]", position)
        log.debug4(current)
        log.debug4(desired)
        for key, val in desired.items():
            # Support deletion
            if val is None:
                current.pop(key, None)

            # Check for the special directive to remove from a primitive list
            elif key.startswith(_DIRECTIVE_DELETE_FROM_PRIMITIVE_LIST):
                target_key = key.split("/", 1)[-1]
                if target_key not in current:
                    raise ValueError(
                        f"Cannot delete from unknown primitive list [{target_key}]"
                    )
                target_val = current[target_key]
                if not isinstance(val, list):
                    raise ValueError(
                        "Bad primitive list delete directive. Patch must be a list."
                    )
                if not isinstance(target_val, list):
                    raise ValueError(
                        "Bad primitive list delete directive. Target must be a list."
                    )
                for element in val:
                    try:
                        target_val.remove(element)
                    except ValueError as err:
                        raise ValueError(
                            "Bad primitive list delete directive. Element not found."
                        ) from err

            # Add new keys
            elif key not in current:
                current[key] = val

            # Recurse
            else:
                next_position = ".".join([position, key])
                log.debug4("Recursing to [%s]", next_position)
                current[key] = _strategic_merge(
                    current[key],
                    val,
                    next_position,
                    merge_patch_keys,
                )

        return current

    # If they are lists, apply the strategic merge
    if isinstance(desired, list) and isinstance(current, list):
        merge_key = merge_patch_keys.get(position)
        log.debug4("Performing list merge at [%s]. Merge key: %s", position, merge_key)

        # If no merge key given for this path, do an overwrite merge
        if not merge_key:
            return desired

        # Otherwise, align elements by their merge keys and recursively merge
        # each one

        # Make sure these are lists of dicts
        if not all(isinstance(itm, dict) and merge_key in itm for itm in current):
            raise ValueError(
                f"Current at [{position}] contains elements without [{merge_key}]"
            )
        if not all(isinstance(itm, dict) and merge_key in itm for itm in desired):
            raise ValueError(
                f"Desired at [{position}] contains elements without [{merge_key}]"
            )

        # Create dicts based on the merge key
        current_dict = OrderedDict([(itm[merge_key], itm) for itm in current])
        desired_dict = OrderedDict([(itm[merge_key], itm) for itm in desired])

        # Perform the merge on each item
        for item_key, item in desired_dict.items():
            # Get the directive (default to merge)
            directive = item.pop(_DIRECTIVE_KEY, _DIRECTIVE_MERGE)
            log.debug4("Element [%s] directive: %s", item_key, directive)

            # If "delete" remove from the current
            if directive == _DIRECTIVE_DELETE:
                log.debug4("Doing delete")
                if item_key not in current_dict:
                    raise ValueError(
                        f"Invalid [{_DIRECTIVE_DELETE}] on missing element [{item_key}]"
                    )
                del current_dict[item_key]

            # If "replace" just replace in the current
            elif directive == _DIRECTIVE_REPLACE or item_key not in current_dict:
                log.debug4("Doing replace")
                current_dict[item_key] = item

            # If "merge" recurse
            elif directive == _DIRECTIVE_MERGE:
                # NOTE: list nesting is not represented in the position for
                #   merge keys
                current_dict[item_key] = _strategic_merge(
                    current_dict[item_key],
                    item,
                    position,
                    merge_patch_keys,
                )

            # Otherwise, it's an error
            else:
                raise ValueError(f"Invalid directive: [{directive}]")

        # Return the ordered list of updated elements
        return list(current_dict.values())

    # If not one of the special types, overwrite
    log.debug4("Performing overwrite")
    return desired
