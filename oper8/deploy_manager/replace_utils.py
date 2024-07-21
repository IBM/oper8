"""This file contains common utilities for detecting if a replace operation is required
for a resource
"""
# Standard
from typing import Any, Callable, List

# First Party
import alog

log = alog.use_channel("DMRPLC_UTILS")


def modified_lists(
    current_manifest: dict, desired_manifest: dict, in_list: bool = False
) -> bool:
    """Helper function to check if there are any differences in the lists of the desired manifest.
    This is required because Kubernetes combines lists which is often not the desired use
    """
    if isinstance(current_manifest, list) and isinstance(desired_manifest, list):
        # if the desired has less then the current then return True. Removing
        # from a list requires a put
        if len(current_manifest) > len(desired_manifest):
            log.debug4("Requires replace due to list deletion")
            return True
        # Iterate over the desired manifest
        for i in range(min(len(current_manifest), len(desired_manifest))):
            if modified_lists(current_manifest[i], desired_manifest[i], in_list=True):
                return True
    if isinstance(current_manifest, dict) and isinstance(desired_manifest, dict):
        key_intersection = set(current_manifest.keys()).intersection(
            set(desired_manifest.keys())
        )
        # If there are no common keys and we're in a list then return True
        # this means we have a new object
        if in_list and not key_intersection:
            log.debug4("Requires replace due to no common key in list")
            return True

        # Tack if one key has the same value. This is
        # required for kubernetes merges
        at_least_one_common = False
        for k in key_intersection:
            if current_manifest[k] == desired_manifest[k]:
                at_least_one_common = True
            if modified_lists(current_manifest[k], desired_manifest[k]):
                return True
        if in_list and not at_least_one_common:
            log.debug4("Requires replace due to no common key/value in list")
            return True
    return False


def modified_value_from(current_manifest: Any, desired_manifest: Any) -> bool:
    """Helper function to check if a manifest switched from value to valueFrom. These are mutually
    exclusive thus they require a replace command.
    """
    if isinstance(current_manifest, list) and isinstance(desired_manifest, list):
        iteration_len = min(len(current_manifest), len(desired_manifest))
        for i in range(iteration_len):
            if modified_value_from(current_manifest[i], desired_manifest[i]):
                return True
    if isinstance(current_manifest, dict) and isinstance(desired_manifest, dict):
        if ("value" in current_manifest and "valueFrom" in desired_manifest) or (
            "valueFrom" in current_manifest and "value" in desired_manifest
        ):
            log.debug4("Requires replace due to value/valueFrom change")
            return True
        else:
            for k in set(current_manifest.keys()).intersection(
                set(desired_manifest.keys())
            ):
                if modified_value_from(current_manifest[k], desired_manifest[k]):
                    return True
    return False


REPLACE_FUNCS: List[Callable[[str, str], bool]] = [modified_lists, modified_value_from]


def requires_replace(current_manifest: dict, desired_manifest: dict) -> bool:
    """Function to determine if a resource requires a replace operation instead
    of apply. This can occur due to list merging, or updating envVars

    Args:
        current_manifest (dict): The current manifest in the cluster
        desired_manifest (dict): The desired manifest that should be applied

    Returns:
        bool: If the current manifest requires a replace operation
    """
    for func in REPLACE_FUNCS:
        if func(current_manifest, desired_manifest):
            log.debug4("Manifest requires replace", desired_manifest)
            return True
    return False
