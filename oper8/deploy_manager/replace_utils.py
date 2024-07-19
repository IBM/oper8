"""This file contains common utilities for detecting if a replace operation is required
for a resource
"""
# Standard
from typing import Any, Callable, List

# Third Party
from openshift.dynamic.apply import recursive_list_diff

# First Party
import alog

log = alog.use_channel("DMRPLC_UTILS")


def modified_lists(current_manifest: dict, desired_manifest: dict) -> bool:
    """Helper function to check if there are any differences in the lists of the desired manifest.
    This is required because Kubernetes combines lists which is often not the desired use
    """
    for k in set(current_manifest.keys()).intersection(set(desired_manifest.keys())):
        if isinstance(current_manifest[k], list) and isinstance(
            desired_manifest[k], list
        ):
            if bool(recursive_list_diff(current_manifest[k], desired_manifest[k])):
                return True
        elif isinstance(current_manifest[k], dict) and isinstance(  # noqa: SIM102
            desired_manifest[k], dict
        ):
            if modified_lists(current_manifest[k], desired_manifest[k]):
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
