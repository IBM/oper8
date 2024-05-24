"""
This library holds common verification routines for individual kubernetes
resources.
"""

# Standard
from datetime import datetime
from functools import partial
from typing import Callable, List, Optional

# Third Party
import dateutil.parser

# First Party
import alog

# Local
from . import status
from .session import _SESSION_NAMESPACE  # pylint: disable=cyclic-import

## Globals #####################################################################

log = alog.use_channel("VERFY")

DEFAULT_TIMESTAMP_KEY = "lastTransitionTime"
AVAILABLE_CONDITION_KEY = "Available"
COMPLETE_CONDITION_KEY = "Complete"
PROGRESSING_CONDITION_KEY = "Progressing"
NEW_RS_AVAILABLE_REASON = "NewReplicaSetAvailable"

# Type definition for the signature of a resource verify function
# NOTE: I'm not sure why pylint dislikes this name. In my view, this is a shared
#   global which should have all-caps casing.
RESOURCE_VERIFY_FUNCTION = Callable[[dict], bool]  # pylint: disable=invalid-name


## Main Functions ##############################################################


def verify_resource(
    kind: str,
    name: str,
    api_version: str,
    session,
    *,
    # Use a predfined _SESSION_NAMESPACE default instead of None to differentiate between
    # nonnamespaced resources (which pass None) and those that use session.namespace
    namespace: Optional[str] = _SESSION_NAMESPACE,
    verify_function: Optional[RESOURCE_VERIFY_FUNCTION] = None,
    is_subsystem: bool = False,
    condition_type: Optional[str] = None,
    timestamp_key: Optional[str] = None,
) -> bool:
    """Verify a resource detailed in a ManagedObject.

    NOTE: we can't do type-hinting on the session because importing
        DeploySession creates a circular dependency with Component. We should
        probably fix that...

    This function will run the appropriate verification function for the given
    resource kind.

    Args:
        kind:  str
            The kind of the resource to look for
        name:  str
            The name of the resource to look for
        api_version:  str
            The api_version of the resource to look for
        session:  DeploySession
            The current deployment session

    Kwargs:
        is_subsystem:  bool
            Whether or not the given kind is an oper8 subsystem
        condition_type:  str
            For non-stanard types, this is the type name for the condition to
            check for verification
        timestamp_key:  str
            For non-standard types, this is the key in the condition to use to
            sort by date

    Returns:
        success:  bool
            True on successful deployment verification, False on failure
            conditions
    """

    # Configure namespace if it isn't set
    namespace = namespace if namespace != _SESSION_NAMESPACE else session.namespace

    # Get the state of the object
    log.debug2("Fetching current content for [%s/%s] to verify it", kind, name)
    success, content = session.get_object_current_state(
        kind=kind, name=name, api_version=api_version, namespace=namespace
    )
    assert success, f"Failed to fetch state of [{kind}/{name}]"

    # If the object is not found, it is not verified
    if not content:
        log.debug("Could not find [%s/%s]. Not Ready.", kind, name)
        return False

    # If a custom condition_type is given, then use the general condition
    # verifier
    if condition_type is not None:
        log.debug(
            "Using custom verification for [%s/%s] with condition [%s]",
            kind,
            name,
            condition_type,
        )
        return _verify_condition(
            content, condition_type, True, timestamp_key or DEFAULT_TIMESTAMP_KEY
        )

    # Run the appropriate verification function if there is one available
    verify_fn = verify_function or _resource_verifiers.get(kind)
    if not verify_fn and is_subsystem:
        log.debug("Using oper8 subsystem verifier for [%s/%s]", kind, name)
        verify_fn = partial(
            verify_subsystem,
            desired_version=session.version,
        )

    # If a verifier was found, run it
    if verify_fn:
        log.debug2("Running [%s] verifier for [%s/%s]", kind, kind, name)
        return verify_fn(content)

    # If no other verifier found, we consider it verified as long as it is
    # present in the cluster
    log.debug2("No kind-specific verifier for [%s/%s]", kind, name)
    return True


## Individual Resources ########################################################


def verify_pod(object_state: dict) -> bool:
    """Verify that a pod resources is ready"""
    return _verify_condition(object_state, "Ready", True)


def verify_job(object_state: dict) -> bool:
    """Verify that a job has completed successfully"""
    # https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/job-v1/#JobStatus
    return _verify_condition(object_state, COMPLETE_CONDITION_KEY, True)


def verify_deployment(object_state: dict) -> bool:
    """Verify that all members of a deployment are ready
    and all members are rolled out to new version in case of update.
    """
    return _verify_condition(
        object_state, AVAILABLE_CONDITION_KEY, True
    ) and _verify_condition(
        object_state,
        PROGRESSING_CONDITION_KEY,
        True,
        expected_reason=NEW_RS_AVAILABLE_REASON,
    )


def verify_statefulset(object_state: dict) -> bool:
    """Verify that all desired replicas of a StatefulSet are ready"""
    obj_status = object_state.get("status", {})
    expected_replicas = obj_status.get("replicas")
    if expected_replicas is None:
        log.debug2("No replicas found in statefulset status. Not ready.")
        return False
    ready_replicas = obj_status.get("readyReplicas", 0)
    return ready_replicas == expected_replicas


def verify_subsystem(object_state: dict, desired_version: str = None) -> bool:
    """Verify that an oper8-managed subsystem is ready"""

    current_version = status.get_version(object_state.get("status", {}))
    # Once rollout finishes with verification, version status is added.
    #   Until then, mark the subsystem as unverified.
    if desired_version and not current_version:
        log.debug2(
            "Reconciled version %s does not match desired: %s",
            current_version,
            desired_version,
        )
        return False

    return (
        _verify_condition(
            object_state, status.READY_CONDITION, True, status.TIMESTAMP_KEY
        )
        and _verify_condition(
            object_state, status.UPDATING_CONDITION, False, status.TIMESTAMP_KEY
        )
        and current_version == desired_version
    )


_resource_verifiers = {
    "Pod": verify_pod,
    "Job": verify_job,
    "Deployment": verify_deployment,
    "StatefulSet": verify_statefulset,
}

## Helpers #####################################################################


def _verify_condition(
    object_state: dict,
    type_val: str,
    expected_status: bool,
    timestamp_key: str = DEFAULT_TIMESTAMP_KEY,
    expected_reason: Optional[str] = None,
) -> bool:
    """Perform the guts of checking for an expected condition has expected status
    and reason
    """

    # Look for the condition of the given type
    conditions = _get_conditions(object_state, type_val)
    log.debug2("Found %d [%s] conditions", len(conditions), type_val)

    # If no conditions, the resource is not verified
    if not conditions:
        log.debug2("No %s conditions. Not verified", type_val)
        return False

    # Sort the conditions by transaction time
    latest_cond = _sort_conditions_by_date(conditions, timestamp_key)[0]
    log.debug3("Latest '%s' condition: %s", type_val, latest_cond)
    return _check_condition(latest_cond, expected_status, expected_reason)


def _get_conditions(object_state: dict, type_val: str) -> List[dict]:
    """Get the list of conditions from an object state"""
    return [
        cond
        for cond in object_state.get("status", {}).get("conditions", [])
        if cond.get("type") == type_val
    ]


def _parse_condition_timestamp(condition: dict, timestamp_key: str) -> dict:
    """Parse the timestamp in a condition in place"""
    timestamp = condition.get(timestamp_key)
    log.debug3("Timestamp [%s]: %s", timestamp_key, timestamp)
    if isinstance(timestamp, str):
        return dateutil.parser.parse(timestamp)
    if isinstance(timestamp, datetime):
        return timestamp
    log.warning("Found condition with no valid timestamp. Using epoch")
    return datetime.fromtimestamp(0)


def _sort_conditions_by_date(conditions: List[dict], timestamp_key: str) -> List[dict]:
    """Helper to parse datestamps and sort a list of conditions. The sort will
    put newest conditions first.
    """

    return sorted(
        conditions,
        key=lambda x: _parse_condition_timestamp(x, timestamp_key),
        reverse=True,
    )


def _check_condition(
    condition: dict, expected_status: bool, expected_reason: Optional[str] = None
) -> bool:
    """Helper to parse a condition object and check if it has expected values."""

    def is_expected_status() -> bool:
        """Helper to parse the various ways a 'status' may be represented in a
        condition
        """
        obj_status = condition.get("status")
        if not obj_status:
            return False
        if isinstance(obj_status, str):
            return obj_status.lower() == str(expected_status).lower()
        return bool(obj_status) == expected_status

    def is_expected_reason() -> bool:
        if expected_reason is None:
            return True
        obj_reason = condition.get("reason")
        return obj_reason == expected_reason

    return is_expected_status() and is_expected_reason()
