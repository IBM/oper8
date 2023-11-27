"""
This module holds the common functionality used to represent the status of
resources managed by oper8

Oper8 supports the following orthogonal status conditions:

* Ready: True if the service is able to take traffic
* Updating: True if a modification is being actively applied to the application

Additionally, oper8 supports a top-level status element to report the detailed
status of the managed components. The schema is:
{
    "componentStatus": {
        "allComponents": [list of all node names],
        "deployedComponents": [list of nodes that have successfully deployed],
        "verifiedComponents": [list of nodes that have successfully verified],
        "failedComponents": [list of nodes that have successfully verified],
        "deployed": "N/M",
        "verified": "N/M",
    }
}
"""

# Standard
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union
import copy

# Third Party
from deepdiff import DeepDiff

# First Party
import alog

# Local
from . import config
from .dag import CompletionState
from .utils import nested_get, nested_set

log = alog.use_channel("STTUS")

## Public ######################################################################

# The "type" values in the condition
READY_CONDITION = "Ready"
UPDATING_CONDITION = "Updating"

# The key in the condition used for the timestamp
TIMESTAMP_KEY = "lastTransactionTime"

# The keys for component status information
COMPONENT_STATUS = "componentStatus"
COMPONENT_STATUS_ALL_NODES = "allComponents"
COMPONENT_STATUS_DEPLOYED_NODES = "deployedComponents"
COMPONENT_STATUS_UNVERIFIED_NODES = "unverifiedComponents"
COMPONENT_STATUS_FAILED_NODES = "failedComponents"
COMPONENT_STATUS_DEPLOYED_COUNT = "deployed"
COMPONENT_STATUS_VERIFIED_COUNT = "verified"

# The fields in status that hold the version information
# NOTE: These intentionally match IBM CloudPak naming conventions
VERSIONS_FIELD_CURRENT_VERSION = "versions.reconciled"
VERSIONS_FIELD_AVAILABLE_VERSIONS = "versions.available.versions"
# The field in status that holds the operator version information
OPERATOR_VERSION = "operatorVersion"


class ReadyReason(Enum):
    """Nested class to hold reason constants for the Ready condition"""

    # The application is stable and ready for traffic
    STABLE = "Stable"

    # The application is rolling out for the first time
    INITIALIZING = "Initializing"

    # The application rollout is in progress and will continue
    # the next reconcile
    IN_PROGRESS = "InProgress"

    # The application has hit an unrecoverable config error during rollout
    CONFIG_ERROR = "ConfigError"

    # The application has hit an unrecoverable error during rollout
    ERRORED = "Errored"


class UpdatingReason(Enum):
    """Nested class to hold reason constants for the Updating condition"""

    # There are no updates to apply to the application
    STABLE = "Stable"

    # A required precondition was not met
    PRECONDITION_WAIT = "PreconditionWait"

    # A required deployment verification condition was not met
    VERIFY_WAIT = "VerifyWait"

    # The application attempted to perform an operation against the cluster that
    # failed unexpectedly
    CLUSTER_ERROR = "ClusterError"

    # An error occurred, so the application is not attempting to update
    ERRORED = "Errored"

    # Version upgrade is initiated
    VERSION_CHANGE = "VersionChange"


class ServiceStatus(Enum):
    """Nested class to hold status constants for the service status"""

    # Installation or Update reconciliation is in-progress
    IN_PROGRESS = "InProgress"

    # Installation or Update failed with error
    FAILED = "Failed"

    # Service is in stable state
    COMPLETED = "Completed"


def make_application_status(  # pylint: disable=too-many-arguments,too-many-locals
    ready_reason: Optional[Union[ReadyReason, str]] = None,
    ready_message: str = "",
    updating_reason: Optional[Union[UpdatingReason, str]] = None,
    updating_message: str = "",
    component_state: Optional[CompletionState] = None,
    external_conditions: Optional[List[dict]] = None,
    external_status: Optional[dict] = None,
    version: Optional[str] = None,
    supported_versions: Optional[List[str]] = None,
    operator_version: Optional[str] = None,
    kind: Optional[str] = None,
) -> dict:
    """Create a full status object for an application

    Args:
        ready_reason:  Optional[ReadyReason or str]
            The reason enum for the Ready condition
        ready_message:  str
            Plain-text message explaining the Ready condition value
        updating_reason:  Optional[UpdatingReason or str]
            The reason enum for the Updating condition
        updating_message:  str
            Plain-text message explaining the Updating condition value
        component_state:  Optional[CompletionState]
            The terminal state of components in the latest rollout
        external_conditions:  Optional[List[dict]]
            Additional conditions to include in the update
        external_status:  Optional[dict]
            Additional key/value status elements besides "conditions" that
            should be preserved through the update
        version:  Optional[str]
            The verified version of the application
        supported_versions:  Optional[List[str]]
            The list of supported versions for this application
        operator_version:  Optional[str]
            The operator version for this application
        kind: Optional[str]
            The kind of reconciliing CR. If specified, this function adds
            service status field which is compliant with IBM Cloud Pak
            requirements.

    Returns:
        current_status:  dict
            Dict representation of the status for the application
    """
    now = datetime.now()
    conditions = []
    if ready_reason is not None:
        conditions.append(_make_ready_condition(ready_reason, ready_message, now))
    if updating_reason is not None:
        conditions.append(
            _make_updating_condition(updating_reason, updating_message, now)
        )
    conditions.extend(external_conditions or [])
    status = external_status or {}
    status["conditions"] = conditions

    # If a component_state is given, create the top-level status elements to
    # track which components have deployed and verified
    if component_state is not None:
        log.debug2("Adding component state to status")
        status[COMPONENT_STATUS] = _make_component_state(component_state)
        log.debug3(status[COMPONENT_STATUS])

    # Create the versions section
    if version is not None:
        nested_set(status, VERSIONS_FIELD_CURRENT_VERSION, version)
    if supported_versions is not None:
        nested_set(
            status,
            VERSIONS_FIELD_AVAILABLE_VERSIONS,
            [_make_available_version(version) for version in supported_versions],
        )
    if operator_version is not None:
        nested_set(status, OPERATOR_VERSION, operator_version)

    # Create service status section
    if kind:
        # make field name follow k8s naming convention
        service_status_field = kind[0].lower()
        if len(kind) > 1:
            service_status_field += kind[1:]
        service_status_field += "Status"

        # Only update service status if the current value is set by oper8. This
        # allows services to override the service status section
        current_service_status = status.get(service_status_field)
        managed_service_values = [status.value for status in ServiceStatus]
        if (
            not current_service_status
            or current_service_status in managed_service_values
        ):
            current_service_status = _make_service_status(
                ready_reason, updating_reason
            ).value

        status[service_status_field] = current_service_status

    return status


def update_application_status(current_status: dict, **kwargs) -> dict:
    """Create an updated status based on the values in the current status

    Args:
        current_status:  dict
            The dict representation of the status for a given application
        **kwargs:
            Additional keyword args to pass to make_application_status

    Returns:
        updated_status:  dict
            Updated dict representation of the status for the application
    """
    # Make a deep copy of current_status so that we aren't accidentally
    # modifying the current status object. This prevents a bug where status
    # changes are not detected
    current_status = copy.deepcopy(current_status)

    # Make a dict of type -> condition. This is necessary because other
    # conditions may be applied by ansible
    current_conditions = current_status.get("conditions", [])
    current_condition_map = {cond["type"]: cond for cond in current_conditions}
    ready_cond = current_condition_map.get(READY_CONDITION, {})
    updating_cond = current_condition_map.get(UPDATING_CONDITION, {})

    # Setup the kwargs for the status call
    ready_reason = ready_cond.get("reason")
    updating_reason = updating_cond.get("reason")
    if ready_reason:
        kwargs.setdefault("ready_reason", ReadyReason(ready_reason))
    if updating_reason:
        kwargs.setdefault("updating_reason", UpdatingReason(updating_reason))
    kwargs.setdefault("ready_message", ready_cond.get("message", ""))
    kwargs.setdefault("updating_message", updating_cond.get("message", ""))

    # Extract external conditions managed by other portions of the operator
    external_conditions = [
        cond
        for cond in current_conditions
        if cond.get("type") not in [READY_CONDITION, UPDATING_CONDITION]
    ]
    log.debug3("External conditions: %s", external_conditions)
    kwargs["external_conditions"] = external_conditions
    log.debug3("Merged status kwargs: %s", kwargs)

    # Extract external status elements (besides conditions) managed by other
    # portions of the operator
    external_status = {
        key: val for key, val in current_status.items() if key != "conditions"
    }
    kwargs["external_status"] = external_status
    kwargs["operator_version"] = config.operator_version

    return make_application_status(**kwargs)


def update_resource_status(
    deploy_manager: "DeployManagerBase",  # noqa: F821
    kind: str,
    api_version: str,
    name: str,
    namespace: str,
    **kwargs: dict,
) -> dict:
    """Create an updated status based on the values in the current status

    Args:
        deploy_manager: DeployManagerBase
            The deploymanager used to get and set status
        kind: str
            The kind of the resource
        api_version: str
            The api_version of the resource
        name: str
            The name of the resource
        namespace: str
            The namespace the resource is located in
        **kwargs: Dict
            Any additional keyword arguments to be passed to update_application_status

    Returns:
        status_object: Dict
            The applied status if successful

    """
    log.debug3(
        "Updating status for %s/%s.%s/%s",
        namespace,
        api_version,
        kind,
        name,
    )

    # Fetch the current status from the cluster
    success, current_state = deploy_manager.get_object_current_state(
        api_version=api_version,
        kind=kind,
        name=name,
        namespace=namespace,
    )
    if not success:
        log.warning("Failed to fetch current state for %s/%s/%s", namespace, kind, name)
        return {}
    current_status = (current_state or {}).get("status", {})
    log.debug3("Pre-update status: %s", current_status)

    # Merge in the given status
    status_object = update_application_status(current_status, kind=kind, **kwargs)
    log.debug3("Updated status: %s", status_object)

    # Check to see if the status values of any conditions have changed and
    # only update the status if it has changed
    if status_changed(current_status, status_object):
        log.debug("Found meaningful change. Updating status")
        log.debug2("(current) %s != (updated) %s", current_status, status_object)

        # Do the update
        success, _ = deploy_manager.set_status(
            kind=kind,
            name=name,
            namespace=namespace,
            api_version=api_version,
            status=status_object,
        )

        # Since this is just a status update, we don't fail if the update fails,
        # but we do throw a warning
        if not success:
            log.warning("Failed to update status for [%s/%s/%s]", namespace, kind, name)
            return {}

    return status_object


def status_changed(current_status: dict, new_status: dict) -> bool:
    """Compare two status objects to determine if there is a meaningful change
    between the current status and the proposed new status. A meaningful change
    is defined as any change besides a timestamp.

    Args:
        current_status:  dict
            The raw status dict from the current CR
        new_status:  dict
            The proposed new status

    Returns:
        status_changed:  bool
            True if there is a meaningful change between the current status and
            the new status
    """
    # Status objects must be dicts
    if not isinstance(current_status, dict) or not isinstance(new_status, dict):
        return True

    # Perform a deep diff, excluding timestamps
    return bool(
        DeepDiff(
            current_status,
            new_status,
            exclude_obj_callback=lambda _, path: path.endswith(f"{TIMESTAMP_KEY}']"),
        )
    )


def get_condition(type_name: str, current_status: dict) -> dict:
    """Extract the given condition type from a status object

    Args:
        type:  str
            The condition type to fetch
        current_status:  dict
            The dict representation of the status for a given application

    Returns:
        condition:  dict
            The condition object if found, empty dict otherwise
    """
    cond = [
        cond
        for cond in current_status.get("conditions", [])
        if cond.get("type") == type_name
    ]
    if cond:
        assert len(cond) == 1, f"Found multiple condition entries for {type_name}"
        return cond[0]
    return {}


def get_version(current_status: dict) -> Optional[str]:
    """Extract the current version (not desired version) from a status object

    Args:
        current_status: dict
            The dict representation of the status for a given application

    Returns:
        version: Optional[dict]
            The current version if found in a status object, None otherwise.

    """
    return nested_get(current_status, VERSIONS_FIELD_CURRENT_VERSION)


## Implementation Details ######################################################


def _make_status_condition(
    type_name: str,
    status: bool,
    reason: str,
    message: str,
    last_transaction_time: datetime,
):
    """Convert the condition to the dict representation to be added to the
    kubernetes object
    """
    return {
        "type": type_name,
        "status": str(status),
        "reason": reason.value,
        "message": message,
        TIMESTAMP_KEY: last_transaction_time.isoformat(),
    }


def _make_ready_condition(
    reason: Union[ReadyReason, str],
    message: str,
    last_transaction_time: datetime,
):
    """Construct a ready condition with a reason and determine the status based
    on the reason
    """
    if isinstance(reason, str):
        reason = ReadyReason(reason)
    ready_status = reason == ReadyReason.STABLE
    log.debug2("%s status %s: %s", READY_CONDITION, ready_status, reason)
    return _make_status_condition(
        READY_CONDITION, ready_status, reason, message, last_transaction_time
    )


def _make_updating_condition(
    reason: Union[UpdatingReason, str],
    message: str,
    last_transaction_time: datetime,
):
    """Construct an updating condition with a reason and determine the status
    based on the reason
    """
    if isinstance(reason, str):
        reason = UpdatingReason(reason)
    if reason in [
        UpdatingReason.STABLE,
        UpdatingReason.CLUSTER_ERROR,
        UpdatingReason.ERRORED,
    ]:
        updating_status = False
    else:
        updating_status = True

    log.debug2("%s status %s: %s", UPDATING_CONDITION, updating_status, reason)
    return _make_status_condition(
        UPDATING_CONDITION, updating_status, reason, message, last_transaction_time
    )


def _make_component_state(component_state: CompletionState) -> dict:
    """Make the component state object"""
    all_nodes = sorted([comp.get_name() for comp in component_state.all_nodes])
    deployed_nodes = sorted(
        [
            comp.get_name()
            for comp in component_state.verified_nodes.union(
                component_state.unverified_nodes
            )
        ]
    )
    verified_nodes = sorted(
        [comp.get_name() for comp in component_state.verified_nodes]
    )
    unverified_nodes = sorted(
        [comp.get_name() for comp in component_state.unverified_nodes]
    )
    failed_nodes = sorted([comp.get_name() for comp in component_state.failed_nodes])
    return {
        COMPONENT_STATUS_ALL_NODES: all_nodes,
        COMPONENT_STATUS_DEPLOYED_NODES: deployed_nodes,
        COMPONENT_STATUS_UNVERIFIED_NODES: unverified_nodes,
        COMPONENT_STATUS_FAILED_NODES: failed_nodes,
        COMPONENT_STATUS_DEPLOYED_COUNT: f"{len(deployed_nodes)}/{len(all_nodes)}",
        COMPONENT_STATUS_VERIFIED_COUNT: f"{len(verified_nodes)}/{len(all_nodes)}",
    }


def _make_available_version(version: str) -> dict:
    """Make an object for the available version list following the IBM CloudPak
    spec
    """
    return {"name": version}


def _make_service_status(
    ready_reason: Optional[Union[ReadyReason, str]] = None,
    updating_reason: Optional[Union[UpdatingReason, str]] = None,
) -> ServiceStatus:
    """Make service status based on current ready and updating reason"""
    # consider empty ready/updating status as reconciliation is starting
    if ready_reason is None or updating_reason is None:
        return ServiceStatus.IN_PROGRESS

    ready_reason = (
        ReadyReason(ready_reason) if isinstance(ready_reason, str) else ready_reason
    )
    updating_reason = (
        UpdatingReason(updating_reason)
        if isinstance(updating_reason, str)
        else updating_reason
    )

    # Completed case
    if ready_reason == ReadyReason.STABLE and updating_reason == UpdatingReason.STABLE:
        return ServiceStatus.COMPLETED

    # Failed case
    if ready_reason in [ReadyReason.ERRORED, ReadyReason.CONFIG_ERROR]:
        return ServiceStatus.FAILED
    if updating_reason in [UpdatingReason.ERRORED, UpdatingReason.CLUSTER_ERROR]:
        return ServiceStatus.FAILED

    # The other cases are considered In-progress
    return ServiceStatus.IN_PROGRESS
