"""
Shared utilities accessible to all components
"""

# Standard
from typing import Dict, Optional, Union
import base64
import copy
import re

# First Party
import alog

# Local
from . import constants

# First party
from oper8 import Component, Session, assert_config

log = alog.use_channel("UTIL")


def b64_secret(val):
    if isinstance(val, str):
        val = val.encode("utf-8")
    return base64.b64encode(val).decode("utf-8")


def b64_secret_decode(val):
    if isinstance(val, str):
        val = val.encode("utf-8")
    return base64.b64decode(val).decode("utf-8")


def get_resource_cluster_name(
    resource_name: str,
    component: Component,
    session: Session,
):
    """Common helper function to get the name a given kubernetes resource should
    use when deployed to the cluster.

    Args:
        resource_name:  str
            The raw name for the resource (e.g. sireg-secret)
        component:  Union[Component, str]
            The component (or component name) that owns this resource
        session:  Session
            The session for the current reconciliation deploy

    Returns:
        resource_cluster_name:  str
            The resource name with appropriate scoping and truncation added
    """
    if is_global(component, session):
        log.debug2(
            "Applying global name logic to [%s] for component [%s]",
            resource_name,
            component,
        )
        return session.get_truncated_name(resource_name)
    return session.get_scoped_name(resource_name)


def get_replicas(
    session: Session,
    component_name: str,
    unscoped_name: str,
    force: bool = False,
    replicas_override: Union[int, None] = None,
) -> Union[int, None]:
    """
    Get the replica count for the given resource.

    This function consolidates logic for getting replicas for all components in
    the application. It allows replicas to be conditionally set only when needed
    to avoid thrashing with HPAs.

    Args:
        session: Session
            The current deploy session
        component_name: str
            The name of the component to get replicas for
        unscoped_name: str
            The external name of the deployment without scoping
        force: bool
            If True, the state of the cluster will not be checked
        replicas_override: int or None
            An override value to use in place of the normal config-based value

    Returns:
        replicas: int or None
            If replicas should not be set for this resource, None is returned,
            otherwise the number of replicas is returned based on the t-shirt
            size for the instance.
    """

    # Fetch the current state of the deployment
    if not force:
        name = get_resource_cluster_name(
            resource_name=unscoped_name,
            component=component_name,
            session=session,
        )
        success, content = session.get_object_current_state(
            kind="Deployment",
            name=name,
            api_version="apps/v1",
        )
        assert success, f"Failed to look up state for [{name}]"

        # Check the current content to see if this is a t-shirt size change
        if content is not None:
            # Fetch the current replica count. We'll reuse this if there's no
            # reason to change
            replicas = content.get("spec", {}).get("replicas")

            # If we found replicas, check for t-shirt size change
            if replicas is None:
                log.debug("No replicas found for [%s]. Using config.".name)
            else:
                assert isinstance(replicas, int), "Replicas is not an int!"
                current_size = session.spec.size
                deployed_size = (
                    content.get("metadata", {}).get("labels", {}).get("instance-size")
                )
                if replicas == 0 and not session.spec.get("backup", {}).get(
                    "offlineQuiesce", False
                ):
                    log.debug(
                        "Found [%s] with size [%s] and offlineQuiesce off. Need "
                        "to scale up from [%s] replicas.",
                        name,
                        current_size,
                        replicas,
                    )
                elif current_size == deployed_size:
                    log.debug(
                        "Found [%s] with size [%s]. Not changing replicas from [%s].",
                        name,
                        current_size,
                        replicas,
                    )
                    return replicas
                else:
                    log.debug(
                        "Found t-shirt size change for [%s] from [%s -> %s]",
                        name,
                        deployed_size,
                        current_size,
                    )

    # Look up the replicas based on the t-shirt size
    size = session.spec.size
    replica_map = session.config.get("replicas", {}).get(size, {})
    replicas = replicas_override or replica_map.get(component_name)
    log.debug3("Replica map for [%s]: %s", size, replica_map)
    assert_config(
        replicas is not None,
        f"No replicas for [{component_name}] available for size [{size}]",
    )
    return replicas


def is_global(component: Union[Component, str], session: Session) -> bool:
    """Determine if the given component is global in this deployment

    Args:
        component:  Union[Component, str]
            The component to fetch the slot name for
        session:  Session
            The session for the current deployment

    Returns:
        is_global:  bool
            True if the given component is global!
    """
    component_name = component if isinstance(component, str) else component.name
    return session.config.get(component_name, {}).get(constants.GLOBAL_SLOT, False)


def get_deploy_labels(session, base_labels=None):
    """Get labels for a Deployment resource on top of the standard base labels"""
    # Shallow copy is fine here since labels are one-level deep and only strings
    deploy_labels = copy.copy(base_labels or {})
    deploy_labels["instance-size"] = session.spec.size
    return deploy_labels


def camelcase_to_snake_case(x):
    if isinstance(x, str):
        return re.sub(r"(?<!^)(?=[A-Z])", "_", x).lower()
    elif isinstance(x, list):
        return [y if isinstance(y, str) else camelcase_to_snake_case(y) for y in x]
    elif isinstance(x, dict):
        return {
            camelcase_to_snake_case(k): v
            if isinstance(v, str)
            else camelcase_to_snake_case(v)
            for k, v in x.items()
        }
    else:
        return x


def snake_case_to_camelcase(x):
    if isinstance(x, str):
        return (
            x[0].lower() + "".join([world.capitalize() for world in x.split("_")])[1:]
        )
    elif isinstance(x, list):
        return [y if isinstance(y, str) else snake_case_to_camelcase(y) for y in x]
    elif isinstance(x, dict):
        return {
            snake_case_to_camelcase(k): v
            if isinstance(v, str)
            else snake_case_to_camelcase(v)
            for k, v in x.items()
        }
    else:
        return x


def get_slot_name(component: Union[Component, str], session: Session) -> str:
    """Get the slot name for the given component in the current deployment

    Args:
        component:  Union[Component, str]
            The component to fetch the slot name for
        session:  DeploySession
            The session for the current deployment

    Returns:
        slot_name:  str
            The string name of the slot where the given component will live for
            this deployment. For global components, the static global slot name
            is returned
    """
    if not is_global(component, session):
        return session.name
    return constants.GLOBAL_SLOT


def get_labels(
    cluster_name: str,
    session: Session,
    component_name: Optional[str] = None,
) -> Dict[str, str]:
    """Common utility for fetching the set of metadata.labels for a given resource.
    Args:
        cluster_name:  str
            The name of the resource as it will be applied to the cluster
            including any scoping applied by get_resource_cluster_name
        session:  DeploySession
            The session for the current deployment
        component_name:  str
            The name of the component that manages this resource.
            NOTE: This argument is optional for backwards compatibility,
                but should always be provided to ensure accurate labels!
    Returns:
        labels:  Dict[str, str]
            The full set of labels to use for the given resource
    """
    labels = {
        "app": cluster_name,
        "app.kubernetes.io/managed-by": "Oper8",
        "app.kubernetes.io/instance": session.name,
    }
    if component_name:
        labels["component"] = component_name
    if slot_name := get_slot_name(component_name, session):
        labels["slot"] = slot_name

    # Add user-specified labels from the CR's spec.labels field
    user_labels = session.spec.labels or {}
    labels.update(user_labels)

    return labels


def metadata_defaults(
    cluster_name: str,
    session: Session,
    **kwargs,
) -> dict:
    """This function will create the metadata object given the external name for
    a resource. The external name should be created using
    common.get_resource_external_name. These functions are separate because the
    external name is often needed independently, so it will be pre-computed at
    the start of most components.

    Args:
        cluster_name:  str
            The fully scoped and truncated name that the resource will use in
            the cluster (metadata.name)
        session:  DeploySession
            The session for the current reconciliation deploy

    Returns:
        metadata:  dict
            The constructed metadata dict
    """
    # NOTE: For the time being, there are no defaults injected here, but we will
    #   retain the abstraction function so that we can add defaulting
    #   functionality (e.g. multi-namespace deployments) without touching every
    #   file.
    return {"name": cluster_name, **kwargs}


def from_string_or_number(value: Union[int, str, float]) -> Union[int, str, float]:
    """Handle strings or numbers for fields that can be either string or numeric

    Args:
        value: Union[str, int, float]
            Quantity type that can be in numeric or string form (e.g. resources)

    Returns:
        formatted_value: Union[str, int, float]
            The value formatted as the correct type
    """
    # By default no conversion is needed
    formatted_value = value

    # If it's a string, try converting it to an int, then a float
    if isinstance(value, str):
        for target_type in [int, float]:
            try:
                formatted_value = target_type(value)
                break
            except ValueError:
                pass

    return formatted_value


def mount_mode(octal_val):
    """This helper gets the decimal version of an octal representation of file
    permissions used for a volume mount.

    Args:
        octal_val:  int or str
            The number as octal (e.g. 755 or "0755")

    Returns:
        decimal_val:  int
            The decimal integer value corresponding to the given octal value
            which can be used in VolumeMount's default_mode field
    """
    return int(str(octal_val), 8)
