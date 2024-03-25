"""
This module holds shared functionality for adding dependency annotations to all
resources that need them.

A dependency annotation on a Pod encodes a unique hash of the set of
data-resources that the Pod depends on. For example, if a Pod mounds a Secret
and a ConfigMap, the dependency annotation will hold a unique hash of the data
content of these secrets. The role of the dependency annotation is to force a
rollover when upstream data-resources change their content so that the content
is guaranteed to be picked up by the consuming Pod.
"""

# Standard
from typing import List, Optional, Tuple, Union
import hashlib
import json

# First Party
import alog

# Local
from .constants import DEPS_ANNOTATION
from oper8 import Component, Session
from oper8.session import _SESSION_NAMESPACE
from oper8.utils import merge_configs

log = alog.use_channel("DEPS")

## Common Functions ############################################################


@alog.logged_function(log.debug)
def add_deps_annotation(
    component: Component,
    session: Session,
    resource_definition: dict,
) -> dict:
    """Add the dependency hash annotation to any pods found in the given object

    Args:
        component:  Component
            The component that this resource belongs to
        session:  Session
            The session for this deploy
        resource_definition:  dict
            The dict representation of the resource to modify

    Returns:
        resource_definition:  dict
            The dict representation of the resource with any modifications
            applied
    """
    resource_name = "{}/{}".format(
        resource_definition.get("kind"),
        resource_definition.get("metadata", {}).get("name"),
    )

    # Look for any/all pod annotations
    pod = _find_pod(resource_definition)
    if pod is not None:
        log.debug2("Found Pod for [%s]", resource_name)
        log.debug4(pod)

        # Traverse through and look for anything that looks like a secret or
        # configmap reference
        deps_map = _find_pod_data_deps(pod)
        log.debug3("Deps Map: %s", deps_map)
        if deps_map:
            # Go through each dependency and determine if it needs to be fetched
            # of if it's part of the owning component
            deps_list = []
            for dep_kind, dep_names in deps_map.items():
                for dep_name in dep_names:
                    # Look for this object in the objects managed by this
                    # component.
                    #
                    # NOTE: This will only be the components which have been
                    #   declared earlier in the chart or have explicitly been
                    #   marked as upstreams of this object.
                    found_in_component = False
                    for obj in component.managed_objects:
                        log.debug4("Checking %s/%s", obj.kind, obj.name)
                        if obj.kind == dep_kind and obj.name == dep_name:
                            log.debug3(
                                "Found intra-chart dependency of %s: %s",
                                resource_name,
                                obj,
                            )
                            deps_list.append(obj.definition)
                            found_in_component = True
                            break

                    # If not found in the component, add it as a lookup
                    if not found_in_component:
                        log.debug3(
                            "Found extra-chart dependency of %s: %s/%s",
                            resource_name,
                            dep_kind,
                            dep_name,
                        )
                        deps_list.append((dep_kind, dep_name))

            # Add the annotation with the full list
            md = pod.setdefault("metadata", {})
            annos = md.setdefault("annotations", {})
            md["annotations"] = merge_configs(
                annos, get_deps_annotation(session, deps_list, resource_name)
            )

    log.debug4("Updated Definition of [%s]: %s", resource_name, resource_definition)
    return resource_definition


def get_deps_annotation(
    session: Session,
    dependencies: List[Union[dict, Tuple[str, str]]],
    resource_name: str = "",
    namespace: Optional[str] = _SESSION_NAMESPACE,
) -> dict:
    """Get a dict holding an annotation key/value pair representing the unique
    content hash of all given dependencies. This can be used to force pods to
    roll over when a dependency such as a ConfigMap or Secret changes its
    content. This function supports two ways of fetching dependency content:

    1. Dict representation of the object
    2. Tuple of the scoped (kind, name) for the object

    Additionally, this function holds special logic for ConfigMap and Secret
    dependencies, but can handle arbitrary kinds. For kinds without special
    logic, the full dict representation is used to compute the hash.

    Args:
        session:  Session
            The current session
        dependencies:  list(dict or str or cdk8s.ApiObject)
            An ordered list of dependencies to compute the content hash from
        resource_name:  str
            A string name for the resource (used for logging)
        namespace:  Optional[str]
            Namespace where the dependencies live. Defaults to session.namespace

    Returns:
        deps_annotation:  dict
            A dict representation of the key/value pair used to hold the content
            hash for the given set of dependencies
    """
    content_hash = hashlib.sha1()
    namespace = namespace if namespace != _SESSION_NAMESPACE else session.namespace
    for dep in dependencies:
        # Get the dict representation depending on what type this is
        if isinstance(dep, tuple):
            log.debug3("[%s] Handling tuple dependency: %s", resource_name, dep)
            assert len(dep) == 2, f"Invalid dependency tuple given: {dep}"
            kind, name = dep
            success, dep_dict = session.get_object_current_state(
                name=name,
                kind=kind,
                namespace=namespace,
            )
            assert success, f"Failed to fetch current state of {kind}/{name}"

            # There are several reasons that the upstream dependency would not
            # be found, some legitimate and some not:
            #
            # 1. The dependency is not managed by this operator and this is a
            #   dry run. This can't be solved since we don't have control over
            #   the state of the cluster in dry run.
            #
            # 2. The dependency is part of a cyclic dependency between
            #   Components. While a sign of something bad, this is ultimately
            #   something that needs to be solved by decoupling the Component
            #   dependencies.
            #
            # 3. The upstream is an undeclared chart dependency. This is an
            #   easily fixed bug in the component by adding the necessary
            #   add_dependency() calls.
            #
            # 4. The upstream is part of an undeclared component dependency.
            #   This is an easily fixed bug in the parent Application by adding
            #   the missing add_component_dependency() calls.
            #
            # Since some of these are things that should be quickly fixed, but
            # some are signs of larger systemic problems, we warn and move on.
            # For (1), these external dependencies should be present in the
            # cluster. For the rest, once the deploy completes for the coupled
            # components, the resources will show up and the next reconcile will
            # cause the hash to change to what it should be.
            if dep_dict is None:
                log.warning(
                    "Working around missing external data dependency for [%s]: %s/%s",
                    resource_name,
                    kind,
                    name,
                )
                continue
        else:
            log.debug3(
                "[%s] Handling dict dependency: %s",
                resource_name,
                dep.get("metadata", {}).get("name"),
            )
            assert isinstance(dep, dict), f"Unknown dependency type: {type(dep)}"
            dep_dict = dep

        # The hash should be unique to the name and kind
        kind = dep_dict.get("kind", "")
        name = dep_dict.get("metadata", {}).get("name", "")
        content_hash.update(kind.encode("utf-8"))
        content_hash.update(name.encode("utf-8"))

        # Compute the data hash based on any kind-specific logic
        if kind in ["Secret", "ConfigMap"]:
            log.debug2("Getting data hash for dep of kind %s", kind)
            data_dict = dep_dict.get("data", {})
        else:
            log.debug2("Getting full hash for dep of kind %s", kind)
            data_dict = dep_dict
        log.debug4("Data Dict: %s", data_dict)

        # Add to the overall hash
        content_hash.update(json.dumps(data_dict, sort_keys=True).encode("utf-8"))

    # Return the annotation dict
    final_hash = content_hash.hexdigest()
    log.debug2("[%s] Final Hash: %s", resource_name, final_hash)
    return {DEPS_ANNOTATION: final_hash}


## Implementation Details ######################################################


def _find_pod(resource_definition: dict) -> dict:
    """Look through the object and return a refernce to any pod resource or
    template found
    """
    kind = resource_definition.get("kind")
    log.debug2("Looking for pod annotations for %s", kind)

    # Pod
    if kind == "Pod":
        return resource_definition

    # Deployment, ReplicaSet, StatefulSet
    if kind in ["Deployment", "ReplicaSet", "StatefulSet"]:
        return resource_definition.setdefault("spec", {}).setdefault("template", {})

    # No pod annotations found
    log.debug("No pod annotations found for [%s]", kind)
    return None


def _find_pod_data_deps(pod: dict) -> dict:
    """Look through a pod's definition for any references to Secret or ConfigMap
    resources
    """
    log.debug4("Looking for deps in: %s", pod)
    pod_spec = pod.get("spec", {})

    deps_map = {}

    # volumes
    for volume in pod_spec.get("volumes", []):
        # Secret volume
        secret_name = volume.get("secret", {}).get("secretName")
        if secret_name:
            log.debug2("Found Secret volume dependency: %s", secret_name)
            deps_map.setdefault("Secret", set()).add(secret_name)

        # ConfigMap volume
        cm_name = volume.get("configMap", {}).get("name")
        if cm_name:
            log.debug2("Found ConfigMap volume dependency: %s", cm_name)
            deps_map.setdefault("ConfigMap", set()).add(cm_name)

    # env
    for container in pod_spec.get("containers", []):
        for env_var in container.get("env", []):
            value_from = env_var.get("valueFrom", {})
            if value_from:
                # Secret reference
                secret_name = value_from.get("secretKeyRef", {}).get("name")
                if secret_name:
                    log.debug2("Found Secret env dependency: %s", secret_name)
                    deps_map.setdefault("Secret", set()).add(secret_name)

                # ConfigMap reference
                cm_name = value_from.get("configMapKeyRef", {}).get("name")
                if cm_name:
                    log.debug2("Found ConfigMap env dependency: %s", cm_name)
                    deps_map.setdefault("ConfigMap", set()).add(cm_name)

    # Return the set of named deps types
    return {dep_type: sorted(list(deps)) for dep_type, deps in deps_map.items()}
