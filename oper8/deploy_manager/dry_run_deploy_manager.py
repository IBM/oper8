"""
The DryRunDeployManager implements the DeployManager interface but does not
actually interact with the cluster and instead holds the state of the cluster in
a local map.
"""

# Standard
from datetime import datetime, timedelta
from functools import partial
from queue import Empty, Queue
from threading import RLock
from typing import Callable, Iterator, List, Optional, Tuple
import copy
import operator
import random
import uuid

# First Party
import alog

# Local
from ..managed_object import ManagedObject
from ..utils import merge_configs
from .base import DeployManagerBase, DeployMethod
from .kube_event import KubeEventType, KubeWatchEvent
from .owner_references import update_owner_references

log = alog.use_channel("DRY-RUN")

# Lock to ensure disable/deploys are thread safe
DRY_RUN_CLUSTER_LOCK = RLock()


class DryRunDeployManager(DeployManagerBase):
    """
    Deploy manager which doesn't actually deploy!
    """

    def __init__(
        self,
        resources=None,
        owner_cr=None,
        strict_resource_version=False,
        generate_resource_version=True,
    ):
        """Construct with a static value to use for whether or not the functions
        should report change.
        """
        self._owner_cr = owner_cr
        self._cluster_content = {}
        self.strict_resource_version = strict_resource_version
        self.generate_resource_version = generate_resource_version

        # Dicts of registered watches and watchers
        self._watches = {}
        self._finalizers = {}

        # Deploy provided resources
        self._deploy(resources or [], call_watches=False, manage_owner_references=False)

    ## Interface ###############################################################

    def deploy(
        self,
        resource_definitions,
        manage_owner_references=True,
        method: DeployMethod = DeployMethod.DEFAULT,
        **_,
    ):
        log.info("DRY RUN deploy")
        return self._deploy(
            resource_definitions,
            manage_owner_references=manage_owner_references,
            method=method,
        )

    def disable(self, resource_definitions):
        log.info("DRY RUN disable")
        changed = False
        for resource in resource_definitions:
            api_version = resource.get("apiVersion")
            kind = resource.get("kind")
            name = resource.get("metadata", {}).get("name")
            namespace = resource.get("metadata", {}).get("namespace")
            _, content = self.get_object_current_state(
                kind=kind, api_version=api_version, namespace=namespace, name=name
            )
            if content is not None:
                changed = True

                # Set resource finalizers
                with DRY_RUN_CLUSTER_LOCK:
                    self._cluster_content[namespace][kind][api_version][name][
                        "metadata"
                    ]["deletionTimestamp"] = datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    self._cluster_content[namespace][kind][api_version][name][
                        "metadata"
                    ]["deletionGracePeriodSeconds"] = 0

                # Call any registered finalizers
                for key, callback in self._get_registered_watches(
                    api_version, kind, namespace, name, finalizer=True
                ):
                    log.debug2(
                        "Calling registered finalizer [%s] for [%s]", callback, key
                    )
                    callback(self._cluster_content[namespace][kind][api_version][name])

                # If finalizers have been cleared and object hasn't already been deleted then
                # remove the key
                current_obj = (
                    self._cluster_content.get(namespace, {})
                    .get(kind, {})
                    .get(api_version, {})
                    .get(name, {})
                )
                if current_obj and not current_obj.get("metadata", {}).get(
                    "finalizers", []
                ):
                    with DRY_RUN_CLUSTER_LOCK:
                        self._delete_key(namespace, kind, api_version, name)

        return True, changed

    def get_object_current_state(self, kind, name, namespace=None, api_version=None):
        log.info(
            "DRY RUN get_object_current_state of [%s/%s] in [%s]", kind, name, namespace
        )

        # Look in the cluster state
        matches = []
        kind_entries = self._cluster_content.get(namespace, {}).get(kind, {})
        log.debug3("Kind entries: %s", kind_entries)
        for api_ver, entries in kind_entries.items():
            log.debug3("Checking api_version [%s // %s]", api_ver, api_version)
            if name in entries and (api_ver == api_version or api_version is None):
                matches.append(entries[name])
        log.debug(
            "Found %d matches for [%s/%s] in %s", len(matches), kind, name, namespace
        )
        if len(matches) == 1:
            return True, copy.deepcopy(matches[0])
        return True, None

    def filter_objects_current_state(
        self,
        kind,
        namespace=None,
        api_version=None,
        label_selector=None,
        field_selector=None,
    ):  # pylint: disable=too-many-arguments
        log.info(
            "DRY RUN filter_objects_current_state of [%s] in [%s]", kind, namespace
        )
        # Look in the cluster state
        matches = []
        kind_entries = self._cluster_content.get(namespace, {}).get(kind, {})
        log.debug3("Kind entries: %s", kind_entries)
        for api_ver, entries in kind_entries.items():
            # Make sure api version matches
            log.debug3("Checking api_version [%s // %s]", api_ver, api_version)
            if api_ver != api_version and api_version is not None:
                continue

            for resource in entries.values():
                # Make sure Labels Match
                log.debug3("Resource: %s", resource)

                labels = resource.get("metadata", {}).get("labels", {})
                log.debug3("Checking label_selector [%s // %s]", labels, label_selector)
                if label_selector is not None and not _match_selector(
                    labels, label_selector
                ):
                    continue

                # Only do the work for field selector if one exists
                log.debug3("Checking field_selector [%s]", field_selector)
                if field_selector is not None and not _match_selector(
                    _convert_dict_to_dot(resource),
                    field_selector,
                ):
                    continue

                # Add deep copy of entry to matches list
                matches.append(copy.deepcopy(resource))

        return True, matches

    def set_status(
        self,
        kind,
        name,
        namespace,
        status,
        api_version=None,
    ):  # pylint: disable=too-many-arguments
        log.info(
            "DRY RUN set_status of [%s.%s/%s] in %s: %s",
            api_version,
            kind,
            name,
            namespace,
            status,
        )
        object_content = self.get_object_current_state(
            kind, name, namespace, api_version
        )[1]
        if object_content is None:
            log.debug("Did not find [%s/%s] in %s", kind, name, namespace)
            return False, False
        prev_status = object_content.get("status")
        object_content["status"] = status
        self._deploy([object_content], call_watches=False)
        return True, prev_status != status

    def watch_objects(  # pylint: disable=too-many-arguments,too-many-locals,unused-argument
        self,
        kind: str,
        api_version: Optional[str] = None,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
        resource_version: Optional[str] = None,
        timeout: Optional[int] = 15,
        **kwargs,
    ) -> Iterator[KubeWatchEvent]:
        """Watch the DryRunDeployManager for resource changes by registering
        callbacks"""

        event_queue = Queue()
        resource_map = {}

        def add_event(resource_map: dict, manifest: dict):
            """Callback triggered when resources are deployed"""
            resource = ManagedObject(manifest)
            event_type = KubeEventType.ADDED

            watch_key = self._watch_key(
                api_version=resource.api_version,
                kind=resource.kind,
                namespace=resource.namespace,
                name=resource.name,
            )
            if watch_key in resource_map:
                log.debug4("Watch key detected, setting Modified event type")
                event_type = KubeEventType.MODIFIED

            resource_map[watch_key] = resource
            event = KubeWatchEvent(
                type=event_type,
                resource=resource,
            )
            event_queue.put(event)

        def delete_event(resource_map: dict, manifest: dict):
            """Callback triggered when resources are disabled"""
            resource = ManagedObject(manifest)
            watch_key = self._watch_key(
                api_version=resource.api_version,
                kind=resource.kind,
                namespace=resource.namespace,
                name=resource.name,
            )
            if watch_key in resource_map:
                del resource_map[watch_key]

            event = KubeWatchEvent(
                type=KubeEventType.DELETED,
                resource=resource,
            )
            event_queue.put(event)

        # Get initial resources
        _, manifests = self.filter_objects_current_state(
            kind=kind,
            api_version=api_version,
            namespace=namespace,
            label_selector=label_selector,
            field_selector=field_selector,
        )
        for manifest in manifests:
            resource = ManagedObject(manifest)
            watch_key = self._watch_key(
                kind=resource.kind,
                api_version=resource.api_version,
                name=resource.name,
                namespace=resource.namespace,
            )
            resource_map[watch_key] = resource

            event = KubeWatchEvent(type=KubeEventType.ADDED, resource=resource)
            log.debug2("Yielding initial event %s", event)
            yield event

        end_time = datetime.max
        if timeout:
            end_time = datetime.now() + timedelta(seconds=timeout)

        # Register callbacks
        self.register_watch(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
            name=name,
            callback=partial(add_event, resource_map),
        )
        self.register_finalizer(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
            name=name,
            callback=partial(delete_event, resource_map),
        )

        # Yield any events from the callback queue
        log.debug2("Waiting till %s", end_time)
        while True:
            sec_till_end = (end_time - datetime.now()).seconds or 1
            try:
                event = event_queue.get(timeout=sec_till_end)
                log.debug2("Yielding event %s", event)
                yield event
            except Empty:
                pass

            if datetime.now() > end_time:
                return

    ## Dry Run Methods #########################################################
    def register_watch(  # pylint: disable=too-many-arguments
        self,
        api_version: str,
        kind: str,
        callback: Callable[[dict], None],
        namespace="",
        name="",
    ):
        """Register a callback to watch for deploy events on a given
        api_version/kind
        """
        watch_key = self._watch_key(
            api_version=api_version, kind=kind, namespace=namespace, name=name
        )
        log.debug("Registering watch for %s", watch_key)
        self._watches.setdefault(watch_key, []).append(callback)

    def register_finalizer(  # pylint: disable=too-many-arguments
        self,
        api_version: str,
        kind: str,
        callback: Callable[[dict], None],
        namespace="",
        name="",
    ):
        """Register a callback to call on deletion events on a given
        api_version/kind
        """
        watch_key = self._watch_key(
            api_version=api_version, kind=kind, namespace=namespace, name=name
        )
        log.debug("Registering finalizer for %s", watch_key)
        self._finalizers.setdefault(watch_key, []).append(callback)

    ## Implementation Details ##################################################

    @staticmethod
    def _watch_key(api_version="", kind="", namespace="", name=""):
        return ":".join([api_version or "", kind or "", namespace or "", name or ""])

    def _get_registered_watches(  # pylint: disable=too-many-arguments
        self,
        api_version: str = "",
        kind: str = "",
        namespace: str = "",
        name: str = "",
        finalizer: bool = False,
    ) -> List[Tuple[str, Callable]]:
        # Get the scoped watch key
        resource_watch_key = self._watch_key(
            api_version=api_version, kind=kind, namespace=namespace, name=name
        )
        namespaced_watch_key = self._watch_key(
            api_version=api_version, kind=kind, namespace=namespace
        )
        global_watch_key = self._watch_key(api_version=api_version, kind=kind)

        # Get which watch list we're pulling from
        callback_map = self._watches
        if finalizer:
            callback_map = self._finalizers

        output_list = []
        log.debug3(
            "Looking for resourced key: %s namespace key %s global key %s",
            resource_watch_key,
            namespaced_watch_key,
            global_watch_key,
        )
        for key, callback_list in callback_map.items():
            if key in [resource_watch_key, namespaced_watch_key, global_watch_key]:
                log.debug3("%d Callbacks found for key %s", len(callback_list), key)
                for callback in callback_list:
                    output_list.append((key, callback))

        return output_list

    def _delete_key(self, namespace, kind, api_version, name):
        del self._cluster_content[namespace][kind][api_version][name]
        if not self._cluster_content[namespace][kind][api_version]:
            del self._cluster_content[namespace][kind][api_version]
        if not self._cluster_content[namespace][kind]:
            del self._cluster_content[namespace][kind]
        if not self._cluster_content[namespace]:
            del self._cluster_content[namespace]

    def _deploy(
        self,
        resource_definitions,
        call_watches=True,
        manage_owner_references=True,
        method: DeployMethod = DeployMethod.DEFAULT,
    ):
        log.info("DRY RUN deploy")
        changes = False
        for resource in resource_definitions:
            api_version = resource.get("apiVersion")
            kind = resource.get("kind")
            name = resource.get("metadata", {}).get("name")
            namespace = resource.get("metadata", {}).get("namespace")
            log.debug(
                "DRY RUN deploy [%s/%s/%s/%s]", namespace, kind, api_version, name
            )
            log.debug4(resource)

            # If owner CR configured, add ownerReferences
            if self._owner_cr and manage_owner_references:
                log.debug2("Adding dry-run owner references")
                update_owner_references(self, self._owner_cr, resource)
                log.debug3(
                    "All owner references: %s", resource["metadata"]["ownerReferences"]
                )

            with DRY_RUN_CLUSTER_LOCK:
                entries = (
                    self._cluster_content.setdefault(namespace, {})
                    .setdefault(kind, {})
                    .setdefault(api_version, {})
                )
                current = copy.deepcopy(entries.get(name, {}))
                old_resource_version = current.get("metadata", {}).pop(
                    "resourceVersion", None
                )
                changes = changes or (current != resource)

                if "metadata" not in resource:
                    resource["metadata"] = {}

                if (
                    self.strict_resource_version
                    and resource["metadata"].get("resourceVersion")
                    and old_resource_version
                    and resource["metadata"].get("resourceVersion")
                    != old_resource_version
                ):
                    log.warning(
                        "Unable to deploy resource. resourceVersion is out of date"
                    )
                    return False, False

                resource["metadata"]["creationTimestamp"] = entries.get(
                    "metadata", {}
                ).get("creationTimestamp", datetime.now().isoformat())
                resource["metadata"]["uid"] = entries.get("metadata", {}).get(
                    "uid", str(uuid.uuid4())
                )

                if self.generate_resource_version:
                    resource["metadata"]["resourceVersion"] = str(
                        random.randint(1, 1000)
                    ).zfill(5)

                # Depending on the deploy method either update or fully replace the object
                if method == DeployMethod.DEFAULT or method == DeployMethod.REPLACE:
                    entries[name] = resource
                else:
                    if name in entries:
                        entries[name] = merge_configs(entries[name], resource)
                    # If the object doesn't already exist then just add it
                    else:
                        entries[name] = resource

            # Call any registered watches
            if call_watches:
                for key, callback in self._get_registered_watches(
                    api_version, kind, namespace, name
                ):
                    log.debug2("Calling registered watch [%s] for [%s]", callback, key)
                    callback(resource)

            # Delete Key if it has already been disabled and doesn't have finalizers
            if self._cluster_content[namespace][kind][api_version][name].get(
                "metadata", {}
            ).get("deletionTimestamp") and not self._cluster_content[namespace][kind][
                api_version
            ][
                name
            ].get(
                "metadata", {}
            ).get(
                "finalizers"
            ):
                with DRY_RUN_CLUSTER_LOCK:
                    self._delete_key(namespace, kind, api_version, name)

        return True, changes


def _match_selector(values, value_selector) -> bool:  # pylint: disable=too-many-locals
    """This function implements the kubernetes selector to determine if
    a set of values matches the selector. For the complete documentation regardin
    selectors see:
    https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
    """
    log.debug2("DRY RUN match_selector [%s/%s]", values, value_selector)

    # Configure List of Operators used in splitting
    equality_ops = ["=", "==", "!="]
    # Note the spaces are required to correctly distinguish between operators and random characters
    set_ops = [" in ", " notin "]
    existence_ops = ["!", ""]

    # Define dict of ops to python functions. Tried to use operator library as much as possible but
    # four custom functions were needed
    def _in(a, b):  # pylint: disable=invalid-name
        return a in b

    def not_in(a, b):  # pylint: disable=invalid-name
        return not _in(a, b)

    def exists(a, _):  # pylint: disable=invalid-name
        return a is not None

    def not_exists(a, _):  # pylint: disable=invalid-name
        return a is None

    operator_actions = {
        "=": operator.eq,
        "==": operator.eq,
        "!=": operator.ne,
        " in ": _in,
        " notin ": not_in,
        "!": not_exists,
        "": exists,
    }

    # Sort the list of operators reverse by size so when splitting it doesn't mess up
    # e.g. check splitting by != before = as = is apart of !=.
    operator_list = sorted(operator_actions.keys(), key=len, reverse=True)

    # Loop through all selectors to validate each one
    for selector in _split_selectors(value_selector):
        # Variables set during splitting and used for verification
        action = None
        expected_key = None
        expected_value = None

        # Get the operator in use by the selector. This is done
        # by trying to split by each operator until one works
        for op in operator_list:  # pylint: disable=invalid-name
            # Either split the selector or remove the NonExistence operator
            split_selector = []
            if op in existence_ops:
                split_selector = [selector.replace(op, "")]
            else:
                split_selector = selector.split(op)

            # If Selector doesn't split correctly for equality_ops/set_ops
            if (op in equality_ops or op in set_ops) and len(split_selector) != 2:
                continue

            # If the operator is ! then make sure ! was actually removed from the selector
            if (op == "!") and "!" not in selector:
                continue

            # Get the action and key
            action = operator_actions[op]
            expected_key = split_selector[0].strip()

            # If op is equality then the entire second half is the expected value
            if op in equality_ops:
                expected_value = split_selector[1].strip()

            # If op is set then format the expected values that are in the form (value,value,etc)
            # Into a list
            elif op in set_ops:
                string_value = split_selector[1]
                string_value = string_value.replace("(", "")
                string_value = string_value.replace(")", "")
                expected_value = string_value.split(",")

            # Once the selector is found break out of the loop
            break

        # Get the value at the expected key
        value = values.get(expected_key)

        # Convert all values to strings unless None
        value = str(value).strip() if value is not None else value

        if not action(value, expected_value):
            log.debug3(
                "Label with key: %s and value: %s does not match selector %s",
                expected_key,
                value,
                selector,
            )
            return False

    # If all selectors matched then return True
    return True


def _split_selectors(selector=""):
    """Split up selectors by , but ignoring those surrounded by () e.g.
    'app,app in (frontend, backend)' becomes ['app','app in (frontend, backend)']
    """
    output_list = []
    current_selector = ""
    in_paren = False

    # Loop through selector character by character
    for char in selector:
        # If we've reached a comma not while in parentheses then append to output
        # and start anew
        if char == "," and not in_paren:
            output_list.append(current_selector)
            current_selector = ""
            continue

        # If detect paren then update in_paren
        if char == "(" and not in_paren:
            in_paren = True
        elif char == ")" and in_paren:
            in_paren = False

        # Append the current char
        current_selector += char

    # If current selector is not empty then add it to the output list
    if current_selector:
        output_list.append(current_selector)

    return output_list


def _convert_dict_to_dot(dictionary, prefix=""):
    """Helper function to convert a dictionary to a map
    of strings dotted together. For example {a:{b:1},c:2}
    becomes {a.b:1,c:2}
    """
    if not isinstance(dictionary, dict):
        return {prefix: dictionary}

    output_dict = {}
    for key in dictionary:
        new_key = key if prefix == "" else f"{prefix}.{key}"
        output_dict = {**output_dict, **_convert_dict_to_dot(dictionary[key], new_key)}
    return output_dict
