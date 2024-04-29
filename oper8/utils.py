"""
Common utilities shared across components in the library
"""

# Standard
from typing import Any
import copy
import datetime
import inspect

# Third Party
import six

# First Party
import aconfig
import alog

# Local
from . import config, constants
from .dag import ResourceNode
from .exceptions import assert_cluster

log = alog.use_channel("OPUTL")


# Forward declaration for Session
SESSION_TYPE = "Session"

# Sentinel for missing dict values
__MISSING__ = "__MISSING__"

## Dicts #######################################################################


def merge_configs(base, overrides) -> dict:
    """Helper to perform a deep merge of the overrides into the base. The merge
    is done in place, but the resulting dict is also returned for convenience.

    The merge logic is quite simple: If both the base and overrides have a key
    and the type of the key for both is a dict, recursively merge, otherwise
    set the base value to the override value.

    Args:
        base:  dict
            The base config that will be updated with the overrides
        overrides:  dict
            The override config

    Returns:
        merged:  dict
            The merged results of overrides merged onto base
    """
    for key, value in overrides.items():
        if (
            key not in base
            or not isinstance(base[key], dict)
            or not isinstance(value, dict)
        ):
            base[key] = value
        else:
            base[key] = merge_configs(base[key], value)

    return base


def nested_set(dct: dict, key: str, val: Any):
    """Helper to set values in a dict using 'foo.bar' key notation

    Args:
        dct:  dict
            The dict into which the key will be set
        key:  str
            Key that may contain '.' notation indicating dict nesting
        val:  Any
            The value to place at the nested key
    """
    parts = key.split(constants.NESTED_DICT_DELIM)
    for i, part in enumerate(parts[:-1]):
        dct = dct.setdefault(part, {})
        if not isinstance(dct, dict):
            raise TypeError(
                "Intermediate key {} is not a dict".format(  # pylint: disable=consider-using-f-string
                    constants.NESTED_DICT_DELIM.join(parts[:i])
                )
            )
    dct[parts[-1]] = val


def nested_get(dct: dict, key: str, dflt=None) -> Any:
    """Helper to get values from a dict using 'foo.bar' key notation

    Args:
        dct:  dict
            The dict into which the key will be set
        key:  str
            Key that may contain '.' notation indicating dict nesting

    Returns:
        val:  Any
            Whatever is found at the given key or None if the key is not found.
            This includes missing intermediate dicts.
    """
    parts = key.split(constants.NESTED_DICT_DELIM)
    for i, part in enumerate(parts[:-1]):
        dct = dct.get(part, __MISSING__)
        if dct is __MISSING__:
            return dflt
        if not isinstance(dct, dict):
            raise TypeError(
                "Intermediate key {} is not a dict".format(  # pylint: disable=consider-using-f-string
                    constants.NESTED_DICT_DELIM.join(parts[:i])
                )
            )
    return dct.get(parts[-1], dflt)


# Stolen from kubernetes but modified for None pruning and safety
# https://github.com/kubernetes-client/python/blob/d67bc8c2bdb89b29c17c1ba0edb03a48d977c0e2/kubernetes/client/api_client.py#L202
def sanitize_for_serialization(obj):  # pylint: disable=too-many-return-statements
    """Builds a JSON POST object.
    If obj is None, return None.
    If obj is str, int, long, float, bool, return directly.
    If obj is datetime.datetime, datetime.date
        convert to string in iso8601 format.
    If obj is list, sanitize each element in the list.
    If obj is dict, return the dict.
    If obj is OpenAPI model, return the properties dict.
    :param obj: The data to serialize.
    :return: The serialized form of data.
    """
    if obj is None:  # pylint: disable=no-else-return
        return None
    elif isinstance(obj, (float, bool, bytes, six.text_type) + six.integer_types):
        return obj
    elif isinstance(obj, list):
        return [sanitize_for_serialization(sub_obj) for sub_obj in obj]
    elif isinstance(obj, tuple):
        return tuple(sanitize_for_serialization(sub_obj) for sub_obj in obj)
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    elif isinstance(obj, ResourceNode):
        return sanitize_for_serialization(obj.manifest)
    elif isinstance(obj, property):
        return sanitize_for_serialization(obj.fget())

    if isinstance(obj, dict):
        obj_dict = obj
    elif hasattr(obj, "attribute_map"):
        # Convert model obj to dict except
        # `openapi_types` and `attribute_map`.
        # Convert attribute name to json key in
        # model definition for request.
        obj_dict = {}
        for attr, name in six.iteritems(obj.attribute_map):
            if hasattr(obj, attr):
                obj_dict[name] = getattr(obj, attr)

    # Prune fields which are None but keep
    # empty arrays or dictionaries
    return_dict = {}
    for key, val in six.iteritems(obj_dict):
        updated_obj = sanitize_for_serialization(val)
        if updated_obj is not None:
            return_dict[key] = updated_obj
    return return_dict


## Common Usage Patterns #######################################################


def get_passthrough_annotations(session):
    """This helper gets the set of annotations that should be passed from a
    parent CR to a child subsystem CR.

    Args:
        session:  DeploySession
            The session for the current deploy

    Returns:
        annotations:  Dict[str, str]
            The dict mapping of annotations that should be passed through
    """
    annotations = session.metadata.get("annotations", {})
    passthrough_annotations = {
        k: v for k, v in annotations.items() if k in constants.ALL_ANNOTATIONS
    }

    log.debug2("Oper8 passthrough annotations: %s", passthrough_annotations)
    return passthrough_annotations


def get_manifest_version(cr_manifest: aconfig.Config) -> str:
    """Get the version for a given custom resource or from the config
    if version override provided

    Args:
        cr_manifest: aconfig.Config
            The manifest to pull the version from

    Returns:
        version: str
            The current version
    """
    if config.vcs.version_override:
        return config.vcs.version_override
    return nested_get(cr_manifest, config.vcs.field)


def add_finalizer(session: SESSION_TYPE, finalizer: str):
    """This helper adds a finalizer to current session CR

    Args:
        session:  Session
            The session for the current deploy
        finalizer: str
            The finalizer to be added
    """
    if finalizer in session.finalizers:
        return

    log.debug("Adding finalizer: %s", finalizer)

    manifest = {
        "kind": session.kind,
        "apiVersion": session.api_version,
        "metadata": copy.deepcopy(session.metadata),
    }
    manifest["metadata"].setdefault("finalizers", []).append(finalizer)
    success, _ = session.deploy_manager.deploy([manifest])

    # Once successfully added to cluster than add it to session
    assert_cluster(success, f"Failed add finalizer {finalizer}")
    session.finalizers.append(finalizer)


def remove_finalizer(session: SESSION_TYPE, finalizer: str):
    """This helper gets removes a finalizer from the current session controller

    Args:
        session:  Session
            The session for the current deploy
        finalizer: str
            The finalizer to remove

    Returns:
        annotations:  Dict[str, str]
            The dict mapping of annotations that should be passed through
    """
    if finalizer not in session.finalizers:
        return

    log.debug("Removing finalizer: %s", finalizer)

    # Create manifest with only required fields
    manifest = {
        "kind": session.kind,
        "apiVersion": session.api_version,
        "metadata": copy.deepcopy(session.metadata),
    }

    # Check to see if the object exists in the cluster
    success, found = session.get_object_current_state(
        kind=session.kind,
        api_version=session.api_version,
        name=session.name,
    )
    assert_cluster(success, "Failed to look up CR for self")

    # If still present in the cluster, update it without the finalizer
    if found:
        manifest["metadata"]["finalizers"].remove(finalizer)
        success, _ = session.deploy_manager.deploy([manifest])

        # Once successfully removed from cluster than remove from session
        assert_cluster(success, f"Failed remove finalizer {finalizer}")

    # If the finalizer has been confirmed to not be there, remove it from the
    # in-memory finalizers
    session.finalizers.remove(finalizer)


## General #####################################################################


class classproperty:  # pylint: disable=invalid-name,too-few-public-methods
    """@classmethod+@property
    CITE: https://stackoverflow.com/a/22729414
    """

    def __init__(self, func):
        self.func = classmethod(func)

    def __get__(self, *args):
        return self.func.__get__(*args)()


class abstractclassproperty:  # pylint: disable=invalid-name,too-few-public-methods
    """This decorator implements a classproperty that will raise when accessed"""

    def __init__(self, func):
        self.prop_name = func.__name__

    def __get__(self, *args):
        # If this is being called by __setattr__, we're ok because it's
        # apptempting to set the attribute on the class
        curframe = inspect.currentframe()
        callframe = inspect.getouterframes(curframe, 2)[1]
        caller_name = callframe[3]
        if caller_name == "__setattr__":
            return None

        # If this is a help() call or a pdoc documentation request, return an
        # object with a docstring indicating that the property is abstract
        if (
            "help" in callframe.frame.f_code.co_names
            or callframe.frame.f_globals["__name__"] == "pdoc"
        ):

            class AbstractClassProperty:  # pylint: disable=missing-class-docstring
                __slots__ = []
                __doc__ = f"""The <{self.prop_name}> property is an abstract class property
                that must be overwritten in derived children
                """

            return AbstractClassProperty

        raise NotImplementedError(
            f"Cannot access abstractclassproperty {self.prop_name}"
        )
