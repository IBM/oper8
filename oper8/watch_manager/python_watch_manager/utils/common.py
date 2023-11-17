"""
Shared utilities for the PythonWatchManager
"""
# Standard
from datetime import timedelta
from typing import Any, List, Optional
import json
import logging
import pathlib
import platform
import re

# First Party
import alog

# Local
from .... import config

log = alog.use_channel("PWMCMMN")


## Time Functions

# Shamelessly stolen from
# https://stackoverflow.com/questions/4628122/how-to-construct-a-timedelta-object-from-a-simple-string
regex = re.compile(
    r"^((?P<hours>\d+?)hr)?((?P<minutes>\d+?)m)?((?P<seconds>\d*\.?\d+?)s)?$"
)


def parse_time_delta(
    time_str: str,
) -> Optional[timedelta]:  # pylint: disable=inconsistent-return-statements
    """Parse a string into a timedelta. Excepts values in the
    following formats: 1h, 5m, 10s, etc

    Args:
        time_str: str
            The string representation of a timedelta

    Returns:
        result: Optional[timedelta]
            The parsed timedelta if one could be found
    """
    parts = regex.match(time_str)
    if not parts or all(part is None for part in parts.groupdict().values()):
        return None
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = float(param)
    return timedelta(**time_params)


## Identity Util Functions


def get_operator_namespace() -> str:
    """Get the current namespace from a kubernetes file or config"""
    # Default to in cluster namespace file
    namespace_file = pathlib.Path(
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    )
    if namespace_file.is_file():
        return namespace_file.read_text(encoding="utf-8")
    return config.python_watch_manager.lock.namespace


def get_pod_name() -> str:
    """Get the current pod from env variables, config, or hostname"""

    pod_name = config.pod_name
    if not pod_name:
        log.warning("Pod name not detected, falling back to hostname")
        pod_name = platform.node().split(".")[0]

    return pod_name


## Helper functions


def obj_to_hash(obj: Any) -> str:
    """Get the hash of any jsonable python object

    Args:
        obj: Any
            The object to hash

    Returns:
        hash: str
            The hash of obj
    """
    return hash(json.dumps(obj, sort_keys=True))


def get_logging_handlers() -> List[logging.Handler]:
    """Get the current logging handlers"""
    logger = logging.getLogger()
    if not logger.handlers:
        handler = logging.StreamHandler()
        logger.addHandler(handler)

    return logger.handlers
