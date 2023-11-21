"""
This module just loads config at import time and does the initial log config
"""

# Standard
import os

# First Party
import aconfig
import alog

# Local
from .validation import get_invalid_params

# Read the library config, allowing env overrides
library_config = aconfig.Config.from_yaml(
    os.path.join(os.path.dirname(__file__), "config.yaml"),
    override_env_vars=True,
)

# Parse the validation file, not allowing env overrides
validation_config = aconfig.Config.from_yaml(
    os.path.join(os.path.dirname(__file__), "config_validation.yaml"),
    override_env_vars=False,
)

# Validate the loaded config values
invalid_params = get_invalid_params(library_config, validation_config)
assert (
    not invalid_params
), f"Library configuration found invalid values: {invalid_params}"

# Do initial alog configuration
alog.configure(
    default_level=library_config.log_level,
    filters=library_config.log_filters,
    formatter="json" if library_config.log_json else "pretty",
    thread_id=library_config.log_thread_id,
)
