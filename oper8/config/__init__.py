"""
Base operator config module. The config here is only used as a baseline bootup
config. All application-specific config must come from the app_config.
"""

# Standard
import sys as _sys

# Local
from .config import library_config


# Define __getattr__ on this module to delegate to the library config.
def __getattr__(name):
    if name in library_config or hasattr({}, name):
        return getattr(library_config, name)
    raise AttributeError(f"No such config attribute {name}")


# Only expose the library config keys
__all__ = list(library_config.keys())
