"""
Shared module to hold constant values for the library
"""

# Reconciliation configuration annotations
PAUSE_ANNOTATION_NAME = "oper8.org/pause-execution"
CONFIG_DEFAULTS_ANNOTATION_NAME = "oper8.org/config-defaults"

# Leadership annotations
LEASE_NAME_ANNOTATION_NAME = "oper8.org/lease-name"
LEASE_TIME_ANNOTATION_NAME = "oper8.org/lease-time"

# Log config annotations
LOG_DEFAULT_LEVEL_NAME = "oper8.org/log-default-level"
LOG_FILTERS_NAME = "oper8.org/log-filters"
LOG_THREAD_ID_NAME = "oper8.org/log-thread-id"
LOG_JSON_NAME = "oper8.org/log-json"

# List to keep track of all oper8-managed annotations which should be passed
# from a parent Application to a child CR.
# NOTE: The only excluded annotations are
# * temporary patches as this is managed by the oper8_temporary_patch module
#   directly
# * leadership annotations since those may differ per CR instance
PASSTHROUGH_ANNOTATIONS = [
    CONFIG_DEFAULTS_ANNOTATION_NAME,
    LOG_DEFAULT_LEVEL_NAME,
    LOG_FILTERS_NAME,
    LOG_JSON_NAME,
    LOG_THREAD_ID_NAME,
    PAUSE_ANNOTATION_NAME,
]

# BACKWARDS COMPATIBILITY: We maintain the ALL_ANNOTATIONS name for
# compatibility with old code that accessed this directly
ALL_ANNOTATIONS = PASSTHROUGH_ANNOTATIONS

# The name of the annotation used to attach TemporaryPatch resources to a given
# oper8-managed CR
TEMPORARY_PATCHES_ANNOTATION_NAME = "oper8.org/temporary-patches"

# The name of the annotation used to indicate the internal name of each
# oper8-managed resource
INTERNAL_NAME_ANOTATION_NAME = "oper8.org/internal-name"

# Default namespace if none given
DEFAULT_NAMESPACE = "default"

# Delimiter used for nested dict keys
NESTED_DICT_DELIM = "."

# Name of the spec section used to provide config overrides
CONFIG_OVERRIDES = "configOverrides"
