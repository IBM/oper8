"""Useful Constants"""


## Reconcile Constants

# Default timeout when joining processes
JOIN_PROCESS_TIMEOUT = 5

# Default Poll Time for running reconcile cleanup on shutdown
SHUTDOWN_RECONCILE_POLL_TIME = 0.1

## Timer Constants

# Minimum wait time between checks in periodic thread
MIN_SLEEP_TIME = 1


## Filter Constants

# Only keep a set number of resource versions per watched resource
# this limits the amount of memory used
RESOURCE_VERSION_KEEP_COUNT = 20

# List of reserved annotations used by the platforms
RESERVED_PLATFORM_ANNOTATIONS = [
    "k8s.io",
    "kubernetes.io",
    "openshift.io",
]
