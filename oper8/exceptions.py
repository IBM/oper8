"""
This module implements custom exceptions
"""

## Base Error ##################################################################


class Oper8Error(Exception):
    """Base class for all oper8 exceptions"""

    def __init__(self, message: str, is_fatal_error: bool):
        """Construct with a flag indicating whether this is a fatal error. This
        will be a static property of all children.
        """
        super().__init__(message)
        self._is_fatal_error = is_fatal_error

    @property
    def is_fatal_error(self):
        """Property indicating whether or not this error should signal a fatal
        state in the rollout
        """
        return self._is_fatal_error


## Fatal Errors ################################################################


class Oper8FatalError(Oper8Error):
    """An Oper8FatalError is one that indicates an unexpected, and likely
    unrecoverable, failure during a reconciliation.
    """

    def __init__(self, message: str = ""):
        super().__init__(message=message, is_fatal_error=True)


class RolloutError(Oper8FatalError):
    """Exception indicating a failure during application rollout"""

    def __init__(self, message: str = "", completion_state=None):
        self.completion_state = completion_state
        super().__init__(message)


class ConfigError(Oper8FatalError):
    """Exception caused during usage of user-provided configuration"""


class ClusterError(Oper8FatalError):
    """Exception caused during chart construction when a cluster operation fails
    in an unexpected way.
    """


## Expected Errors #############################################################


class Oper8ExpectedError(Oper8Error):
    """An Oper8ExpectedError is one that indicates an expected failure condition
    that should cause a reconciliation to terminate, but is expected to resolve
    in a subsequent reconciliation.
    """

    def __init__(self, message: str = ""):
        super().__init__(message=message, is_fatal_error=False)


class PreconditionError(Oper8ExpectedError):
    """Exception caused during chart construction when an expected precondition
    is not met.
    """


class VerificationError(Oper8ExpectedError):
    """Exception caused during resource verification when a desired verification
    state is not reached.
    """


## Assertions ##################################################################


def assert_precondition(condition: bool, message: str = ""):
    """Replacement for assert() which will throw a PreconditionError. This
    should be used when building a chart which requires that a precondition is
    met before continuing.
    """
    if not condition:
        raise PreconditionError(message)


def assert_verified(condition: bool, message: str = ""):
    """Replacement for assert() which will throw a VerificationError. This
    should be used when verifying the state of a resource in the cluster.
    """
    if not condition:
        raise VerificationError(message)


def assert_config(condition: bool, message: str = ""):
    """Replacement for assert() which will throw a ConfigError. This should be
    used when building a chart which requires that certain conditions be true in
    the deploy_config or app_config.
    """
    if not condition:
        raise ConfigError(message)


def assert_cluster(condition: bool, message: str = ""):
    """Replacement for assert() which will throw a ClusterError. This should
    be used when building a chart which requires that an operation in the
    cluster (such as fetching an existing secret) succeeds.
    """
    if not condition:
        raise ClusterError(message)


## Compatibility Exceptions ##################################################################
class Oper8DeprecationWarning(DeprecationWarning):
    """This warning is issued for deprecated APIs"""


class Oper8PendingDeprecationWarning(PendingDeprecationWarning):
    """This warning is issued for APIs that are still supported but will be removed eventually"""
