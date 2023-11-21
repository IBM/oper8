"""
Test the custom assert functions
"""

# Third Party
import pytest

# Local
from oper8 import exceptions


def test_assert_precondition_pass():
    """Make sure that no exception is throw by assert_precondition when it
    passes
    """
    exceptions.assert_precondition(True)


def test_assert_precondition_fail():
    """Make sure the right exception is thrown by assert_precondition when it
    fails
    """
    exception_msg = "error mesage"
    with pytest.raises(exceptions.PreconditionError, match=exception_msg):
        exceptions.assert_precondition(False, exception_msg)


def test_assert_config_pass():
    """Make sure that no exception is throw by assert_config when it
    passes
    """
    exceptions.assert_config(True)


def test_assert_config_fail():
    """Make sure the right exception is thrown by assert_config when it
    fails
    """
    exception_msg = "error mesage"
    with pytest.raises(exceptions.ConfigError, match=exception_msg):
        exceptions.assert_config(False, exception_msg)


def test_assert_cluster_pass():
    """Make sure that no exception is throw by assert_cluster when it
    passes
    """
    exceptions.assert_cluster(True)


def test_assert_cluster_fail():
    """Make sure the right exception is thrown by assert_cluster when it
    fails
    """
    exception_msg = "error mesage"
    with pytest.raises(exceptions.ClusterError, match=exception_msg):
        exceptions.assert_cluster(False, exception_msg)


def test_exception_derived_from_base():
    """Make sure the derived classes is instance of the base Oper8 Class"""
    fatal_error = exceptions.Oper8FatalError()
    assert isinstance(fatal_error, exceptions.Oper8Error)
    expected_error = exceptions.Oper8ExpectedError()
    assert isinstance(expected_error, exceptions.Oper8Error)


def test_cluter_is_fatal():
    """Make sure the cluster error is considered fatal error"""
    with pytest.raises(exceptions.ClusterError) as cluster_error:
        exceptions.assert_cluster(False)
    assert isinstance(cluster_error.value, exceptions.Oper8FatalError)
    assert cluster_error.value.is_fatal_error


def test_config_is_fatal():
    """Make sure the config error is considered fatal error"""
    with pytest.raises(exceptions.ConfigError) as config_error:
        exceptions.assert_config(False)
    assert isinstance(config_error.value, exceptions.Oper8FatalError)
    assert config_error.value.is_fatal_error


def test_rollout_is_fatal():
    """Make sure the rollout error is considered fatal error"""
    with pytest.raises(exceptions.RolloutError) as rollout_error:
        raise exceptions.RolloutError()
    assert isinstance(rollout_error.value, exceptions.Oper8FatalError)
    assert rollout_error.value.is_fatal_error


def test_precondition_is_non_fatal():
    """Make sure the expected errors are not setting the fatal error flag"""
    with pytest.raises(exceptions.PreconditionError) as precondition_error:
        exceptions.assert_precondition(False)
    assert not precondition_error.value.is_fatal_error
    assert not isinstance(precondition_error.value, exceptions.Oper8FatalError)
    assert isinstance(precondition_error.value, exceptions.Oper8ExpectedError)


def test_verification_is_non_fatal():
    """Make sure the expected errors are not setting the fatal error flag"""
    msg = "Some verification message"
    with pytest.raises(exceptions.VerificationError, match=msg) as verification_error:
        exceptions.assert_verified(False, msg)
    assert not verification_error.value.is_fatal_error
    assert not isinstance(verification_error.value, exceptions.Oper8FatalError)
    assert isinstance(verification_error.value, exceptions.Oper8ExpectedError)
