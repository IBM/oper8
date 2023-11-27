"""
Test the functions in the common utility
"""

# Third Party
import pytest

# First Party
import aconfig
import alog

# Local
from oper8.exceptions import ConfigError
from oper8.test_helpers.helpers import (
    MockDeployManager,
    configure_logging,
    setup_cr,
    setup_session,
)
from oper8.test_helpers.oper8x_helpers import set_object_test_state
from oper8.x.utils import common

configure_logging()
log = alog.use_channel("TEST")


## Unit Tests ##################################################################


def test_get_replicas():

    # Size small
    _app_config = aconfig.Config(
        {"replicas": {"small": {"foo": 1}, "medium": {"foo": 2}}}
    )
    _deploy_config = aconfig.Config({"size": "small"})
    _cr = setup_cr(
        metadata={"namespace": "test", "name": "foo"},
        kind="Deployment",
        apiVersion="apps/v1",
    )
    _dm = MockDeployManager(resources=[_cr])

    session = setup_session(
        app_config=_app_config,
        deploy_config=_deploy_config,
        deploy_manager=_dm,
    )
    # get_replicas when previous state is not present should return valid
    # replica count
    assert common.get_replicas(session, "foo", "foo") == 1

    # Make sure an override is respected when resource is not present
    assert common.get_replicas(session, "foo", "foo", replicas_override=2) == 2

    # Set the state in the cluster and make sure that get_replicas returns
    # the current replication count
    success, changed = set_object_test_state(
        session,
        kind="Deployment",
        name="foo",
        value={
            "apiVersion": "apps/v1",
            "metadata": {
                "labels": common.get_deploy_labels(
                    session,
                )
            },
            "spec": {"replicas": 3},
        },
    )
    assert success
    assert changed
    assert common.get_replicas(session, "foo", "foo") == 3

    # Make sure an override is not used when resource is present
    assert common.get_replicas(session, "foo", "foo", replicas_override=2) == 3

    # Make sure a force=True call returns the value even when the resource
    # exists in the cluster
    assert common.get_replicas(session, "foo", "foo", force=True) == 1

    # Make sure that changing the t-shirt size causes the replicas to be
    # returned
    session.spec.size = "medium"
    assert common.get_replicas(session, "foo", "foo") == 2

    # Size medium: make sure sizes are used as keys correctly
    session = setup_session(
        app_config=_app_config,
        deploy_config=aconfig.Config({"size": "medium"}),
    )
    assert common.get_replicas(session, "foo", "foo") == 2

    # Size large: make sure misconfigured size raises a ConfigError
    session = setup_session(
        app_config=_app_config,
        deploy_config=aconfig.Config({"size": "large"}),
    )
    with pytest.raises(ConfigError):
        common.get_replicas(session, "foo", "foo")


def test_snake_case_to_camelcase():
    test_input = [
        None,
        "string_snake_case",
        {
            "dict_key_snake_case": "dict_value_snake_case",
            "dict_key_snake_case1": None,
        },
    ]
    expected = [
        None,
        "string_snake_case",
        {"dictKeySnakeCase": "dict_value_snake_case", "dictKeySnakeCase1": None},
    ]
    camel_conversion = common.snake_case_to_camelcase(test_input)
    assert camel_conversion == expected
    round_trip = common.camelcase_to_snake_case(camel_conversion)
    assert round_trip == test_input
