"""
Test the verify_resources functionality
"""

# Standard
from datetime import datetime, timedelta
from typing import Optional

# First Party
import alog

# Local
from oper8 import status
from oper8.session import _SESSION_NAMESPACE
from oper8.test_helpers.helpers import (
    MockDeployManager,
    configure_logging,
    setup_session,
)
from oper8.utils import nested_set
from oper8.verify_resources import (
    AVAILABLE_CONDITION_KEY,
    COMPLETE_CONDITION_KEY,
    DEFAULT_TIMESTAMP_KEY,
    NEW_RS_AVAILABLE_REASON,
    PROGRESSING_CONDITION_KEY,
    verify_resource,
)

configure_logging()
log = alog.use_channel("TEST")

## Helpers #####################################################################

TEST_NAMESPACE = "test"


def run_test_verify(
    kind,
    conditions=None,
    status=None,
    populate_state=True,
    version="1.2.3",
    obj_namespace=TEST_NAMESPACE,
    search_namespace=TEST_NAMESPACE,
    **kwargs
):
    """Common helper for all tests"""

    # We're not testing the lookup functionality, so just use a canned name and
    # api_version
    name = "foo"
    api_version = "v1"

    # Set up the state of the cluster
    object_state = {}
    if populate_state:
        object_state = {
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {
                "name": name,
                "namespace": obj_namespace,
            },
        }
        if conditions is not None:
            status = status or {}
            status.update({"conditions": conditions})

        if status is not None:
            object_state["status"] = status

    # Set up a mock deploy manager with the seed state
    dm = MockDeployManager(resources=[object_state])

    # Set up the session with the state
    session = setup_session(
        namespace=TEST_NAMESPACE, deploy_manager=dm, version=version
    )

    # Run the verify call on the resource
    return verify_resource(
        kind=kind,
        name=name,
        api_version=api_version,
        session=session,
        namespace=search_namespace,
        **kwargs
    )


def make_condition(
    type_name,
    status,
    timestamp=None,
    timestamp_key=None,
    timestamp_str=True,
    reason: Optional[str] = None,
):
    """Helper for making conditions easily"""
    timestamp_key = timestamp_key or DEFAULT_TIMESTAMP_KEY
    if timestamp_str:
        timestamp = timestamp or datetime.now().isoformat()
    return {
        "type": type_name,
        "status": str(status),
        timestamp_key: timestamp,
        "reason": reason,
    }


def make_status_with_version():
    out = {}
    nested_set(out, status.VERSIONS_FIELD_CURRENT_VERSION, "1.0.0")
    return out


## Tests #######################################################################

##########
## Pods ##
##########


def test_verify_pod_ready():
    """Make sure a ready pod verifies cleanly"""
    assert run_test_verify(kind="Pod", conditions=[make_condition("Ready", True)])


def test_verify_pod_unready():
    """Make sure an unready pod returns as unverified"""
    assert not run_test_verify(kind="Pod", conditions=[make_condition("Ready", False)])


def test_verify_pod_missing():
    """Make sure a missing pod returns as unverified"""
    assert not run_test_verify(kind="Pod", populate_state=None)


def test_verify_pod_no_conditions():
    """Make sure a pod with no conditions returns as unverified"""
    assert not run_test_verify(kind="Pod", conditions=[])


def test_verify_pod_no_status():
    """Make sure a pod with no status returns as unverified"""
    assert not run_test_verify(kind="Pod", conditions=None)


def test_verify_pod_separate_namespace():
    """Make sure a ready pod from a different namespace verifies cleanly"""
    assert run_test_verify(
        kind="Pod",
        conditions=[make_condition("Ready", True)],
        obj_namespace="adifferent",
        search_namespace="adifferent",
    )


def test_verify_pod_null_namespace():
    """Make sure a ready pod in the same namespace verifies cleanly"""
    assert run_test_verify(
        kind="Pod",
        conditions=[make_condition("Ready", True)],
        obj_namespace=TEST_NAMESPACE,
        search_namespace=_SESSION_NAMESPACE,
    )


def test_verify_pod_custom_verification():
    """Make sure a ready pod fails to verify with a custom override"""
    assert not run_test_verify(
        kind="Pod",
        conditions=[make_condition("Ready", True)],
        verify_function=lambda resource: False,
    )


##########
## Jobs ##
##########


def test_verify_job_completed():
    """Make sure a completed job verifies cleanly"""
    assert run_test_verify(
        kind="Job", conditions=[make_condition(COMPLETE_CONDITION_KEY, True)]
    )


def test_verify_job_failed():
    """Make sure a failed job returns as unverified"""
    assert not run_test_verify(kind="Job", conditions=[make_condition("Failed", True)])


def test_verify_job_suspended():
    """Make sure a suspended job returns as unverified"""
    assert not run_test_verify(
        kind="Job", conditions=[make_condition("Suspended", True)]
    )


def test_verify_job_missing():
    """Make sure a missing job returns as unverified"""
    assert not run_test_verify(kind="Job", populate_state=None)


def test_verify_job_no_conditions():
    """Make sure a job with no conditions returns as unverified"""
    assert not run_test_verify(kind="Job", conditions=[])


def test_verify_job_no_status():
    """Make sure a job with no status returns as unverified"""
    assert not run_test_verify(kind="Job", conditions=None)


def test_verify_job_separate_namespace():
    """Make sure a completed job from a different namespace verifies cleanly"""
    assert run_test_verify(
        kind="Job",
        conditions=[make_condition(COMPLETE_CONDITION_KEY, True)],
        obj_namespace="adifferent",
        search_namespace="adifferent",
    )


def test_verify_job_null_namespace():
    """Make sure a completed job in the same namespace verifies cleanly"""
    assert run_test_verify(
        kind="Job",
        conditions=[make_condition(COMPLETE_CONDITION_KEY, True)],
        obj_namespace=TEST_NAMESPACE,
        search_namespace=_SESSION_NAMESPACE,
    )


#################
## Deployments ##
#################


def test_verify_deploy_stable():
    """Make sure a deploy with all of the expected replicas verifies cleanly"""
    assert run_test_verify(
        kind="Deployment",
        conditions=[
            make_condition(AVAILABLE_CONDITION_KEY, True),
            make_condition(
                PROGRESSING_CONDITION_KEY, True, reason=NEW_RS_AVAILABLE_REASON
            ),
        ],
    )


def test_verify_deploy_unavailable():
    """Make sure a deploy without all of the expected replicas returns as
    unverified
    """
    assert not run_test_verify(
        kind="Deployment", conditions=[make_condition(AVAILABLE_CONDITION_KEY, False)]
    )


def test_verify_deploy_rollout_uncompleted():
    """Make sure a deploy is not verified while rolling out to new version
    is still in-progress
    """
    assert not run_test_verify(
        kind="Deployment",
        conditions=[
            make_condition(AVAILABLE_CONDITION_KEY, True),
            make_condition(PROGRESSING_CONDITION_KEY, True, reason="ReplicaSetUpdated"),
        ],
    )


def test_verify_deploy_missing():
    """Make sure a missing deploy returns as unverified"""
    assert not run_test_verify(kind="Deployment", populate_state=None)


def test_verify_deploy_no_conditions():
    """Make sure a deploy with no conditions returns as unverified"""
    assert not run_test_verify(kind="Deployment", conditions=[])


def test_verify_deploy_no_status():
    """Make sure a deploy with no status returns as unverified"""
    assert not run_test_verify(kind="Deployment", conditions=None)


##################
## StatefulSets ##
##################


def test_verify_ss_available():
    """Make sure a statefulset with all of the expected replicas verifies
    cleanly
    """
    assert run_test_verify(
        kind="StatefulSet", status={"replicas": 3, "readyReplicas": 3}
    )


def test_verify_ss_some_ready():
    """Make sure a statefulset with only some replicas ready is unverified"""
    assert not run_test_verify(
        kind="StatefulSet", status={"replicas": 3, "readyReplicas": 2}
    )


def test_verify_ss_none_ready():
    """Make sure a statefulset with no readyReplicas is unverified"""
    assert not run_test_verify(kind="StatefulSet", status={"replicas": 3})


def test_verify_ss_missing():
    """Make sure a missing deploy returns as unverified"""
    assert not run_test_verify(kind="StatefulSet", populate_state=None)


def test_verify_ss_no_status():
    """Make sure a deploy with no conditions returns as unverified"""
    assert not run_test_verify(kind="StatefulSet")


################
## Subsystems ##
################


def test_verify_subsystem_ready():
    """Make sure a ready subsystem verifies cleanly"""
    assert run_test_verify(
        kind="FooBar",
        is_subsystem=True,
        version="1.0.0",
        status=make_status_with_version(),
        conditions=[
            make_condition(
                status.READY_CONDITION, True, timestamp_key=status.TIMESTAMP_KEY
            ),
            make_condition(
                status.UPDATING_CONDITION, False, timestamp_key=status.TIMESTAMP_KEY
            ),
        ],
    )


def test_verify_subsystem_unready():
    """Make sure an unready subsystem verifies returns as unverified"""
    assert not run_test_verify(
        kind="FooBar",
        is_subsystem=True,
        version="1.0.0",
        status=make_status_with_version(),
        conditions=[
            make_condition(
                status.READY_CONDITION, False, timestamp_key=status.TIMESTAMP_KEY
            )
        ],
    )


def test_verify_subsystem_unready_updating_in_progress():
    """Make sure a subsystem is not verified during update"""
    assert not run_test_verify(
        kind="FooBar",
        is_subsystem=True,
        version="1.0.0",
        status=make_status_with_version(),
        conditions=[
            make_condition(
                status.READY_CONDITION, True, timestamp_key=status.TIMESTAMP_KEY
            ),
            make_condition(
                status.UPDATING_CONDITION, True, timestamp_key=status.TIMESTAMP_KEY
            ),
        ],
    )


def test_verify_subsystem_unready_version_mismatch():
    """Make sure a subsystem is not verified in case updating is not started
    by checking current version and desired version difference.
    """
    assert not run_test_verify(
        kind="FooBar",
        is_subsystem=True,
        version="2.0.0",
        status=make_status_with_version(),
        conditions=[
            make_condition(
                status.READY_CONDITION, True, timestamp_key=status.TIMESTAMP_KEY
            ),
            make_condition(
                status.UPDATING_CONDITION, False, timestamp_key=status.TIMESTAMP_KEY
            ),
        ],
    )


def test_verify_subsystem_unready_no_version():
    """Make sure a subsystem is not verified in case version status is not set
    as initial rollout is not completed.
    """
    assert not run_test_verify(
        kind="FooBar",
        is_subsystem=True,
        version="2.0.0",
        conditions=[
            make_condition(
                status.READY_CONDITION, True, timestamp_key=status.TIMESTAMP_KEY
            ),
            make_condition(
                status.UPDATING_CONDITION, False, timestamp_key=status.TIMESTAMP_KEY
            ),
        ],
    )


def test_verify_subsystem_missing():
    """Make sure a missing subsystem returns as unverified"""
    assert not run_test_verify(kind="FooBar", is_subsystem=True, populate_state=False)


def test_verify_subsystem_no_conditions():
    """Make sure a subsystem with no conditions returns as unverified"""
    assert not run_test_verify(kind="FooBar", is_subsystem=True, conditions=[])


def test_verify_subsystem_no_status():
    """Make sure a subsystem with no status returns as unverified"""
    assert not run_test_verify(kind="FooBar", is_subsystem=True, conditions=None)


#######################
## Custom Conditions ##
#######################


def test_custom_condition_true():
    """Make sure that verification with a custom condition that is true returns
    as verified
    """
    assert run_test_verify(
        kind="WingBat",
        condition_type="Custom",
        conditions=[make_condition("Custom", True)],
    )


def test_custom_condition_true_custom_timestamp():
    """Make sure that verification with a custom condition and a custom
    timestamp key that is true returns as verified
    """
    assert run_test_verify(
        kind="WingBat",
        condition_type="Custom",
        timestamp_key="custom",
        conditions=[make_condition("Custom", True, timestamp_key="custom")],
    )


def test_custom_condition_false():
    """Make sure that verification with a custom condition that is false returns
    as unverified
    """
    assert not run_test_verify(
        kind="WingBat",
        condition_type="Custom",
        conditions=[make_condition("Custom", False)],
    )


def test_custom_condition_missing():
    """Make sure that verification with a custom condition that is missing
    returns as unverified
    """
    assert not run_test_verify(
        kind="WingBat", condition_type="Custom", populate_state=False
    )


###########
## Other ##
###########


def test_multiple_condition_entries():
    """Make sure that a resource with multiple condition entries for the same
    type uses the latest
    """
    first_ts = datetime.now()
    t0 = first_ts.isoformat()
    t1 = (first_ts + timedelta(minutes=1)).isoformat()
    assert run_test_verify(
        kind="Pod",
        conditions=[
            make_condition("Ready", False, timestamp=t0),
            make_condition("Ready", True, timestamp=t1),
        ],
    )


def test_non_bool_str_value():
    """Make sure that a condition with a string value that doesn't look like a
    bool returns as unverified
    """
    assert not run_test_verify(
        kind="WingBat",
        condition_type="Custom",
        conditions=[make_condition("Custom", "NotABool")],
    )


def test_generic_resource_present():
    """Make sure that a resource without a known verifier returns true when
    present
    """
    assert run_test_verify(kind="Service")


def test_generic_resource_missing():
    """Make sure that a resource without a known verifier returns false when
    not present
    """
    assert not run_test_verify(kind="Service", populate_state=False)


def test_condition_missing_status():
    """Make sure that a resource with a condition that has no status returns as
    unverified
    """
    assert not run_test_verify(kind="Pod", conditions=[{"type": "Ready"}])


def test_condition_non_str_status():
    """Make sure that a non-string status is handled by casting to bool"""
    assert run_test_verify(kind="Pod", conditions=[{"type": "Ready", "status": True}])


def test_datetime_timestamp():
    """Make sure that when a timestamp field has been parsed as a datetime
    object by the yaml parser, it is handled correctly
    """
    assert run_test_verify(
        kind="Pod",
        conditions=[
            make_condition(
                "Ready", True, timestamp=datetime.now(), timestamp_str=False
            ),
        ],
    )
