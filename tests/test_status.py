"""
Test the construction and management of status objects
"""

# Standard
from datetime import datetime
import copy

# Third Party
import pytest

# First Party
import alog

# Local
from oper8 import status
from oper8.dag import CompletionState, Node
from oper8.status import ReadyReason, ServiceStatus, UpdatingReason
from oper8.test_helpers.helpers import configure_logging, library_config
from oper8.utils import nested_get

configure_logging()

log = alog.use_channel("TEST")

## Helpers #####################################################################


def strip_timestamps(res):
    for entry in res.get("conditions", []):
        if entry["type"] in [status.READY_CONDITION, status.UPDATING_CONDITION]:
            assert "lastTransactionTime" in entry
        if "lastTransactionTime" in entry:
            del entry["lastTransactionTime"]
    return res


## make_application_status #####################################################


def test_make_application_status_empty():
    """Make sure only specified status elements are present in the output"""
    res = status.make_application_status()
    assert res == {"conditions": []}


def test_make_application_status_partial():
    """Make sure a subset of the conditions can be given"""
    res = status.make_application_status(ready_reason=status.ReadyReason.STABLE)
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.STABLE.value,
                "status": "True",
                "message": "",
            }
        ]
    }


def test_make_application_status_reason_all_args():
    """Make sure that all args make it through to the output"""
    version = "1.0.0"
    res = status.make_application_status(
        ready_reason=status.ReadyReason.INITIALIZING,
        ready_message="Ready Message",
        updating_reason=status.UpdatingReason.PRECONDITION_WAIT,
        updating_message="Updating Message",
        version=version,
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.INITIALIZING.value,
                "status": "False",
                "message": "Ready Message",
            },
            {
                "type": status.UPDATING_CONDITION,
                "reason": status.UpdatingReason.PRECONDITION_WAIT.value,
                "status": "True",
                "message": "Updating Message",
            },
        ],
        "versions": {"reconciled": version},
    }


def test_make_application_status_reason_str():
    """Make sure that a reason argument can be given as a string"""
    res = status.make_application_status(
        ready_reason=status.ReadyReason.INITIALIZING.value,
        updating_reason=status.UpdatingReason.PRECONDITION_WAIT.value,
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.INITIALIZING.value,
                "status": "False",
                "message": "",
            },
            {
                "type": status.UPDATING_CONDITION,
                "reason": status.UpdatingReason.PRECONDITION_WAIT.value,
                "status": "True",
                "message": "",
            },
        ]
    }


def test_make_application_status_invalid_reason_enum():
    """Make sure that an invalid reason enum value throws an appropriate error"""
    with pytest.raises(ValueError):
        status.make_application_status(ready_reason="foobar")


def test_make_application_status_component_state():
    """Make sure that component_state gets correctly incorporated into status"""
    res = status.make_application_status(
        component_state=CompletionState(
            verified_nodes=[Node("B"), Node("A")],
            unverified_nodes=[Node("C")],
            failed_nodes=[Node("D")],
            unstarted_nodes=[Node("E")],
        )
    )
    assert res == {
        "conditions": [],
        status.COMPONENT_STATUS: {
            status.COMPONENT_STATUS_ALL_NODES: ["A", "B", "C", "D", "E"],
            status.COMPONENT_STATUS_DEPLOYED_NODES: ["A", "B", "C"],
            status.COMPONENT_STATUS_UNVERIFIED_NODES: ["C"],
            status.COMPONENT_STATUS_FAILED_NODES: ["D"],
            status.COMPONENT_STATUS_DEPLOYED_COUNT: "3/5",
            status.COMPONENT_STATUS_VERIFIED_COUNT: "2/5",
        },
    }


def test_make_application_status_supported_versions():
    """Make sure that setting supported versions in status creates the correct
    status.versions entries
    """
    versions = ["one", "two", "3", "v4.0.0"]
    res = status.make_application_status(supported_versions=versions)
    assert nested_get(res, status.VERSIONS_FIELD_AVAILABLE_VERSIONS) == [
        {"name": version} for version in versions
    ]


@pytest.mark.parametrize(
    ["ready_reason", "updating_reason", "service_status"],
    [
        [
            ReadyReason.STABLE,
            UpdatingReason.STABLE,
            ServiceStatus.COMPLETED,
        ],
        [
            ReadyReason.STABLE.value,
            UpdatingReason.STABLE.value,
            ServiceStatus.COMPLETED,
        ],  # make sure string type reason value is handled correctly
        [
            ReadyReason.INITIALIZING,
            UpdatingReason.VERIFY_WAIT,
            ServiceStatus.IN_PROGRESS,
        ],  # waiting install completion scenario
        [
            ReadyReason.STABLE,
            UpdatingReason.VERSION_CHANGE,
            ServiceStatus.IN_PROGRESS,
        ],  # upgrade scenario
        [
            ReadyReason.INITIALIZING,
            UpdatingReason.ERRORED,
            ServiceStatus.FAILED,
        ],  # error scenario
        [
            ReadyReason.ERRORED,
            UpdatingReason.VERIFY_WAIT,
            ServiceStatus.FAILED,
        ],  # error scenario
        [
            None,
            None,
            ServiceStatus.IN_PROGRESS,
        ],  # starting reconciliation
    ],
)
def test_make_application_status_service_status(
    ready_reason, updating_reason, service_status
):
    """Make sure service status is correctly set based on current ready/updating status"""
    kind = "SampleService"
    res = status.make_application_status(
        ready_reason=ready_reason, updating_reason=updating_reason, kind=kind
    )

    assert res.get("sampleServiceStatus", "") == service_status.value


def test_make_application_status_operator_version():
    """Make sure that setting operator version in status creates the correct
    status.versions entry
    """
    operator_version = "1.2.3"
    res = status.make_application_status(operator_version=operator_version)
    assert nested_get(res, status.OPERATOR_VERSION) == "1.2.3"


def test_make_application_status_no_operator_version():
    """Make sure that None operator version does not error"""
    operator_version = None
    res = status.make_application_status(operator_version=operator_version)


## update_application_status ###################################################


def test_update_application_status_empty_current():
    """Make sure an empty current_status dict is handled by using the defaults
    from make_application_status
    """
    res = status.update_application_status(
        current_status={},
        ready_reason=status.ReadyReason.STABLE,
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.STABLE.value,
                "status": "True",
                "message": "",
            }
        ]
    }


def test_update_application_status_no_current_override():
    """Make sure a value in current_status without an override ends up in the
    output status
    """
    res = status.update_application_status(
        current_status=status.make_application_status(
            ready_reason=status.ReadyReason.STABLE, version="1.0.0"
        )
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.STABLE.value,
                "status": "True",
                "message": "",
            }
        ],
        "versions": {"reconciled": "1.0.0"},
    }


def test_update_application_status_with_current_override():
    """Make sure a value in current_status and an override kwarg uses the
    override
    """
    res = status.update_application_status(
        current_status=status.make_application_status(
            ready_reason=status.ReadyReason.STABLE
        ),
        ready_reason=status.ReadyReason.ERRORED,
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.ERRORED.value,
                "status": "False",
                "message": "",
            }
        ]
    }


def test_update_application_status_with_all_current():
    """Make sure that when the status has all entries, they all pass through or
    are overridden as expected
    """
    res = status.update_application_status(
        current_status=status.make_application_status(
            ready_reason=status.ReadyReason.STABLE,
            updating_reason=status.UpdatingReason.PRECONDITION_WAIT,
            version="1.0.0",
        ),
        ready_reason=status.ReadyReason.ERRORED,
        updating_reason=status.UpdatingReason.ERRORED,
        version="2.0.0",
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.ERRORED.value,
                "status": "False",
                "message": "",
            },
            {
                "type": status.UPDATING_CONDITION,
                "reason": status.UpdatingReason.ERRORED.value,
                "status": "False",
                "message": "",
            },
        ],
        "versions": {
            "reconciled": "2.0.0",
        },
    }


def test_update_application_status_external_conditions():
    """Make sure that conditions managed by other parts of the operator are
    preserved when updating status.
    """
    external_condition = {"type": "Foo", "bar": "baz"}
    res = status.update_application_status(
        current_status={"conditions": [external_condition]},
        ready_reason=status.ReadyReason.ERRORED,
    )
    assert strip_timestamps(res) == {
        "conditions": [
            {
                "type": status.READY_CONDITION,
                "reason": status.ReadyReason.ERRORED.value,
                "status": "False",
                "message": "",
            },
            external_condition,
        ]
    }


def test_update_application_status_external_status():
    """Make sure that non-condition status elements managed by other parts of
    the operator are preserved when updating status.
    """
    external_status = {"foo": "bar", "baz": "bat"}
    res = status.update_application_status(
        current_status=external_status,
        ready_reason=status.ReadyReason.ERRORED,
    )
    expected = copy.copy(external_status)
    expected["conditions"] = [
        {
            "type": status.READY_CONDITION,
            "reason": status.ReadyReason.ERRORED.value,
            "status": "False",
            "message": "",
        },
    ]

    assert strip_timestamps(res) == expected


def test_update_application_status_invalid_kwarg():
    """Make sure an invalid kwarg throws an error"""
    with pytest.raises(TypeError):
        res = status.update_application_status(current_status={}, foo="bar")


def test_update_application_status_no_mutation():
    """Make sure that the original status object is not mutated"""
    current_status = status.make_application_status(
        ready_reason=status.ReadyReason.STABLE,
        version="old version",
    )
    current_status_copy = copy.deepcopy(current_status)
    status.update_application_status(
        current_status=current_status,
        ready_reason=status.ReadyReason.ERRORED,
        version="new version",
    )
    assert current_status == current_status_copy


def test_update_application_status_supported_versions_not_changed():
    """Make sure that updating a status object which contains supported versions
    does not remove them
    """
    versions = ["one", "two", "3", "v4.0.0"]
    initial_status = status.make_application_status(supported_versions=versions)
    updated_status = status.update_application_status(
        current_status=initial_status, ready_reason=status.ReadyReason.STABLE
    )
    assert nested_get(updated_status, status.VERSIONS_FIELD_AVAILABLE_VERSIONS) == [
        {"name": version} for version in versions
    ]


def test_update_application_status_operator_version():
    """Make sure that the non-null config.operator_version is pulled correctly"""
    with library_config(operator_version="1.2.3"):
        updated_cfg = status.update_application_status({})
    assert nested_get(updated_cfg, status.OPERATOR_VERSION) == "1.2.3"


def test_update_application_status_no_operator_version():
    """Make sure that the null config.operator_version and null status.version does not error or display"""
    with library_config(operator_version=None):
        updated_cfg = status.update_application_status({})
    assert status.OPERATOR_VERSION.split(".")[0] not in updated_cfg


def test_update_application_status_version_no_operator_version():
    """Make sure that the null config.operator_version does not error or display in non-null status.version"""
    with library_config(operator_version=None):
        updated_cfg = status.update_application_status(
            current_status={}, version="1.2.3"
        )
    assert status.OPERATOR_VERSION not in updated_cfg


## status_changed ##############################################################


def test_status_changed_different_types():
    """Make sure that if the statuses are different types, a change is indicated"""
    assert status.status_changed(1, "two")
    assert status.status_changed(None, {})


def test_status_changed_different_non_dicts():
    """Make sure that if the statuses are not dicts, a change is indicated"""
    assert status.status_changed("one", "one")


def test_status_changed_no_conditions():
    """Make sure that without conditions, a deep-equal is used"""
    ts_a = datetime.now().isoformat()
    ts_b = datetime.now().isoformat()
    assert not status.status_changed({"a": 1, "b": ts_a}, {"a": 1, "b": ts_a})
    assert status.status_changed({"a": 1, "b": ts_a}, {"a": 1, "b": ts_b})


def test_status_changed_no_conditions_cur():
    """Make sure that with conditions in the new status, but not in the current,
    a change is indicated
    """
    assert status.status_changed({}, {"conditions": []})


def test_status_changed_conditions_different_len():
    """Make sure that with differnt length condition lists, a change is
    indicated
    """
    assert status.status_changed(
        {"conditions": [{"type": "a"}]},
        {"conditions": [{"type": "a"}, {"type": "b"}]},
    )


def test_status_changed_condition_type_mismatch():
    """Make sure that if the elements of a conditions list have different types
    change is indicated
    """
    assert status.status_changed(
        {"conditions": [None]},
        {"conditions": [{"type": "a"}]},
    )


def test_status_changed_conditions_match_different_timestamps():
    """Make sure that if the conditions match but have different timestamps, a
    change is not indicated
    """
    current_status = status.make_application_status(
        ready_reason=status.ReadyReason.STABLE
    )
    new_status = status.make_application_status(ready_reason=status.ReadyReason.STABLE)
    assert current_status != new_status
    assert not status.status_changed(current_status, new_status)


def test_status_changed_conditions_mismatch_different_values():
    """Make sure that if the conditions have different values, a change is
    indicated
    """
    current_status = status.make_application_status(
        ready_reason=status.ReadyReason.STABLE
    )
    new_status = status.make_application_status(ready_reason=status.ReadyReason.ERRORED)
    assert status.status_changed(current_status, new_status)


def test_status_changed_non_list_conditions_error():
    """Make sure that if conditions are not a list, we raise an error"""
    with pytest.raises(AssertionError):
        assert status.status_changed(
            {"conditions": {}},
            {"conditions": {}},
        )


def test_status_changed_condition_type_non_dict():
    """Make sure that if the elements of a conditions list are not dicts, an
    error is thrown
    """
    with pytest.raises(AssertionError):
        assert status.status_changed(
            {"conditions": [1]},
            {"conditions": [1]},
        )


## get_condition ###############################################################


def test_get_condition_when_present():
    """Make sure get_condition returns a condition when it is found"""
    st = status.make_application_status(ready_reason=status.ReadyReason.STABLE)
    assert status.get_condition(status.READY_CONDITION, strip_timestamps(st)) == {
        "type": status.READY_CONDITION,
        "reason": status.ReadyReason.STABLE.value,
        "status": "True",
        "message": "",
    }


def test_get_condition_when_missing():
    """Make sure get_condition returns an empty dict when the requested
    condition is missing
    """
    st = status.make_application_status(ready_reason=status.ReadyReason.STABLE)
    assert status.get_condition(status.UPDATING_CONDITION, st) == {}


def test_get_condition_no_conditions():
    """Make sure get_condition returns an empty dict when the current status is
    missing the conditions section
    """
    assert status.get_condition(status.UPDATING_CONDITION, {"foo": "bar"}) == {}


## get_version ###############################################################


@pytest.mark.parametrize(["version"], [["1.0.0"], [None]])
def test_get_version(version):
    """Make sure current verified version can be obtained"""
    st = status.make_application_status(version=version)
    assert version == status.get_version(st)


## Status Mappings #############################################################


def check_status_map(condition_fn, status_map):
    for reason, status in status_map.items():
        cond = condition_fn(
            reason=reason, message="", last_transaction_time=datetime.now()
        )
        assert str(status) == cond["status"]


def test_ready_status_map():
    """Make sure the status bools are set correctly for each ReadyReason"""
    check_status_map(
        status._make_ready_condition,
        {
            status.ReadyReason.INITIALIZING: False,
            status.ReadyReason.STABLE: True,
            status.ReadyReason.CONFIG_ERROR: False,
            status.ReadyReason.ERRORED: False,
        },
    )


def test_updating_status_map():
    """Make sure the status bools are set correctly for each UpdatingReason"""
    check_status_map(
        status._make_updating_condition,
        {
            status.UpdatingReason.STABLE: False,
            status.UpdatingReason.PRECONDITION_WAIT: True,
            status.UpdatingReason.VERIFY_WAIT: True,
            status.UpdatingReason.CLUSTER_ERROR: False,
            status.UpdatingReason.ERRORED: False,
        },
    )
