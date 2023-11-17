"""
Tests for the Leadership common functions
"""
# Third Party
import pytest

# Local
from oper8.test_helpers.helpers import library_config
from oper8.watch_manager.python_watch_manager.leader_election import (
    AnnotationLeadershipManager,
    DryRunLeadershipManager,
    LeaderForLifeManager,
    LeaderWithLeaseManager,
    get_leader_election_class,
)

## Helpers #####################################################################


@pytest.mark.parametrize(
    ["config", "expected_class"],
    [
        ["leader-for-life", LeaderForLifeManager],
        ["leader-with-lease", LeaderWithLeaseManager],
        ["annotation", AnnotationLeadershipManager],
        ["dryrun", DryRunLeadershipManager],
    ],
)
def test_get_leader_election_class(config, expected_class):
    with library_config(python_watch_manager={"lock": {"type": config}}):
        assert get_leader_election_class() == expected_class
