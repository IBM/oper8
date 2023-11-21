"""
Test the DAG CompletionState class
"""

# First Party
import alog

# Local
from oper8.dag import CompletionState, Node

log = alog.use_channel("TEST")

################################################################################
## Completion State #######################################################################
################################################################################


def test_dag_completion_state_str():
    """Coverage test to ensure that stringifying a CompletionState doesn't
    throw! Coverage :)
    """
    log.debug(
        str(
            CompletionState(
                verified_nodes=[Node("A")],
                unverified_nodes=[Node("B")],
                failed_nodes=[Node("C")],
                unstarted_nodes=[Node("D")],
            )
        )
    )
