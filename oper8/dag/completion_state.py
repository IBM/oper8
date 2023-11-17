"""
CompletionState holds info about how a DAG Runner completes
"""


# Standard
from typing import List, Optional

# First Party
import alog

# Local
from .node import Node

log = alog.use_channel("DAG")

## Completion state ##############################################################


class CompletionState:
    """
    This class holds the definition of a CompletionState which manages all
    the information about how the nodes in a rollout Runner terminated
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        verified_nodes: Optional[List[Node]] = None,
        unverified_nodes: Optional[List[Node]] = None,
        failed_nodes: Optional[List[Node]] = None,
        unstarted_nodes: Optional[List[Node]] = None,
        exception: Optional[Exception] = None,
    ):
        """Construct with each node set"""
        self.verified_nodes = set(verified_nodes or [])
        self.unverified_nodes = set(unverified_nodes or [])
        self.failed_nodes = set(failed_nodes or [])
        self.unstarted_nodes = set(unstarted_nodes or [])
        self.all_nodes = (
            self.verified_nodes.union(self.unverified_nodes)
            .union(self.failed_nodes)
            .union(self.unstarted_nodes)
        )
        self.exception = exception

        # Make sure the sets are not overlapping
        sets = [
            self.verified_nodes,
            self.unverified_nodes,
            self.failed_nodes,
            self.unstarted_nodes,
        ]
        for i, node_set_a in enumerate(sets):
            for j, node_set_b in enumerate(sets):
                if i != j:
                    assert not node_set_a.intersection(node_set_b), (
                        "Programming Error: "
                        + f"CompletionState constructed with overlapping sets: {str(self)}"
                    )

    def __str__(self):
        return "\n".join(
            [
                f"[NODES] {key}: {list(sorted(nodes))}"
                for key, nodes in [
                    ("Verified", [node.get_name() for node in self.verified_nodes]),
                    ("Unverified", [node.get_name() for node in self.unverified_nodes]),
                    ("Failed", [node.get_name() for node in self.failed_nodes]),
                    ("Unstarted", [node.get_name() for node in self.unstarted_nodes]),
                ]
            ]
            + [
                f"Exception: {self.exception}",
            ]
        )

    def __eq__(self, other: "CompletionState"):
        return (
            self.verified_nodes == other.verified_nodes
            and self.unverified_nodes == other.unverified_nodes
            and self.failed_nodes == other.failed_nodes
            and self.unstarted_nodes == other.unstarted_nodes
        )

    def deploy_completed(self) -> bool:
        """Determine if the dag completed all nodes through to the deploy
        step

        NOTE: An empty node set is considered completed

        Returns:
            completed:  bool
                True if there are no failed nodes and no unstarted nodes
        """
        return not self.failed_nodes and not self.unstarted_nodes

    def verify_completed(self) -> bool:
        """Determine if the dag completed all nodes through to the verification
        step

        NOTE: An empty node set is considered verified

        Returns:
            completed:  bool
                True if there are no nodes found outside of the verified_nodes
                and there is no exception in the termination state
        """
        return (
            not self.unverified_nodes
            and not self.failed_nodes
            and not self.unstarted_nodes
            and not self.exception
        )

    def failed(self) -> bool:
        """Determine if any of the nodes failed

        Returns:
            failed:  bool
                True if there are any nodes in the failed state or there is a
                fatal error
        """
        return bool(self.failed_nodes) or self._fatal_exception()

    def _fatal_exception(self):
        """Helper to determine if there is a fatal exception in the state"""
        return self.exception is not None and getattr(
            self.exception, "is_fatal_error", True
        )
