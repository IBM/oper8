"""
Test the DAG Runner functionality
"""

# Standard
import time

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.dag import CompletionState, DagHaltError, Graph, Node, Runner

log = alog.use_channel("TEST")

################################################################################
## Runner Tests #############################################################
################################################################################


class TestRunner:
    """Test the DAG runner directly"""

    #################
    ## Happy Paths ##
    #################

    @pytest.mark.parametrize("threads", [0, 1, 2])
    def test_happy_path(self, threads):
        """Make sure that a simple DAG with dependencies completes and that
        order is correctly preserved
        """

        # Set up nodes in a diamond DAG
        #
        #    A
        #   / \
        #  B  C
        #  \ /
        #   D
        end_order = []

        def node_a():
            end_order.append("A")

        def node_b():
            end_order.append("B")

        def node_c():
            end_order.append("C")

        def node_d():
            end_order.append("D")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node(Node("D", node_d))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("A"))
        graph.add_node_dependency(Node("D"), Node("B"))
        graph.add_node_dependency(Node("D"), Node("C"))

        # Run it
        runner = Runner(threads=threads, graph=graph)
        runner.run()

        # Make sure A came before B and C and D can last
        assert len(end_order) == 4
        assert end_order[0] == "A"
        assert end_order[-1] == "D"
        assert "B" in end_order
        assert "C" in end_order

    def test_multi_root(self):
        """Make sure that a DAG with multiple roots executes to completion"""

        # Set up nodes with multiple roots
        #
        #    A B
        #      |
        #      C
        end_order = []

        def node_a():
            end_order.append("A")

        def node_b():
            end_order.append("B")

        def node_c():
            end_order.append("C")

        graph = Graph()
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("C", node_c))
        graph.add_node_dependency(Node("C"), Node("B"))

        # Run it
        runner = Runner(graph=graph)
        runner.run()

        # Make sure C came after B
        assert len(end_order) == 3
        assert "A" in end_order
        assert "B" in end_order
        assert "C" in end_order
        assert end_order.index("C") > end_order.index("B")

    def test_slow_nodes(self):
        """Make sure that a DAG with slow nodes that have GIL releases will
        still execute in the correct order
        """

        # Set up nodes where one is slow and has a sleep
        #
        #    A
        #   / \
        #  B  C (slow)
        #  |  |
        #  D  E (slow)
        start_order = []
        end_order = []

        def node_a():
            start_order.append("A")
            end_order.append("A")

        def node_b():
            start_order.append("B")
            end_order.append("B")

        def node_c():
            start_order.append("C")
            time.sleep(0.5)
            end_order.append("C")

        def node_d():
            start_order.append("D")
            end_order.append("D")

        def node_e():
            start_order.append("E")
            time.sleep(0.1)
            end_order.append("E")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node(Node("D", node_d))
        graph.add_node(Node("E", node_e))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("A"))
        graph.add_node_dependency(Node("D"), Node("B"))
        graph.add_node_dependency(Node("E"), Node("C"))

        # Run it
        runner = Runner(graph=graph)
        runner.run()
        assert len(start_order) == 5
        assert len(end_order) == 5

        # A start/end before any others
        assert start_order[0] == "A"
        assert end_order[0] == "A"

        # C started before D, but ended after D
        c_start_idx = start_order.index("C")
        c_end_idx = end_order.index("C")
        d_start_idx = start_order.index("D")
        d_end_idx = end_order.index("D")
        assert c_start_idx < d_start_idx
        assert c_end_idx > d_end_idx

        # E start/end after C
        e_start_idx = start_order.index("E")
        e_end_idx = end_order.index("E")
        assert c_start_idx < e_start_idx
        assert c_end_idx < e_end_idx

    def test_incomplete_node(self):
        """Make sure that if a node throws a non-failing DagHaltError, all other
        nodes that can be run are run to completion.
        """

        # Set a graph with an intermediate node that will throw a non-failing
        # DagHaltError
        #
        #    A
        #   / \
        #  B [C]
        #  |  |
        #  D  E
        end_order = []

        def node_a():
            end_order.append("A")

        def node_b():
            end_order.append("B")

        def node_c():
            raise DagHaltError(failure=False)

        def node_d():
            end_order.append("D")

        def node_e():
            end_order.append("E")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node(Node("D", node_d))
        graph.add_node(Node("E", node_e))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("A"))
        graph.add_node_dependency(Node("D"), Node("B"))
        graph.add_node_dependency(Node("E"), Node("C"))

        # Run it
        runner = Runner(graph=graph)
        runner.run()

        # Make sure the completion order looks like A, B, D
        assert end_order == ["A", "B", "D"]

        # Make sure that the runner indicates an incomplete termination state
        completion_state = runner.completion_state()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        # Make sure that the completion state looks correct
        assert completion_state == CompletionState(
            verified_nodes=[Node("A"), Node("B"), Node("D")],
            unverified_nodes=[Node("C")],
            failed_nodes=[],
            unstarted_nodes=[Node("E")],
        )

    def test_custom_verify_pass(self):
        """Make sure that giving a dependency a custom verify function which
        passes allows the graph to proceed to a dependent node
        """
        # Set up a simple graph with a custom verify on the one edge
        #
        #    A
        #   [|]
        #    B
        def empty_node():
            pass

        def custom_verify():
            log.debug("Custom verify called!")
            return True

        graph = Graph()
        graph.add_node(Node("A", empty_node))
        graph.add_node(Node("B", empty_node))
        graph.add_node_dependency(Node("B"), Node("A"), custom_verify)

        # Run it
        runner = Runner(graph=graph)
        runner.run()

        # Make sure that the runner indicates full completion
        completion_state = runner.completion_state()
        assert completion_state.verify_completed()
        assert not completion_state.failed()

        # Make sure that the completion state looks correct
        assert completion_state == CompletionState(
            verified_nodes=[Node("A"), Node("B")],
            unverified_nodes=[],
            failed_nodes=[],
            unstarted_nodes=[],
        )

    def test_custom_verify_fail(self):
        """Make sure that giving a dependency a custom verify function which
        fails stops the graph from proceeding to the dependent node
        """

        # Set up a simple graph with a custom verify on the one edge
        #
        #    A
        #   [|]
        #    B
        def empty_node():
            pass

        def custom_verify():
            log.debug("Custom verify called!")
            return False

        graph = Graph()
        graph.add_node(Node("A", empty_node))
        graph.add_node(Node("B", empty_node))
        graph.add_node_dependency(Node("B"), Node("A"), custom_verify)

        # Run it
        runner = Runner(graph=graph)
        runner.run()

        # Make sure that the runner indicates incomplete termination
        completion_state = runner.completion_state()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        # Make sure that the completion state looks correct
        assert completion_state == CompletionState(
            verified_nodes=[Node("A")],
            unverified_nodes=[],
            failed_nodes=[],
            unstarted_nodes=[Node("B")],
        )

    def test_custom_verify_both(self):
        """Make sure that two nodes can depend on the same upstream but use
        different verification functions
        """

        # Set up a graph with two nodes off of the root, one of which has a
        # custom verify that passes and the other of which has a custom verify
        # that fails.
        #
        #    A
        #   /[\]
        #  B  C
        def empty_node():
            pass

        def custom_verify_fail():
            log.debug("Custom verify FAIL called!")
            return False

        def custom_verify_pass():
            log.debug("Custom verify PASS called!")
            return True

        graph = Graph()
        graph.add_node(Node("A", empty_node))
        graph.add_node(Node("B", empty_node))
        graph.add_node(Node("C", empty_node))
        graph.add_node_dependency(Node("B"), Node("A"), custom_verify_pass)
        graph.add_node_dependency(Node("C"), Node("A"), custom_verify_fail)

        # Run it
        runner = Runner(graph=graph)
        runner.run()

        # Make sure that the runner indicates incomplete termination
        completion_state = runner.completion_state()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        # Make sure that the completion state looks correct
        assert completion_state == CompletionState(
            verified_nodes=[Node("A"), Node("B")],
            unverified_nodes=[],
            failed_nodes=[],
            unstarted_nodes=[Node("C")],
        )

    def test_disable_nodes(self):
        # Set up nodes in a diamond DAG
        #
        #    A
        #   / \
        #  B  C
        #  \ /
        #   D
        end_order = []

        def node_a():
            end_order.append("A")

        def node_b():
            end_order.append("B")

        def node_c():
            end_order.append("C")

        def node_d():
            end_order.append("D")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node(Node("D", node_d))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("A"))
        graph.add_node_dependency(Node("D"), Node("B"))
        graph.add_node_dependency(Node("D"), Node("C"))

        # Disable node D

        # Run it
        runner = Runner(graph=graph)
        runner.disable_node(Node("D"))
        runner.run()

        # Make sure only three nodes ran
        assert len(end_order) == 3
        assert end_order[0] == "A"
        assert "D" not in end_order

    def test_disable_reenable_nodes(self):
        # Set up nodes in a diamond DAG
        #
        #    A
        #   / \
        #  B  C
        #  \ /
        #   D
        end_order = []

        def node_a():
            end_order.append("A")

        def node_b():
            end_order.append("B")

        def node_c():
            end_order.append("C")

        def node_d():
            end_order.append("D")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node(Node("D", node_d))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("A"))
        graph.add_node_dependency(Node("D"), Node("B"))
        graph.add_node_dependency(Node("D"), Node("C"))

        # Disable node D

        # Run it
        runner = Runner(graph=graph)
        runner.disable_node(Node("D"))
        runner.enable_node(Node("D"))
        runner.run()

        assert len(end_order) == 4
        assert end_order[0] == "A"
        assert "D" in end_order

    #################
    ## Error Cases ##
    #################

    def test_node_throw_dag(self):
        """Make sure that if a node raises a DagHaltError, the DAG nodes before
        it still completed but nodes after it do not
        """

        # Set up linear nodes with a throw in the middle
        #
        #    A -> B -x C
        start_order = []
        end_order = []

        def node_a():
            start_order.append("A")
            end_order.append("A")

        def node_b():
            start_order.append("B")
            raise DagHaltError()

        def node_c():
            start_order.append("C")
            end_order.append("C")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("B"))

        # Run it
        runner = Runner(graph=graph)
        runner.run()
        assert start_order == ["A", "B"]
        assert end_order == ["A"]

    def test_node_throw_runtime(self):
        """Make sure that if a node throws, the DAG nodes before it still
        completed but nodes after it do not
        """

        # Set up linear nodes with a throw in the middle
        #
        #    A -> B -x C
        start_order = []
        end_order = []

        def node_a():
            start_order.append("A")
            end_order.append("A")

        def node_b():
            start_order.append("B")
            raise RuntimeError("Nope!")

        def node_c():
            start_order.append("C")
            end_order.append("C")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))
        graph.add_node(Node("C", node_c))
        graph.add_node_dependency(Node("B"), Node("A"))
        graph.add_node_dependency(Node("C"), Node("B"))

        # Run it
        runner = Runner(graph=graph)
        runner.run()
        assert start_order == ["A", "B"]
        assert end_order == ["A"]

    def test_node_in_flight(self):
        """Make sure that an in-flight node completes when another node causes a
        halt in execution.
        """

        # Set up two independent nodes, one of which will terminate and the
        # the other of which takes a while
        start_order = []
        end_order = []

        def node_a():
            start_order.append("A")
            time.sleep(0.1)
            end_order.append("A")

        def node_b():
            time.sleep(0.01)
            start_order.append("B")
            raise RuntimeError("Nope!")

        graph = Graph()
        graph.add_node(Node("A", node_a))
        graph.add_node(Node("B", node_b))

        # Run it and make sure A makes it to the end
        runner = Runner(graph=graph)
        runner.run()
        assert start_order == ["A", "B"]
        assert end_order == ["A"]

    def test_dag_early_halt(self):
        """This test exercises a nasty corner case that came up in WA when the
        CLU subsystem introduced custom verify_functions for all components. The
        bug is:

        With slow verification functions, a node running in a thread
        may complete and mark itself verified after a downstream dependency has
        checked its completion status, but before the full set of
        _get_ready_nodes() checks has passed. If this happens and
        _get_completed_nodes() is called afterwards, the short-circuit logic
        below will think that all started nodes have completed and there are no
        ready nodes, thus terminating the DAG prematurely.
        """

        class NodeTest(Node):
            def __init__(
                self, name, call_sleep=0.1, verify_sleep=0.1, verify_fail_times=0
            ):
                super().__init__(name, self)
                self.name = name
                self.call_sleep = call_sleep
                self.verify_sleep = verify_sleep
                self.verify_fail_times = verify_fail_times
                self.verify_call = 0

            def __repr__(self):
                return f"Node[{self.name}]"

            def __call__(self):
                log.info("Calling %s", self)
                time.sleep(self.call_sleep)
                log.info("Done calling %s", self)

            def verify(self):
                time.sleep(self.verify_sleep)
                if self.verify_call < self.verify_fail_times:
                    self.verify_call += 1
                    return False
                return True

        slow_call_parent = NodeTest("A", call_sleep=0.1, verify_sleep=0)
        slow_verify_parent = NodeTest(
            "B", call_sleep=0, verify_sleep=0.1, verify_fail_times=1
        )
        child_c = NodeTest("C")
        child_d = NodeTest("D")

        graph = Graph()
        graph.add_node(slow_call_parent)
        graph.add_node(slow_verify_parent)
        graph.add_node(child_c)
        graph.add_node(child_d)
        graph.add_node_dependency(child_c, slow_call_parent, slow_call_parent.verify)
        graph.add_node_dependency(
            child_d, slow_verify_parent, slow_verify_parent.verify
        )
        runner = Runner("test", graph=graph)
        runner.run()
        completion_state = runner.completion_state()
        assert completion_state.verify_completed()
