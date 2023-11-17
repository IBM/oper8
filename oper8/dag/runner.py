"""
This module contains a collection of classes for executing functions along a DAG
"""


# Standard
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import Callable, List, Optional
import time

# First Party
import alog

# Local
from .completion_state import CompletionState
from .graph import Graph
from .node import Node

log = alog.use_channel("Runner")

## Executer ##############################################################


class Runner:  # pylint: disable=too-many-instance-attributes
    """This is a very simple "keep running until done" Runner executor which uses
    a ThreadPoolExecutor to allow non-blocking calls to execute in parallel.
    """

    @property
    def graph(self) -> str:  # pylint: disable=missing-function-docstring
        return self._graph

    def __init__(  # pylint: disable=too-many-arguments
        self,
        name: str = "",
        threads: Optional[int] = None,
        graph: Optional[Graph] = None,
        default_function: Optional[Callable[["Node"], bool]] = None,
        poll_time: float = 0.05,
        verify_upstream: bool = True,
    ):
        """Construct a Runner which will manage the execution of a single Graph

        Args:
            name:  str
                String name that can be used for logging to differentiate deploy
                and verify Graph executions
            threads:  Optional[int]
                Number of threads to use. If not given, the default behavior of
                ThreadPoolExecutor is to use the number of available cores.
            graph:  Optional[Graph]
                Existing graph to use, if not supplied an empty graph is created
            default_function: Optional[Callable[["Node"],None]]=None
                Function that will be called on node run if specific function is not provided
            poll_time:  float
                How often to check runner status
            verify_upstream: bool
                Conditional to control whether to check a nodes upstream via its edge function
        """
        self.name = name
        # If threads are disabled, use the NonThreadPoolExecutor
        if threads == 0:
            log.debug("Running without threading")
            pool_type = NonThreadPoolExecutor
        else:
            log.debug("Running with %s threads", threads)
            pool_type = ThreadPoolExecutor
        self._pool = pool_type(max_workers=threads)
        self._graph = graph or Graph()
        self._default_node_func = default_function or (lambda _: None)

        self._failed = False
        self._exception = None
        self._verify_upstream = verify_upstream
        self._poll_time = poll_time
        self._started_nodes = []
        self._disabled_nodes = []

        # Nodes can terminate in one of three states:
        #   1. Completed and verified
        #   2. Completed, but not verified
        #   3. Failed
        self._verified_nodes = []
        self._unverified_nodes = []
        self._failed_nodes = []

    ## Public ##

    def disable_node(
        self,
        node: "Node",
    ):
        """Function to disable a node in the graph. This will skip the node in runner without
        changing the graph"""
        graph_node = self.graph.get_node(node.get_name())
        if graph_node:
            self._disabled_nodes.append(graph_node)

    def enable_node(
        self,
        node: "Node",
    ):
        """Function to reenable a node after it was disabled by Runner.disable_node"""
        graph_node = self.graph.get_node(node.get_name())
        if graph_node in self._disabled_nodes:
            self._disabled_nodes.remove(graph_node)

    def completion_state(self):
        """Get the state of which nodes completed and which failed

        Returns:
            completion_state:  CompletionState
                The state holding the full view of the termination state of each
                node
        """
        return CompletionState(
            verified_nodes=self._verified_nodes,
            unverified_nodes=self._unverified_nodes,
            failed_nodes=self._failed_nodes,
            unstarted_nodes=[
                node
                for node in self.graph.get_all_nodes()
                if node not in self._get_completed_nodes()
            ],
            exception=self._exception,
        )

    def run(self):
        """Run the Runner! This will continue until the graph has run to completion
        or halted due to an error.
        """
        node_list = self._get_runnable_nodes()
        log.debug3(
            "Started Nodes: %s, All Nodes: %s",
            self._started_nodes,
            list(node_list),
        )

        # The "node start" loop should terminate if:
        # 1. All nodes have started
        # 2. All started nodes have completed in one form or another and there
        #   are no newly ready nodes
        while len(self._started_nodes) < len(node_list):
            # Get the set of nodes that has completed already
            #
            # NOTE: It's _critically_ important that this be done before getting
            #   the ready nodes. The operation of getting ready nodes can
            #   delegate to user-defined verification functions which may be
            #   very slow and IO bound. With slow verification functions, a node
            #   running in a thread may complete and mark itself verified after
            #   a downstream dependency has checked its completion status, but
            #   before the full set of _get_ready_nodes() checks has passed. If
            #   this happens and _get_completed_nodes() is called afterwards,
            #   the short-circuit logic below will think that all started nodes
            #   have completed and there are no ready nodes, thus terminating
            #   the Runner prematurely.
            completed_nodes = self._get_completed_nodes()

            # Get the currently ready nodes
            ready_nodes = self._get_ready_nodes()

            # If there are no ready nodes and all started nodes have completed
            # in one way or another, we're in an early termination case
            log.debug4("Ready Nodes: %s", ready_nodes)
            log.debug4("Completed Nodes: %s", completed_nodes)
            if not ready_nodes and set(self._started_nodes) == set(completed_nodes):
                log.debug2(
                    "[%s] Graph exhausted all available nodes. Terminating early.",
                    self.name,
                )
                break

            # If there are new ready nodes, start them
            if ready_nodes:
                log.debug2(
                    "Ready nodes: %s. Remaining nodes: %s",
                    ready_nodes,
                    [
                        node
                        for node in node_list
                        if node not in ready_nodes and node not in completed_nodes
                    ],
                )
            for ready_node in ready_nodes:
                self._started_nodes.append(ready_node)
                self._pool.submit(self._run_node, ready_node)
            time.sleep(self._poll_time)

        # Log out the state of the graph once we've terminated, but before we've
        # waited for all nodes to terminate
        log.debug2("[NODES] Started: %s", sorted(self._started_nodes))
        log.debug2("[NODES] Verified: %s", sorted(self._verified_nodes))
        log.debug2("[NODES] Unverified: %s", sorted(self._unverified_nodes))
        log.debug2("[NODES] Failed: %s", sorted(self._failed_nodes))
        log.debug2("[NODES] All: %s", sorted(list(node_list)))

        # Wait until all started nodes have finished one way or the other
        while len(self._get_completed_nodes()) != len(self._started_nodes):
            time.sleep(self._poll_time)

        # Make sure any in-flight nodes complete before terminating
        log.debug2("Waiting for in-flight nodes to complete")
        self._pool.shutdown()
        log.debug2("All nodes complete")
        log.debug2(self.completion_state())

    ## Implementation Details ##

    def _run_node(self, node: "Node"):
        node_name = node.get_name()
        log.debug2("Starting node: %s", node_name)

        try:
            # Call node function or default
            node_func = node.get_data()
            if callable(node_func):
                node_func()
            else:
                self._default_node_func(node)

        except DagHaltError as err:
            log.debug("[%s] DagHaltError caught. Stopping Execution", self.name)
            self._failed = err.failure
            self._exception = err.exception
            if err.failure:
                self._failed_nodes.append(node)
            else:
                self._unverified_nodes.append(node)
        except Exception as err:  # pylint: disable=broad-except
            log.warning(
                "Unexpected exception caught in Runner node: %s", err, exc_info=True
            )
            self._failed = True
            self._failed_nodes.append(node)
        else:
            log.debug2("Node complete: %s", node_name)
            self._verified_nodes.append(node)

    def _dependency_satisfied(self, dep: "Node", verify_fn: Callable = None) -> bool:
        # A dependency is satisfied if
        # a) The upstream has been deployed and no verification function is
        #       given for the dependency
        # b) The upstream has been deployed and the given verification
        #       function passes
        dep_name = dep.get_name()
        if dep not in self._verified_nodes:
            log.debug4("%s not yet verified", dep_name)
            return False

        if not self._verify_upstream:
            log.debug3("%s verified without checking", dep_name)
            return True
        if verify_fn is None:
            log.debug4("%s verified with no verify_fn", dep_name)
            return True

        log.debug4("%s calling verify_fn", dep_name)
        satisfied = verify_fn()
        log.debug4("%s verify_fn() -> %s", dep_name, satisfied)
        return satisfied

    def _get_ready_nodes(self) -> List[str]:
        ready_nodes = []
        for node in [
            n for n in self._get_runnable_nodes() if n not in self._started_nodes
        ]:
            node_name = node.get_name()
            log.debug4("Checking if %s is ready", node_name)
            node_deps = node.get_children()
            satisfied_dependencies = [
                (self._dependency_satisfied(dep, verify_fn), dep)
                for dep, verify_fn in node_deps
            ]
            if all(res[0] for res in satisfied_dependencies):
                ready_nodes.append(node)
            else:
                log.debug3(
                    "[%s] waiting on upstreams: %s",
                    node_name,
                    [res[0] for res in satisfied_dependencies if not res[0]],
                )
        return ready_nodes

    def _get_completed_nodes(self) -> List[str]:
        return self._verified_nodes + self._unverified_nodes + self._failed_nodes

    def _get_runnable_nodes(self) -> List[Node]:
        return set(self.graph.get_all_nodes()) - set(self._disabled_nodes)


## Helper Classes ##############################################################


class NonThreadPoolExecutor(Executor):
    """This "pool" implements the Executor interfaces, but runs without any
    threads. This is used when running a Runner without cocurrency
    """

    def __init__(self, *_, **__):
        """Swallow constructor args so that it can match ThreadPoolExecutor"""
        super().__init__()

    @staticmethod
    def submit(fn: Callable, /, *args, **kwargs):
        """Run the function immediately and return a pre-completed Future"""
        fut = Future()
        fut.set_result(fn(*args, **kwargs))
        return fut

    @staticmethod
    def shutdown(*_, **__):
        """Nothing to do since this is not a real pool"""


class DagHaltError(Exception):
    """Custom exception used to indicate that a Runner execution should halt"""

    def __init__(
        self,
        failure: bool,
        exception: Exception = None,
    ):
        super().__init__()
        self.failure = failure
        self.exception = exception
