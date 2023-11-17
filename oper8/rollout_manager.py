"""
This module holds the implementation of the DAG constructs used to perform the
dependency management for rollout
"""

# Standard
from functools import partial
from typing import Callable, Optional

# First Party
import alog

# Local
from . import config
from .component import Component
from .dag import CompletionState, DagHaltError, Runner
from .exceptions import Oper8Error, VerificationError
from .session import Session

log = alog.use_channel("ROLMGR")

## Rollout Functions ###########################################################


def deploy_component(session: Session, component: Component) -> bool:
    """Deploy a component given a particular session

    Args:
        session: Session
            The current rollout session
        component: Component
            The component to deploy

    Returns:
        result: bool
            The result of the deploy
    """
    # Do the render
    with alog.ContextTimer(log.debug2, "Render duration for %s", component):
        component.render_chart(session)
        log.debug3(
            "Rendered objects for [%s]: %s",
            component,
            [str(obj) for obj in component.managed_objects],
        )

    # Do the deploy
    with alog.ContextTimer(log.debug2, "Deploy duration for %s: ", component):
        return component.deploy(session)


def disable_component(session: Session, component: Component) -> bool:
    """Disable a component given a particular session

    Args:
        session: Session
            The current rollout session
        component: Component
            The component to disable

    Returns:
        result: bool
            The result of the disable
    """
    # Do the render
    with alog.ContextTimer(log.debug2, "Render duration for %s", component):
        component.render_chart(session)
        log.debug3(
            "Rendered objects for [%s]: %s",
            component,
            [str(obj) for obj in component.managed_objects],
        )

    # Do the deploy
    with alog.ContextTimer(log.debug2, "Disable duration for %s: ", component):
        return component.disable(session)


def verify_component(session: Session, component: Component) -> bool:
    """Verify a component given a particular session

    Args:
        session: Session
            The current rollout session
        component: Component
            The component to verify

    Returns:
        result: bool
            The result of the verify
    """
    # Do the verify
    with alog.ContextTimer(log.debug2, "Verify duration for %s: ", component):
        return component.verify(session)


## RolloutManager ##############################################################


class RolloutManager:
    """This class manages the dependencies needed to roll out a set of nodes"""

    @classmethod
    def run_node(
        cls,
        func: Callable[[Component, Session], bool],
        session: Session,
        component: Component,
        fail_halt_runner=True,
    ):
        """
        Generic function to execute a node during Rollout

        Args:
            func: Callable[[Component,Session], bool]
                The function to be called
            session: Session
                The session that's currently being rolled out
            component: Component
                The component being rolled out
        """
        success = False
        exception = None
        rollout_failed = False

        try:
            success = func(session, component)
            if fail_halt_runner and not success:
                rollout_failed = True

        # If a failure occurred by throwing, treat that the same as an
        # explict failure.
        except Oper8Error as err:
            log.debug("Caught Oper8Error during rollout of [%s]", component)
            success = False
            rollout_failed = err.is_fatal_error
            exception = err
        except Exception as err:  # pylint: disable=broad-except
            log.warning(
                "Caught exception during rollout of [%s]",
                component,
                exc_info=True,
            )
            success = False
            rollout_failed = True
            exception = err

        # If the rollout failed for any reason, raise an exception. This
        # will halt the graph execution.
        if not success:
            log.debug("[deploy] Halting rollout")
            raise DagHaltError(rollout_failed, exception=exception)

        log.debug3("Done with executing node: %s", component)

    def __init__(
        self,
        session: Session,
        after_deploy: Optional[Callable[[Session], bool]] = None,
        after_verify: Optional[Callable[[Session], bool]] = None,
    ):
        """Construct with the fully-populated session for the rollout

        Args:
            session:  Session
                The current session for the reconciliation
            after_deploy:  Optional[Callable[[Session] bool]]
                An optional callback hook that will be invoked after the deploy
                phase completes. The return indicates whether the validation has
                passed.
            after_verify:  Optional[Callable[[Session] None]]
                An optional callback hook that will be invoked after the verify
                phase completes. The return indicates whether the validation has
                passed.
        """
        self._session = session
        self._after_deploy = after_deploy
        self._after_verify = after_verify

        # Read pool size from config
        deploy_threads = config.rollout_manager.deploy_threads
        verify_threads = config.rollout_manager.verify_threads

        # If session is in standalone mode, the recursive deploy -> render in
        # subsystems can cause jsii to fail in some spectacular ways. As such,
        # we force single-threaded execution in standalone mode.
        if config.standalone:
            log.info("Running rollout without threads in standalone mode")
            deploy_threads = 0
            verify_threads = 0

        deploy_node_fn = partial(
            RolloutManager.run_node,
            deploy_component,
            self._session,
        )

        verify_node_fn = partial(
            RolloutManager.run_node,
            verify_component,
            self._session,
            fail_halt_runner=False,
        )

        # Override disabled components with the disable function
        self.disabled_nodes = set()
        for component in session.graph:
            if component.disabled:
                component.set_data(
                    partial(
                        RolloutManager.run_node,
                        disable_component,
                        self._session,
                        component,
                    )
                )

                self.disabled_nodes.add(component)

        self._deploy_graph = Runner(
            "deploy",
            threads=deploy_threads,
            graph=session.graph,
            default_function=deploy_node_fn,
            verify_upstream=not config.dry_run,
        )
        self._verify_graph = Runner(
            "verify",
            threads=verify_threads,
            graph=session.graph,
            default_function=verify_node_fn,
            verify_upstream=not config.dry_run,
        )

    def rollout(  # pylint: disable=too-many-locals,too-many-statements
        self,
    ) -> CompletionState:
        """Perform the rollout

        The logic has four phases:
            1. Deploy Graph: This phase executes the Runner which runs the deploy()
                function for each Component in dependency order. For graph edges
                with configured verification functions, they are also executed
                during this phase.
            2. After Deploy: If configured with an after_deploy hook and (1)
                completed all nodes successfully, this function is called. Any
                raised exceptions indicate that the rollout should not proceed.
            3. Verify Graph: This phase executes the Runner which runs the verify()
                function for each Component in dependency order.
            4. After Verify: If configured with an after_verify hook and (3)
                completed all nodes successfully, this function is called. Any
                raised exceptions indicate that the rollout is not fully
                verified.

        The rollout can terminate in one of three states:
            1. incomplete AND failed: Something unexpected happened and the
                rollout terminated in a fatal state.
            2. incomplete AND NOT failed: One or more nodes did not pass their
                verify steps, but all deploy steps that were attempted
                succeeded.
            3. complete AND NOT failed: The rollout completed all nodes,
                including all verify steps. The managed components are ready to
                take traffic.

        Returns:
            completion_state:  CompletionState
                The final completion state of all nodes in the rollout Runner. This
                is a logical composition of the outputs of the above phases
                based on the termination logic above.
        """

        # NOTE: The Runner is guaranteed to not throw (unless there's a bug!)
        #   so we don't need to wrap run() in a try/except since the except
        #   clause will never catch "expected" errors

        ###########################
        ## Phase 1: Deploy Graph ##
        ###########################
        with alog.ContextTimer(log.trace, "Deploy Graph [%s]: ", self._session.id):
            self._deploy_graph.run()
        deploy_completion_state = self._deploy_graph.completion_state()

        # Log phase 1 results:
        #   * SUCCESS => All Components ran `render()` and `deploy()` without
        #       raising. This is fetched from the `verify_completed()` state
        #       since Components may raise precondition errors resulting in
        #       `deploy_completed()` returning True, indicating that all nodes
        #       ran and none reached a failed state, but not all nodes running
        #       to final completion
        #   * FAILED => Some nodes raised fatal errors
        #   * INCOMPLETE => No errors were raised, but some nodes did not fully
        #       complete without raising
        log.debug3("Deploy completion: %s", deploy_completion_state)
        phase1_complete = deploy_completion_state.verify_completed()
        phase1_failed = deploy_completion_state.failed()
        log.debug(
            "[Phase 1] Deploy result: %s",
            "SUCCESS"
            if phase1_complete
            else ("FAILED" if phase1_failed else "INCOMPLETE"),
        )

        ###########################
        ## Phase 2: After Deploy ##
        ###########################

        phase2_complete = phase1_complete
        phase2_exception = None
        if phase1_complete and self._after_deploy:
            log.debug2("Running after-deploy")
            try:
                phase2_complete = self._after_deploy(self._session)
                if not phase2_complete:
                    phase2_exception = VerificationError(
                        "After-deploy verification failed"
                    )
            except Exception as err:  # pylint: disable=broad-except
                log.debug2("Error caught during after-deploy: %s", err, exc_info=True)
                phase2_complete = False
                phase2_exception = err

        # Log phase 2 results
        log.debug(
            "[Phase 2] After deploy result: %s",
            "SUCCESS"
            if phase2_complete
            else ("FAILED" if phase2_exception else "NOT RUN"),
        )

        ###########################
        ## Phase 3: Verify Graph ##
        ###########################

        # If phase 1 ran without erroring, then run the verify Runner
        phase3_complete = False
        phase3_failed = False
        if not phase1_failed:
            # Configured the verify Runner based off of deployed nodes
            # This way only components that have started will be verified
            deployed_nodes = (
                deploy_completion_state.verified_nodes.union(
                    deploy_completion_state.unverified_nodes
                )
            ) - deploy_completion_state.failed_nodes
            log.debug3("Verify phase running with Nodes: %s", deployed_nodes)

            # Enable/Disable all nodes in verify_graph based on whether they
            # were deployed or not
            for comp in set(self._session.get_components()):
                if comp in deployed_nodes:
                    self._verify_graph.enable_node(comp)
                else:
                    self._verify_graph.disable_node(comp)

                # Disabled components should immediately verify
                if comp in self.disabled_nodes:
                    comp.set_data(lambda *_: True)

            # Run the verify Runner
            with alog.ContextTimer(log.trace, "Verify Graph [%s]: ", self._session.id):
                self._verify_graph.run()
            verify_completion_state = self._verify_graph.completion_state()
            log.debug3("Verify completion: %s", verify_completion_state)
            # Only consider phase3 completed if phase1 and phase2 fully completed
            phase3_complete = (
                verify_completion_state.verify_completed()
                and phase1_complete
                and phase2_complete
            )
            phase3_failed = verify_completion_state.failed()
        else:
            verify_completion_state = CompletionState()

        # Log phase 3 results
        log.debug(
            "[Phase 3] Verify result: %s",
            "SUCCESS"
            if phase3_complete
            else (
                "FAILED"
                if phase3_failed
                else ("INCOMPLETE" if phase2_complete else "NOT RUN")
            ),
        )

        ###########################
        ## Phase 4: After Verify ##
        ###########################

        phase4_complete = phase3_complete
        phase4_exception = None
        if phase3_complete and self._after_verify:
            log.debug("Running after-verify")
            try:
                phase4_complete = self._after_verify(self._session)
                if not phase4_complete:
                    phase4_exception = VerificationError(
                        "Application verification incomplete"
                    )
            except Exception as err:  # pylint: disable=broad-except
                log.debug2("Error caught during after-verify: %s", err, exc_info=True)
                phase4_complete = False
                phase4_exception = err

        # Log phase 4 results
        log.debug(
            "[Phase 4] After deploy result: %s",
            "SUCCESS"
            if phase4_complete
            else ("FAILED" if phase4_exception else "NOT RUN"),
        )

        # Create a final completion state with the "deployed nodes" pulled
        # from the deploy results and the "verified nodes" pulled from the
        # verify results.
        #
        # Verified Nodes: Nodes that made it all the way through the verify
        #   graph
        # Unverified Nodes: Nodes that are "verified" in the deploy graph, but
        #   are unverified in the verify graph or were not run in the verify
        #   graph and did not fail in the verify graph
        # Failed Nodes: Nodes that failed in either graph
        # Unstarted Nodes: Nodes that were unstarted in the deploy graph
        # Exception: Any exception from any of the phases above
        verified_nodes = verify_completion_state.verified_nodes
        failed_nodes = verify_completion_state.failed_nodes.union(
            deploy_completion_state.failed_nodes
        )
        unverified_nodes = (
            (
                deploy_completion_state.verified_nodes.union(
                    deploy_completion_state.unverified_nodes
                ).union(verify_completion_state.unverified_nodes)
            )
            - verified_nodes
            - failed_nodes
        )
        unstarted_nodes = deploy_completion_state.unstarted_nodes
        exception = (
            deploy_completion_state.exception
            or phase2_exception
            or verify_completion_state.exception
            or phase4_exception
        )
        completion_state = CompletionState(
            verified_nodes=verified_nodes,
            unverified_nodes=unverified_nodes,
            failed_nodes=failed_nodes,
            unstarted_nodes=unstarted_nodes,
            exception=exception,
        )
        log.debug2("Final rollout state: %s", completion_state)
        return completion_state
