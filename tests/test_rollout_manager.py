"""
Test the rollout manager's functionality
"""

# Standard
from functools import partial
from unittest import mock
import logging
import time

# Third Party
import pytest

# First Party
import alog

# Local
from oper8.dag import CompletionState, Node
from oper8.exceptions import (
    ClusterError,
    PreconditionError,
    RolloutError,
    VerificationError,
)
from oper8.rollout_manager import RolloutManager
from oper8.session import Session
from oper8.test_helpers.helpers import (
    DummyController,
    DummyNodeComponent,
    library_config,
    setup_session,
)

################################################################################
## Helpers #####################################################################
################################################################################

log = alog.use_channel("TEST")


def DummyRolloutComponent(name):
    class _DummyRolloutComponent(DummyNodeComponent):
        """Wrapper for DummyNodeComponent that keeps track of which keeps track of which
        phases have completed and in what order
        """

        def __init__(self, *args, deploy_delay=0, verify_delay=0, **kwargs):
            super().__init__(*args, **kwargs)
            self.completed_states = {}
            self.deploy_delay = deploy_delay
            self.verify_delay = verify_delay

            # Wrap the main lifecycle methods
            for name in ["deploy", "verify"]:
                setattr(self, f"_{name}", getattr(self, name))
                setattr(self, name, partial(self._wrapped_member, name))

        def _wrapped_member(self, name, session):

            # We always render here so that we don't have to worry about
            # pre-rendering in the tests
            self.render_chart(session)
            log.debug("Calling [%s] super().%s", self.name, name)
            delay_name = f"{name}_delay"
            if hasattr(self, delay_name):
                time.sleep(getattr(self, delay_name))
            res = getattr(self, f"_{name}")(session)
            if res:
                self.completed_states[name] = time.time()
            return res

        def deploy_completed(self):
            return "deploy" in self.completed_states

        def verify_completed(self):
            return "verify" in self.completed_states

    setattr(_DummyRolloutComponent, "name", name)
    return _DummyRolloutComponent


def get_end_order(components):
    completed_components = [
        comp for comp in components if "verify" in comp.completed_states
    ]
    return sorted(
        completed_components, key=lambda comp: comp.completed_states["verify"]
    )


################################################################################
## RolloutManager Tests ########################################################
################################################################################


class TestRolloutManager:

    #####################
    ## Rollout Success ##
    #####################

    def test_happy_path(self):
        """Test that a rollout manager can be set up with a session that has
        several dependent components and that it runs to completion correctly
        """

        # Set up a session and parent
        session = setup_session()

        # Set up a small set of components which will all pass
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session)
        comp_c = DummyRolloutComponent("C")(session)

        # Add the deps in a simple dependency relation
        session.add_component_dependency(comp_b, comp_a)
        session.add_component_dependency(comp_c, comp_a)

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(session)
        completion_state = mgr.rollout()
        log.debug2(completion_state)

        # Make sure that it succeeded and that all components completed all
        # forward states
        assert completion_state.deploy_completed()
        assert completion_state.verify_completed()
        assert not completion_state.failed()
        for comp in [comp_a, comp_b, comp_c]:
            assert comp.deploy_completed()
            assert comp.verify_completed()

        # Make sure that the components completed in the expected order (A
        # before either B or C)
        end_order = get_end_order([comp_a, comp_b, comp_c])
        assert comp_a in end_order
        assert comp_b in end_order
        assert comp_c in end_order
        assert comp_a == end_order[0]

        # Make sure the completion state looks right
        assert completion_state == CompletionState(
            verified_nodes=[Node("A"), Node("B"), Node("C")],
            unverified_nodes=[],
            failed_nodes=[],
            unstarted_nodes=[],
        )

    def test_rollout_edge_verify_dry_run(self):
        """When performing a rollout in dry_run, test that a custom edge
        verification function is not executed.
        """
        with library_config(dry_run=True):
            session = setup_session()

            # Set two nodes with a custom edge dependency
            #
            #  A <-x- B
            comp_a = DummyRolloutComponent("A")(session)
            comp_b = DummyRolloutComponent("B")(session)

            def broken_verify(*_, **__):
                raise RuntimeError("broken")

            session.add_component_dependency(comp_b, comp_a, broken_verify)

            # Create the rollout manager and run the rollout
            mgr = RolloutManager(session)
            completion_state = mgr.rollout()

            # Make sure the rollout succeeded. This test's goal is to ensure that
            # the rollout terminates correctly, so getting here is sufficient to
            # show success
            assert completion_state.deploy_completed()
            assert completion_state.verify_completed()
            assert not completion_state.failed()

    def test_rollout_edge_verify_non_dry_run(self):
        """When performing a rollout non dry_run, test that a custom edge
        verification function is executed.
        """
        with library_config(dry_run=False):
            session = setup_session()

            # Set two nodes with a custom edge dependency which will return False
            #
            #  A <-x- B
            comp_a = DummyRolloutComponent("A")(session)
            comp_b = DummyRolloutComponent("B")(session)
            session.add_component_dependency(comp_b, comp_a, lambda _: False)

            # Create the rollout manager and run the rollout
            mgr = RolloutManager(session)
            completion_state = mgr.rollout()

            # Make sure the rollout got to the end without exception and that B did
            # not deploy due to the edge dependency
            assert not completion_state.deploy_completed()
            assert not completion_state.verify_completed()
            assert not completion_state.failed()
            assert comp_a in completion_state.verified_nodes
            assert comp_b in completion_state.unstarted_nodes

    def test_happy_path_after_deploy_verify(self):
        """Test that a rollout manager can be set up with a session that has
        several dependent components and that it runs to completion correctly
        when after_deploy and after_verify hooks is configured and passes.
        """

        # Set up a minimal DAG with a single node
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        # Create the rollout manager and run the rollout
        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        log.debug2(completion_state)

        # Make sure that it succeeded and that all components completed all
        # forward states
        assert completion_state.deploy_completed()
        assert completion_state.verify_completed()

        # Check callbacks.
        assert not completion_state.failed()
        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert after_verify.called
        assert not after_verify_unsuccessful.called

    #####################
    ## Rollout Failure ##
    #####################

    def test_deploy_throw(self):
        """Test that a throw during deploy is handled properly"""
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, deploy_fail=RuntimeError)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        # Setup callbacks.
        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        mgr.rollout()

        # Make sure the right set of phases were hit
        assert comp_a.deploy_completed()
        assert not comp_a.verify_completed()
        assert not comp_b.deploy_completed()
        assert not comp_b.verify_completed()
        assert not comp_c.deploy_completed()
        assert not comp_c.verify_completed()

        # Check callbacks.
        assert not after_deploy.called
        assert after_deploy_unsuccessful.called
        assert not after_verify.called
        assert not after_verify_unsuccessful.called

    def test_deploy_throw_controller(self):
        """Test that a throw during deploy is handled properly when rollout is triggered via controller"""
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, deploy_fail=RuntimeError)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        ctrlr = DummyController()
        ctrlr.setup_components(session)

        with pytest.raises(RolloutError):
            # Rollout should fail at phase1 (deployment) and raise RolloutError.
            ctrlr._rollout_components(session)

        assert not ctrlr.after_deploy.called
        assert ctrlr.after_deploy_unsuccessful.called
        assert not ctrlr.after_deploy.called
        assert not ctrlr.after_deploy.called

    def test_rollout_deploy_incomplete(self):
        """Test the correct after_deploy, after_deploy_unsuccessful, after_verify,
        and after_verify_unsuccessful hooks are executed when deploy is incomplete."""
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, deploy_fail=RuntimeError)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        # Setup callbacks.
        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        mgr.rollout()

        # Make sure the right set of phases were hit
        assert comp_a.deploy_completed()
        assert not comp_a.verify_completed()
        assert not comp_b.deploy_completed()
        assert not comp_b.verify_completed()
        assert not comp_c.deploy_completed()
        assert not comp_c.verify_completed()

        # Check the callbacks.
        assert not after_deploy.called
        assert after_deploy_unsuccessful.called
        assert not after_verify.called
        assert not after_verify_unsuccessful.called

    def test_rollout_verify_incomplete(self):
        """Test that a failed verify test is handled properly as not a failure,
        but also not a completion
        """
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, verify_fail=True)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()

        # Make sure the right set of phases were hit:
        # - All nodes deployed
        # - Only A completed verification
        assert comp_a.deploy_completed()
        assert comp_b.deploy_completed()
        assert comp_c.deploy_completed()
        assert comp_a.verify_completed()
        assert not comp_b.verify_completed()
        assert not comp_c.verify_completed()

        # Check the callbacks.
        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert after_verify_unsuccessful.called

        # Make sure the completion state looks right
        assert completion_state == CompletionState(
            verified_nodes=[Node("A")],
            unverified_nodes=[Node("B"), Node("C")],
            failed_nodes=[],
            unstarted_nodes=[],
        )

    def test_rollout_verify_throw(self):
        """Test that a throw during verify is handled properly."""
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, verify_fail=RuntimeError)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        # Create the rollout manager and run the rollout
        #
        # NOTE: An unexpected exception in verify is considered a failed node,
        #   so we consider the deploy phase to have failed here as well. This is
        #   maybe a little aggressive because at this point the deploy() for
        #   each component has run successfully, but the alternative is to treat
        #   unexpected failures in verify as identical to programmatic False
        #   return values which feels too loose. Ultimately, this shouldn't
        #   happen!
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        assert not completion_state.deploy_completed()
        assert not completion_state.verify_completed()

        # Make sure the right set of phases were hit:
        # - All nodes deployed
        # - Only A completed verification
        assert comp_a.deploy_completed()
        assert comp_b.deploy_completed()
        assert comp_c.deploy_completed()
        assert comp_a.verify_completed()
        assert not comp_b.verify_completed()
        assert not comp_c.verify_completed()

        # Check callbacks.
        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert after_verify_unsuccessful.called

        # Make sure the completion state looks right
        assert completion_state == CompletionState(
            verified_nodes=[Node("A")],
            unverified_nodes=[Node("C")],
            failed_nodes=[Node("B")],
            unstarted_nodes=[],
        )

    def test_after_deploy_false(self):
        """Test that when after_deploy returns False, the rollout does not
        proceed
        """
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=False)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        log.debug2(completion_state)

        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, VerificationError)

    def test_after_deploy_forward_compatibility(self, caplog):
        """
        New arguments are introduced to after_deploy/after_verify callbacks with oper8 v0.1.33.
        Test if the new argument is received with new callbacks, and old callbacks can still be used without it.
        """
        caplog.set_level(logging.WARNING)
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        # Prepare mock functions.
        def new_after_deploy(
            session: Session, deploy_completion_state: CompletionState
        ) -> bool:
            assert isinstance(session, Session)
            assert isinstance(deploy_completion_state, CompletionState)
            return False

        mock_new_after_deploy = mock.Mock()
        mock_new_after_deploy.new_after_deploy.side_effect = new_after_deploy
        after_deploy_unsuccessful = mock.Mock(return_value=True)

        # Run rollout.
        mgr = RolloutManager(
            session,
            after_deploy=mock_new_after_deploy.new_after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
        )
        completion_state = mgr.rollout()
        log.debug2(completion_state)

        assert isinstance(completion_state.exception, VerificationError)
        assert "Please migrate" not in caplog.text

        assert mock_new_after_deploy.new_after_deploy.called
        assert not after_deploy_unsuccessful.called

    def test_after_deploy_backward_compatibility(self, caplog):
        """
        New arguments are introduced to after_deploy/after_verify callbacks with oper8 v0.1.33.
        Test if the new argument is received with new callbacks, and old callbacks can still be used without it.
        """
        caplog.set_level(logging.WARNING)
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        # Prepare mock functions.
        def old_after_deploy(session: Session) -> bool:
            assert isinstance(session, Session)
            return False

        mock_old_after_deploy = mock.Mock()
        mock_old_after_deploy.old_after_deploy.side_effect = old_after_deploy
        after_deploy_unsuccessful = mock.Mock(return_value=True)

        # Run rollout.
        mgr = RolloutManager(
            session,
            after_deploy=mock_old_after_deploy.old_after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
        )
        completion_state = mgr.rollout()
        log.debug2(completion_state)

        # Should warn user to migrate if old after_deploy function is used.
        assert "Please migrate" in caplog.text
        assert isinstance(completion_state.exception, VerificationError)

        assert mock_old_after_deploy.old_after_deploy.called
        assert not after_deploy_unsuccessful.called

    def test_after_deploy_unsuccessful_forward_compatibility(self, caplog):
        """Test the correct after_deploy, after_deploy_unsuccessful, after_verify,
        and after_verify_unsuccessful hooks are executed when deploy is incomplete."""
        caplog.set_level(logging.WARNING)
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, deploy_fail=RuntimeError)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        # Setup callbacks.
        after_deploy = mock.Mock(return_value=True)

        def new_after_deploy_unsuccessful(
            session: Session, failed: bool, deploy_completion_state: CompletionState
        ) -> bool:
            assert isinstance(session, Session)
            assert isinstance(failed, bool)
            assert isinstance(deploy_completion_state, CompletionState)
            return False

        mock_new_after_deploy_unsuccessful = mock.Mock()
        mock_new_after_deploy_unsuccessful.new_after_deploy_unsuccessful.side_effect = (
            new_after_deploy_unsuccessful
        )

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=mock_new_after_deploy_unsuccessful.new_after_deploy_unsuccessful,
        )
        mgr.rollout()

        assert "Please migrate" not in caplog.text

        # Check the callbacks.
        assert not after_deploy.called
        assert mock_new_after_deploy_unsuccessful.new_after_deploy_unsuccessful.called

    def test_after_deploy_unsuccessful_backward_compatibility(self, caplog):
        """Test the correct after_deploy, after_deploy_unsuccessful, after_verify,
        and after_verify_unsuccessful hooks are executed when deploy is incomplete."""
        caplog.set_level(logging.WARNING)
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, deploy_fail=RuntimeError)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        # Setup callbacks.
        after_deploy = mock.Mock(return_value=True)

        def old_after_deploy_unsuccessful(session: Session, failed: bool) -> bool:
            assert isinstance(session, Session)
            assert isinstance(failed, bool)
            return False

        mock_old_after_deploy_unsuccessful = mock.Mock()
        mock_old_after_deploy_unsuccessful.old_after_deploy_unsuccessful.side_effect = (
            old_after_deploy_unsuccessful
        )

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=mock_old_after_deploy_unsuccessful.old_after_deploy_unsuccessful,
        )
        mgr.rollout()

        assert "Please migrate" in caplog.text

        # Check the callbacks.
        assert not after_deploy.called
        assert mock_old_after_deploy_unsuccessful.old_after_deploy_unsuccessful.called

    def test_after_deploy_non_oper8_error(self):
        """Test that when after_deploy raises a non-oper8 error, the rollout is
        considered a failure
        """

        def fail(*_, **__):
            raise RuntimeError("Non-oper8 exception")

        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(side_effect=fail)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, RuntimeError)

    def test_after_deploy_fatal_error(self):
        """Test that when after_deploy raises a fatal error, the rollout is
        considered a failure
        """

        def fail(*_, **__):
            raise ClusterError("Fatal error")

        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(side_effect=fail)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, ClusterError)

    def test_after_deploy_non_fatal_error(self):
        """Test that when after_deploy raises a non-fatal error, the rollout is
        considered incomplete
        """

        def fail(*_, **__):
            raise PreconditionError("Expected error")

        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(side_effect=fail)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, PreconditionError)

    def test_after_verify_false(self):
        """Test that when after_verify returns false, a VerificationError is
        added to the the completion state
        """
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=False)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, VerificationError)

    def test_after_verify_forward_compatibility(self, caplog):
        """Test that when after_verify returns false, a VerificationError is
        added to the the completion state
        """
        caplog.set_level(logging.WARNING)
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)

        def new_after_verify(
            session: Session,
            verify_completion_state: CompletionState,
            deploy_completion_state: CompletionState,
        ):
            assert isinstance(session, Session)
            assert isinstance(verify_completion_state, CompletionState)
            assert isinstance(deploy_completion_state, CompletionState)
            return False

        mock_new_after_verify = mock.Mock()
        mock_new_after_verify.new_after_verify.side_effect = new_after_verify

        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=mock_new_after_verify.new_after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        assert "Please migrate" not in caplog.text

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert mock_new_after_verify.new_after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, VerificationError)

    def test_after_verify_backward_compatibility(self, caplog):
        """Test that when after_verify returns false, a VerificationError is
        added to the the completion state
        """
        caplog.set_level(logging.WARNING)
        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)

        def old_after_verify(
            session: Session,
        ):
            assert isinstance(session, Session)
            return False

        mock_old_after_verify = mock.Mock()
        mock_old_after_verify.old_after_verify.side_effect = old_after_verify

        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=mock_old_after_verify.old_after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        assert "Please migrate" in caplog.text

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert mock_old_after_verify.old_after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, VerificationError)

    def test_after_verify_unsuccessful_forward_compatibility(self, caplog):
        """Test the correct after_deploy, after_deploy_unsuccessful, after_verify,
        and after_verify_unsuccessful hooks are executed when deploy is incomplete."""
        caplog.set_level(logging.WARNING)
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, verify_fail=True)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)

        def new_after_verify_unsuccessful(
            session: Session,
            failed: bool,
            verify_completion_state: CompletionState,
            deploy_completion_state: CompletionState,
        ) -> bool:
            assert isinstance(session, Session)
            assert isinstance(failed, bool)
            assert isinstance(verify_completion_state, CompletionState)
            assert isinstance(deploy_completion_state, CompletionState)
            return False

        mock_new_after_verify_unsuccessful = mock.Mock()
        mock_new_after_verify_unsuccessful.new_after_verify_unsuccessful.side_effect = (
            new_after_verify_unsuccessful
        )

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=mock_new_after_verify_unsuccessful.new_after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()

        assert "Please migrate" not in caplog.text

        # Check the callbacks.
        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert mock_new_after_verify_unsuccessful.new_after_verify_unsuccessful.called

    def test_after_verify_unsuccessful_backward_compatibility(self, caplog):
        """Test the correct after_deploy, after_deploy_unsuccessful, after_verify,
        and after_verify_unsuccessful hooks are executed when deploy is incomplete."""
        caplog.set_level(logging.WARNING)
        session = setup_session()

        # Set up a linear set of components and configure the second-to-last to
        # fail during deploy.
        #
        #    A -> B -x C
        comp_a = DummyRolloutComponent("A")(session)
        comp_b = DummyRolloutComponent("B")(session, verify_fail=True)
        comp_c = DummyRolloutComponent("C")(session)
        comps = [comp_a, comp_b, comp_c]
        for i, comp in enumerate(comps[1:]):
            session.add_component_dependency(comp, comps[i])

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(return_value=True)

        def old_after_verify_unsuccessful(session: Session, failed: bool) -> bool:
            assert isinstance(session, Session)
            assert isinstance(failed, bool)
            return False

        mock_old_after_verify_unsuccessful = mock.Mock()
        mock_old_after_verify_unsuccessful.old_after_verify_unsuccessful.side_effect = (
            old_after_verify_unsuccessful
        )

        # Create the rollout manager and run the rollout
        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=mock_old_after_verify_unsuccessful.old_after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()

        assert "Please migrate" in caplog.text

        # Check the callbacks.
        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert not after_verify.called
        assert mock_old_after_verify_unsuccessful.old_after_verify_unsuccessful.called

    def test_after_verify_non_oper8_error(self):
        """Test that when after_verify raises a non-oper8 error, the rollout is
        considered a failure
        """

        def fail(*_, **__):
            raise RuntimeError("Non-oper8 exception")

        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(side_effect=fail)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, RuntimeError)

    def test_after_verify_fatal_error(self):
        """Test that when after_verify raises a fatal error, the rollout is
        considered a failure
        """

        def fail(*_, **__):
            raise ClusterError("Fatal error")

        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(side_effect=fail)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )
        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, ClusterError)

    def test_after_verify_non_fatal_error(self):
        """Test that when after_verify raises a non-fatal error, the rollout is
        considered incomplete
        """

        def fail(*_, **__):
            raise PreconditionError("Expected error")

        session = setup_session()
        comp = DummyRolloutComponent("A")(session)

        after_deploy = mock.Mock(return_value=True)
        after_deploy_unsuccessful = mock.Mock(return_value=True)
        after_verify = mock.Mock(side_effect=fail)
        after_verify_unsuccessful = mock.Mock(return_value=True)

        mgr = RolloutManager(
            session,
            after_deploy=after_deploy,
            after_deploy_unsuccessful=after_deploy_unsuccessful,
            after_verify=after_verify,
            after_verify_unsuccessful=after_verify_unsuccessful,
        )

        completion_state = mgr.rollout()
        log.debug2(completion_state)
        assert completion_state.deploy_completed()
        assert not completion_state.verify_completed()
        assert not completion_state.failed()

        assert after_deploy.called
        assert not after_deploy_unsuccessful.called
        assert after_verify.called
        assert not after_verify_unsuccessful.called

        assert isinstance(completion_state.exception, PreconditionError)
