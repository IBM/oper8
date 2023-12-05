"""Tests for the Controller class"""

# Standard
from datetime import timedelta
from functools import partial

# Third Party
import pytest

# First Party
import aconfig
import alog

# Local
from oper8 import Component, Controller, ReconcileManager, status
from oper8.dag import Node
from oper8.exceptions import PreconditionError, RolloutError, VerificationError
from oper8.reconcile import RequeueParams
from oper8.test_helpers.helpers import (
    TEST_NAMESPACE,
    DummyController,
    DummyNodeComponent,
    FailOnce,
    MockDeployManager,
    library_config,
    setup_cr,
    setup_session,
)
from tests.test_reconcile import ReconcileDummyController

log = alog.use_channel("TEST")

################################################################################
## Helpers #####################################################################
################################################################################


class AlogConfigureMock:
    def __init__(self):
        self.kwargs = None

    def __call__(self, **kwargs):
        self.kwargs = kwargs


def check_status(deploy_manager, cr, ready_reason=None, updating_reason=None):
    """Shared helper for checking status after reconcile"""
    obj = deploy_manager.get_obj(
        kind=cr.kind,
        name=cr.metadata.name,
        namespace=cr.metadata.namespace,
        api_version=cr.apiVersion,
    )
    assert obj is not None

    ready_cond = status.get_condition(status.READY_CONDITION, obj.status)
    if ready_reason:
        assert ready_cond
        assert ready_cond["reason"] == ready_reason.value
    else:
        assert not ready_cond

    update_cond = status.get_condition(status.UPDATING_CONDITION, obj.status)
    if updating_reason:
        assert update_cond
        assert update_cond["reason"] == updating_reason.value
    else:
        assert not update_cond


def check_bad_reconcile(
    controller_class,
    ready_reason=None,
    updating_reason=None,
    raises=True,
):
    """Common test body for tests of reconcile that raise unexpected errors"""
    cr = setup_cr()
    dm = MockDeployManager(resources=[cr])
    ctrlr = controller_class(deploy_manager=dm)

    # Make sure RolloutError is raised
    if raises:
        with pytest.raises(RolloutError):
            ctrlr.reconcile(cr)
    else:
        result = ctrlr.reconcile(cr)
        assert result.requeue

    # Make sure status is set correctly
    check_status(dm, cr, ready_reason, updating_reason)


class FinalizerController(Controller):
    group = "foo.bar"
    version = "v1"
    kind = "Foo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setup_called = False
        self.finalize_called = False

    def setup_components(self, session):
        self.setup_called = True

    def finalize_components(self, session):
        self.finalize_called = True


################################################################################
## Tests #######################################################################
################################################################################

##################
## Construction ##
##################


def test_construct_defaults():
    """Make sure that a controller can be constructed with its default args"""
    with library_config(dry_run=True):
        DummyController()
    with library_config(dry_run=False):
        DummyController()


def test_construct_input_args():
    """Make sure that a controller can be constructed with given args"""
    DummyController(config_defaults=aconfig.Config({"foo": 1}))


def test_construct_property_check():
    """Make sure that a controller can not be constructed without the required
    abstract class properties defined
    """

    class BadControllerNoGroup(Controller):
        version = "v1"
        kind = "Foo"

        def setup_components(*_, **__):
            pass

    class BadControllerNoVersion(Controller):
        group = "foo.bar"
        kind = "Foo"

        def setup_components(*_, **__):
            pass

    class BadControllerNoKind(Controller):
        group = "foo.bar"
        version = "v1"

        def setup_components(*_, **__):
            pass

    with pytest.raises(NotImplementedError):
        BadControllerNoGroup()

    with pytest.raises(NotImplementedError):
        BadControllerNoVersion()

    with pytest.raises(NotImplementedError):
        BadControllerNoKind()


#######################
## defaults ##
#######################


def test_get_cr_manifest_defaults():
    class TestDummyController(DummyController):
        def get_cr_manifest_defaults(
            self,
        ):
            return aconfig.Config({"example": "config"})

    ctrlr = TestDummyController()
    assert ctrlr.get_cr_manifest_defaults() == aconfig.Config({"example": "config"})


def test_get_config_defaults():
    class TestDummyController(DummyController):
        def get_config_defaults(
            self,
        ):
            return aconfig.Config({"example": "config"})

    ctrlr = TestDummyController()
    assert ctrlr.get_config_defaults() == aconfig.Config({"example": "config"})

    ctrlr = DummyController(config_defaults={"example": "anotherconfig"})
    assert ctrlr.get_config_defaults() == aconfig.Config({"example": "anotherconfig"})


#######################
## _manage_components ##
#######################


def test_manage_components():
    """Make sure manage_components correctly calls either setup_components
    or finalize_components
    """
    session = setup_session()
    ctrlr = ReconcileDummyController()
    ctrlr._manage_components(session, is_finalizer=False)
    assert ctrlr.setup_components.called
    assert not ctrlr.finalize_components.called
    assert not session.graph.empty()
    assert Node("foo") in session.graph


def test_manage_components_finalizer():
    """Make sure manage_components correctly calls either setup_components
    or finalize_components
    """
    session = setup_session()
    ctrlr = ReconcileDummyController()
    ctrlr._manage_components(session, is_finalizer=True)
    assert not ctrlr.setup_components.called
    assert ctrlr.finalize_components.called
    assert session.graph.empty()


#######################
## _rollout_components ##
#######################


def test_rollout_components():
    session = setup_session()
    ctrlr = ReconcileDummyController()

    # Deploy initial component object to ensure disabled
    # components get removed
    session.deploy_manager.deploy(
        [
            {
                "kind": "Baz",
                "apiVersion": "v3",
                "metadata": {"name": "baz", "namespace": session.namespace},
            }
        ]
    )
    assert session.deploy_manager.has_obj(
        kind="Baz", api_version="v3", name="baz", namespace=session.namespace
    )

    ctrlr.setup_components(session)
    completion_state = ctrlr._rollout_components(session)

    # Ensure the cluster state is as expected
    assert session.deploy_manager.has_obj(
        kind="Foo", api_version="v1", name="foo", namespace=session.namespace
    )
    assert session.deploy_manager.has_obj(
        kind="Bar", api_version="v2", name="bar", namespace=session.namespace
    )
    assert not session.deploy_manager.has_obj(
        kind="Baz", api_version="v3", name="baz", namespace=session.namespace
    )

    # Make sure the after_deploy and after_verify were called
    assert ctrlr.after_deploy.called
    assert ctrlr.after_verify.called

    # Test completion state
    assert completion_state.deploy_completed()
    assert completion_state.verify_completed()
    assert not completion_state.failed()
    assert Node("foo") in completion_state.verified_nodes
    assert Node("bar") in completion_state.verified_nodes
    assert Node("baz") in completion_state.verified_nodes


@pytest.mark.parametrize(
    ["raised_exception", "expected_exception"],
    [
        [ValueError, RolloutError],
        [PreconditionError, PreconditionError],
    ],
)
def test_rollout_components_exception(raised_exception, expected_exception):
    session = setup_session()

    ctrlr = DummyController(
        components=[{"name": "bad", "deploy_fail": raised_exception}]
    )
    ctrlr.setup_components(session)
    with pytest.raises(expected_exception):
        ctrlr._rollout_components(session)


###############
## reconcile ##
###############


def test_run_reconcile_ok():
    """Test that a reconciliation with several components that complete cleanly
    exists correctly and sets status conditions to STABLE
    """
    session = setup_session()
    ctrlr = DummyController(
        components=[
            {
                "name": "foo",
                "api_objects": [("foo", {"kind": "Foo", "apiVersion": "v1"})],
            },
            {
                "name": "bar",
                "api_objects": [("bar", {"kind": "Bar", "apiVersion": "v2"})],
                "upstreams": ["foo"],
            },
        ],
    )
    completion_state = ctrlr.run_reconcile(session)
    assert session.deploy_manager.has_obj(
        kind="Foo", name="foo", api_version="v1", namespace=session.namespace
    )
    assert session.deploy_manager.has_obj(
        kind="Bar", name="bar", api_version="v2", namespace=session.namespace
    )

    assert completion_state.deploy_completed()
    assert completion_state.verify_completed()
    assert not completion_state.failed()
    assert Node("foo") in completion_state.verified_nodes
    assert Node("bar") in completion_state.verified_nodes


def test_run_reconcile_rerun():
    """Test that when a rollout is incomplete at the deploy phase, the
    PreconditionError is correctly propagated and the status is set correctly
    """
    session = setup_session()
    ctrlr = ReconcileDummyController()
    ctrlr.run_reconcile(session)

    with pytest.raises(RolloutError):
        ctrlr.run_reconcile(session)


##################
## after_deploy ##
##################


def test_after_deploy_failure():
    """Make sure that a raised error during after_deploy is handled and the
    status is set correctly
    """
    session = setup_session()
    ctrlr = DummyController(after_deploy_fail=True)

    with pytest.raises(VerificationError):
        ctrlr.run_reconcile(session)

    assert ctrlr.after_deploy.called
    assert not ctrlr.after_verify.called


def test_after_deploy_error():
    """Make sure that an error in after_deploy is handled"""
    session = setup_session()
    ctrlr = DummyController(after_deploy_fail="assert")
    with pytest.raises(RolloutError):
        ctrlr.run_reconcile(session)

    # Make sure after_deploy was called, but after_verify was not
    assert ctrlr.after_deploy.called
    assert not ctrlr.after_verify.called


##################
## after_verify ##
##################


def test_after_verify_failure():
    """Make sure that a negative return from after_verify is handled and the
    status is set correctly
    """
    session = setup_session()
    ctrlr = DummyController(after_verify_fail=True)
    with pytest.raises(VerificationError):
        ctrlr.run_reconcile(session)

    # Make sure both after_deploy and after_verify were called
    assert ctrlr.after_deploy.called
    assert ctrlr.after_verify.called


def test_after_verify_error():
    """Make sure that an error in after_verify is handled"""
    session = setup_session()
    ctrlr = DummyController(after_verify_fail="assert")
    with pytest.raises(RolloutError):
        ctrlr.run_reconcile(session)

    # Make sure both after_deploy and after_verify were called
    assert ctrlr.after_deploy.called
    assert ctrlr.after_verify.called


####################
## should_requeue ##
####################


@pytest.mark.parametrize(
    ["requeue", "requeue_params"],
    [
        [True, None],
        [False, None],
        [True, RequeueParams(requeue_after=timedelta(seconds=300.0))],
    ],
)
def test_override_should_requeue(requeue, requeue_params):
    """Make sure overriding should_requeue by children works"""

    def should_requeue(*_, **__):
        return requeue, requeue_params

    class CustomRequeueController(DummyController):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.should_requeue = should_requeue

    session = setup_session()
    ctrlr = CustomRequeueController(
        components=[],
    )
    ctrlr.run_reconcile(session)
    returned_requeue, returned_params = ctrlr.should_requeue()

    assert returned_requeue == requeue
    assert returned_params == requeue_params


def test_should_requeue_error():
    """Make sure status becomes error when should_requeue raises unexpected error"""

    def should_requeue(*_, **__):
        raise RuntimeError("Oh No!")

    class CustomRequeueController(DummyController):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.should_requeue = should_requeue

    session = setup_session()
    ctrlr = CustomRequeueController()

    with pytest.raises(RuntimeError):
        ctrlr.should_requeue(session)


def test_should_requeue_no_obj():
    session = setup_session(deploy_initial_cr=False)
    ctrlr = DummyController()
    assert ctrlr.should_requeue(session) == (False, RequeueParams())


def test_should_requeue_failed_status():
    """Make sure status becomes error when should_requeue raises unexpected error"""
    dm = MockDeployManager(get_state_fail=FailOnce((False, None), fail_number=2))
    session = setup_session(deploy_manager=dm)
    ctrlr = DummyController()

    assert ctrlr.should_requeue(session) == (True, RequeueParams())


###########
## other ##
###########


def test_str():
    """Make sure the group/version/kind are present in the str representation"""
    ctrlr = DummyController()
    assert ctrlr.group in str(ctrlr)
    assert ctrlr.version in str(ctrlr)
    assert ctrlr.kind in str(ctrlr)


def test_has_finalizer():
    """Make sure that the has_finalizer property only returns true when a valid
    finalize_components implementation is provided
    """

    class WithFinalizer(Controller):
        group = "foo.bar"
        version = "v1"
        kind = "Foo"

        def setup_components(self, session):
            pass

        def finalize_components(self, session):
            pass

    class WithoutFinalizer(Controller):
        group = "foo.bar"
        version = "v2"
        kind = "Bar"

        def setup_components(self, session):
            pass

    assert WithFinalizer.has_finalizer
    assert not WithoutFinalizer.has_finalizer


class FooComponent(DummyNodeComponent):
    name = "foo"

    def __init__(self, session):
        super().__init__(
            session=session,
            api_objects=[("foo", {"kind": "Foo", "apiVersion": "v1"})],
        )


class SubsystemController(Controller):
    group = "foo.bar"
    version = "v1"
    kind = "Subsystem"

    def setup_components(self, session):
        FooComponent(session)


class SubsystemComponent(Component):
    name = "Subsystem"

    def build_chart(self, session):
        self.add_resource(
            "subsystem",
            {
                "kind": "Subsystem",
                "apiVersion": "foo.bar/v1",
                "metadata": {"name": "subsystem", "namespace": TEST_NAMESPACE},
            },
        )


class BarComponent(DummyNodeComponent):
    name = "bar"

    def __init__(self, session):
        super().__init__(
            session=session,
            api_objects=[("bar", {"kind": "Bar", "apiVersion": "v1"})],
        )


class ParentController(Controller):
    group = "foo.bar"
    version = "v1"
    kind = "Parent"

    def setup_components(self, session):
        BarComponent(session)
        SubsystemComponent(session)


def test_subsystem_rollout():
    """Test that multiple controllers can work together to rollout a tree of
    components and subsystems
    """

    # Set up the CR for the parent controller
    cr = setup_cr(api_version="foo.bar/v1", kind="Parent", version="v1")

    # Instantiate the controllers with a shared deploy manager
    dm = MockDeployManager()
    rm = ReconcileManager(deploy_manager=dm, reimport_controller=False)
    parent_ctrlr = ParentController()
    parent_reconcile = partial(rm.safe_reconcile, parent_ctrlr)
    subsystem_ctrlr = SubsystemController()
    susbsystem_reconcile = partial(rm.safe_reconcile, subsystem_ctrlr)

    # Register both Controllers to watch their respective resources
    dm.register_watch(
        api_version="foo.bar/v1",
        kind="Parent",
        callback=parent_reconcile,
    )
    dm.register_watch(
        api_version="foo.bar/v1",
        kind="Subsystem",
        callback=susbsystem_reconcile,
    )

    # Deploy the parent CR
    dm.deploy([cr])

    # Make sure the api objects from both controllers were deployed
    assert dm.has_obj(
        kind="Foo", name="foo", api_version="v1", namespace=cr.metadata.namespace
    )
    assert dm.has_obj(
        kind="Bar", name="bar", api_version="v1", namespace=cr.metadata.namespace
    )
