"""
Tests for the DryRunWatchManager
"""

# Third Party
import pytest

# Local
from oper8 import Controller, controller
from oper8.test_helpers.helpers import (
    DummyController,
    MockDeployManager,
    library_config,
    setup_cr,
)
from oper8.watch_manager import DryRunWatchManager, WatchManagerBase

## Helpers #####################################################################


class FooController(DummyController):
    def __init__(self, **kwargs):
        super().__init__(
            components=[
                {
                    "name": "foo",
                    "api_objects": [("foo", {"kind": "Foo", "api_version": "v1"})],
                }
            ],
            **kwargs,
        )


class BarController(FooController):
    """Testable controller with a finalizer"""

    kind = "Bar"

    FINALIZE_CALLED = False

    def finalize_components(self, session):
        self.__class__.FINALIZE_CALLED = True


@pytest.fixture(autouse=True)
def reset_globals():
    """This helper is only used in tests to "reset" the state of the global
    watches dict
    """
    WatchManagerBase._ALL_WATCHES = {}
    BarController.FINALIZE_CALLED = False


## Tests #######################################################################


def test_lazy_construct():
    """Make sure that the controller is not constructed when the watch manager
    is first instantiated
    """

    @controller(group="foo.bar", version="v1", kind="Foo")
    class BadController(Controller):
        def __init__(self, *_, **__):
            raise RuntimeError()

    DryRunWatchManager(BadController)


def test_double_watch():
    """Test that all constructed watch managers get registered"""
    wm = DryRunWatchManager(FooController)
    assert wm.watch()
    assert not wm.watch()


def test_reconcile_single():
    """Test that a single watched controller can receive CR reconcile events"""
    dm = MockDeployManager()
    cr = setup_cr(
        kind=FooController.kind,
        api_version=f"{FooController.group}/{FooController.version}",
    )
    DryRunWatchManager(FooController, deploy_manager=dm)
    DryRunWatchManager.start_all()
    dm.deploy([cr])
    assert dm.has_obj(
        kind="Foo", api_version="v1", name="foo", namespace=cr.metadata.namespace
    )


def test_finalize_called():
    """Test that when an object exists in the cluster and is deleted, the
    finalizer for a Controller with one defined will be called
    """
    cr = setup_cr(
        kind=BarController.kind,
        api_version=f"{BarController.group}/{BarController.version}",
    )
    dm = MockDeployManager(resources=[cr])
    DryRunWatchManager(BarController, deploy_manager=dm)
    DryRunWatchManager.start_all()
    assert dm.has_obj(
        kind=cr.kind,
        api_version=cr.apiVersion,
        name=cr.metadata.name,
        namespace=cr.metadata.namespace,
    )
    dm.disable([cr])
    assert not dm.has_obj(
        kind=cr.kind,
        api_version=cr.apiVersion,
        name=cr.metadata.name,
        namespace=cr.metadata.namespace,
    )
    assert BarController.FINALIZE_CALLED


def test_watch_subsystem():
    """Test that a CR generated by one contoller can trigger another to run"""

    # CR for the parent
    cr = setup_cr(kind="Parent", api_version="foo.bar/v1")

    # Subsystem controller class that createsd a Foo api object
    @controller(group="foo.bar", version="v1", kind="Subsystem")
    class SubsystemController(DummyController):
        def __init__(self):
            super().__init__(
                components=[
                    {
                        "name": "foo",
                        "api_objects": [("foo", {"kind": "Foo", "api_version": "v1"})],
                    }
                ],
            )

    # Parent controller that creates a Bar api object and the CR for the
    # subsystem
    @controller(group="foo.bar", version="v1", kind="Parent")
    class ParentController(DummyController):
        def __init__(self):
            super().__init__(
                components=[
                    {
                        "name": "foo-subsystem",
                        "api_objects": [
                            (
                                cr.metadata.name,
                                {"kind": "Subsystem", "api_version": "foo.bar/v1"},
                            )
                        ],
                    },
                    {
                        "name": "bar",
                        "api_objects": [("bar", {"kind": "Bar", "api_version": "v1"})],
                    },
                ],
            )

    # Set up both controllers to watch
    dm = MockDeployManager()
    DryRunWatchManager(ParentController, deploy_manager=dm)
    DryRunWatchManager(SubsystemController, deploy_manager=dm)
    DryRunWatchManager.start_all()

    # Deploy the CR
    with library_config(standalone=True):
        dm.deploy([cr])

    # Make sure the resources for both the parent and subsystem got deployed
    assert dm.has_obj(
        kind="Foo", name="foo", api_version="v1", namespace=cr.metadata.namespace
    )
    assert dm.has_obj(
        kind="Bar", name="bar", api_version="v1", namespace=cr.metadata.namespace
    )
