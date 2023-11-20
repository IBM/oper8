# Standard
from unittest.mock import Mock

# Third Party
import pytest

# Local
from oper8 import Component, Controller, component, controller
from oper8.test_helpers.helpers import library_config, setup_cr, setup_session

## @component ##################################################################

mock_verify = Mock()


@component(name="pure_comp")
class PureComponent(Component):
    def __init__(self, session):
        super().__init__(session=session)
        self.add_resource(
            "test_obj",
            {
                "kind": "test",
                "apiVersion": "v1",
                "metadata": {"name": session.get_scoped_name("test_obj")},
            },
        )

        # Attach the verify mock
        self.verify = mock_verify.method


def test_component_creation():
    """Ensure that decorated components are created correctly"""

    session = setup_session()

    # Pure components are a Component
    comp = PureComponent(session)
    assert isinstance(comp, Component)

    # Pure components can be built
    config = comp.to_config(session)
    assert config[0].metadata.name == session.get_scoped_name("test_obj")

    # Pure components have a class attribute "name"
    assert hasattr(comp.__class__, "name")


## @controller #################################################################

setup_components_mock = Mock()


@controller(
    group="foo.bar",
    version="v1",
    kind="Foo",
)
class FooController(Controller):
    def setup_components(self, *_, **__):
        setup_components_mock()


def test_controller_creation():
    """Make sure that an @controller decorated class can be created"""

    # Set up the controller
    with library_config(dry_run=True):
        ctrlr = FooController()
    assert isinstance(ctrlr, Controller)

    # Check class properties
    assert ctrlr.__class__.group == "foo.bar"
    assert ctrlr.__class__.version == "v1"
    assert ctrlr.__class__.kind == "Foo"

    # Roll out
    ctrlr.run_reconcile(setup_session())


def test_controller_extra_properties():
    """Make sure that the extra_properties argument to @controller properly
    attaches the given properties to the Controller class
    """

    @controller(
        group="foo.bar",
        version="v2",
        kind="Bar",
        extra_properties={"foo": "bar"},
    )
    class BarController(Controller):
        def setup_components(self, *_, **__):
            setup_components_mock()

    assert hasattr(BarController, "foo")
    assert BarController.foo == "bar"


def test_controller_finalizer():
    """Make sure that the extra_properties argument to @controller properly
    attaches the given properties to the Controller class
    """

    @controller(
        group="foo.bar",
        version="v2",
        kind="Bar",
        finalizer="test",
    )
    class BarController(Controller):
        def setup_components(self, *_, **__):
            setup_components_mock()

    assert BarController.finalizer == "test"
