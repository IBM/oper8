"""
Tests of the common base factory
"""


# Third Party
import pytest

# First Party
import alog

# Local
from oper8 import component
from oper8.test_helpers.helpers import setup_session
from oper8.x.datastores.connection_base import DatastoreConnectionBase
from oper8.x.datastores.factory_base import DatastoreSingletonFactoryBase
from oper8.x.datastores.interfaces import Datastore
from oper8.x.utils import common, constants

## Helpers #####################################################################

log = alog.use_channel("TEST")

###################
## testDataStore ##
###################


class TestConnection(DatastoreConnectionBase):
    """Connection type for Test datastores"""

    __test__ = False

    def __init__(self, session, foo, bar):
        super().__init__(session)
        self.foo = foo
        self.bar = bar

    def to_dict(self):
        return {"foo": self.foo, "bar": self.bar}

    @classmethod
    def from_dict(cls, session, config_dict):
        return TestConnection(session, **config_dict)


class TestFactory(DatastoreSingletonFactoryBase):
    """Single factory for these tests
    NOTE: Since the global state is mutated at import time, we cannot do dynamic
        registration in the tests and must have a single class defined here
    """

    DATASTORE_TYPE = "testDataStore"
    CONNECTION_TYPE = TestConnection
    __test__ = False


class TestDatastoreBase(Datastore):
    __test__ = False

    def doit(self):
        return True


@component(name="test-datastore-one")
class TestDatastoreOne(TestDatastoreBase):

    TYPE_LABEL = "one"
    __test__ = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("Constructing TestDatastoreOne")
        self.foo = False
        if self.config.do_it:
            self.add_resource(
                "secret-one",
                dict(
                    kind="Secret",
                    apiVersion="v1",
                    metadata=dict(name="secret-one"),
                    data=self.config.get("secret_data", {}),
                ),
            )
            self.foo = True

    def get_connection(self):
        return TestConnection(self.session, foo=self.foo, bar=1)


@component(name="test-datastore-two")
class TestDatastoreTwo(TestDatastoreBase):

    TYPE_LABEL = "two"
    __test__ = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("Constructing TestDatastoreTwo")

    def get_connection(self):
        return TestConnection(self.session, foo=False, bar=2)


TestFactory.register_type(TestDatastoreOne)
TestFactory.register_type(TestDatastoreTwo)


#################
## widgetStore ##
#################


class WidgetConnection(DatastoreConnectionBase):
    """Widget store connection type"""

    __test__ = False

    def __init__(self, session, widgets):
        super().__init__(session)
        self.widgets = widgets

    def to_dict(self):
        return {"widgets": self.widgets}

    @classmethod
    def from_dict(cls, session, config_dict):
        return WidgetConnection(session, **config_dict)


class WidgetFactory(DatastoreSingletonFactoryBase):
    """Second datastore factory type"""

    DATASTORE_TYPE = "widgetStore"
    CONNECTION_TYPE = WidgetConnection


class WidgetStoreBase(Datastore):
    pass


@component(name="widget-store-a")
class WidgetStoreA(WidgetStoreBase):

    TYPE_LABEL = "A"
    __test__ = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("Constructing TestDatastoreOne")
        config = self.config
        self.widgets = 0
        if self.config.do_it:
            self.widgets += 1
            self.add_resource(
                "secret-one",
                dict(
                    kind="Secret",
                    apiVersion="v1",
                    metadata=dict(name="secret-one"),
                    data=self.config.get("secret_data", {}),
                ),
            )

    def get_connection(self):
        return WidgetConnection(self.session, self.widgets)


WidgetFactory.register_type(WidgetStoreA)


## Tests #######################################################################

###################
## get_component ##
###################


def test_get_component_construct_by_type():
    """Test that a basic construction works"""
    instance1_name = "test_instance_one"
    instance1_config = {
        "type": "one",
        "key": "val",
    }
    instance2_name = "test_instance_two"
    instance2_config = {
        "type": "two",
        "key": "val",
    }

    _config = {
        TestFactory.DATASTORE_TYPE: {
            instance1_name: instance1_config,
            instance2_name: instance2_config,
        }
    }
    session = setup_session(app_config=_config)

    # Instance 1 is of type "one"
    instance1 = TestFactory.get_component(session, instance1_name)
    assert isinstance(instance1, TestDatastoreOne)
    assert instance1.config == instance1_config

    # Instance 2 is of type "two"
    instance2 = TestFactory.get_component(session, instance2_name)
    assert isinstance(instance2, TestDatastoreTwo)
    assert instance2.config == instance2_config


def test_get_component_base_class_function():
    """Test that a constructed instance can use a base class's function"""
    instance_name = "test_instance_one"
    instance_config = {
        "type": "one",
        "key": "val",
    }
    _config = {
        TestFactory.DATASTORE_TYPE: {
            instance_name: instance_config,
        }
    }
    session = setup_session(app_config=_config)

    instance = TestFactory.get_component(session, instance_name)
    assert instance.doit()


def test_get_component_derived_class_config_use():
    """Test that a derived class can use the session config to take conditional
    action
    """
    instance_name = "test_instance_one"
    secret_data = {"secret": common.b64_secret("value")}

    def app_config_overrides(doit):
        return {
            TestFactory.DATASTORE_TYPE: {
                instance_name: {
                    "type": "one",
                    "do_it": doit,
                    "secret_data": secret_data,
                },
            }
        }

    # Validate that with "do_it" True a secret is created
    with alog.ContextLog(log.debug, "Enabled"):
        _config = app_config_overrides(True)
        session = setup_session(app_config=_config)
        instance = TestFactory.get_component(session, instance_name)
        resources = instance.to_config(session)
        assert len(resources) == 1
        assert resources[0].data == secret_data

    # Validate that with "do_it" False the secret is not created
    with alog.ContextLog(log.debug, "Disabled"):
        _config = app_config_overrides(False)
        session = setup_session(app_config=_config)
        instance = TestFactory.get_component(session, instance_name)
        resources = instance.to_config(session)
        assert len(resources) == 0


def test_get_component_per_deploy_singleton():
    """Test that the same exact instance is returned for multiple calls within
    a single deploy, but across deploys the instance is recreated
    """
    instance_name = "test_instance_one"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {instance_name: {"type": "one"}}
    }
    session = setup_session(app_config=app_config_overrides)
    instance_a = TestFactory.get_component(session, instance_name)
    instance_b = TestFactory.get_component(session, instance_name)
    assert id(instance_a) == id(instance_b)

    session = setup_session(app_config=app_config_overrides)
    instance_c = TestFactory.get_component(session, instance_name)
    assert id(instance_a) != id(instance_c)


def test_get_component_multi_datastore_type():
    """Test that the factory can handle multiple datastore types independently"""
    test_instance_name = "test_instance_one"
    widget_component_name = "widget_component_a"
    widget_bad_instance_name = "widget_component_one"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {test_instance_name: {"type": "one"}},
        WidgetFactory.DATASTORE_TYPE: {
            widget_component_name: {"type": "A"},
            widget_bad_instance_name: {"type": "one"},
        },
    }
    session = setup_session(app_config=app_config_overrides)
    test_one = TestFactory.get_component(session, test_instance_name)
    assert test_one is not None
    widget_a = WidgetFactory.get_component(session, widget_component_name)
    assert widget_a is not None
    with pytest.raises(AssertionError):
        WidgetFactory.get_component(session, widget_bad_instance_name)


def test_get_component_unnamed():
    """Test that the factory can fetch an un-named instance"""
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {"type": "one"},
    }
    session = setup_session(app_config=app_config_overrides)
    test_one = TestFactory.get_component(session)
    assert test_one is not None
    assert test_one.__class__.name == TestDatastoreOne.name


def test_get_component_named_class_override():
    """Test that the factory can create an instance with an overloaded class
    name that is scoped by the instance name
    """
    instance_name = "foo"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {instance_name: {"type": "one"}}
    }
    session = setup_session(app_config=app_config_overrides)
    test_one = TestFactory.get_component(session, instance_name)
    assert test_one is not None
    assert test_one.__class__.name == f"{TestDatastoreOne.name}-{instance_name}"


def test_get_component_disabled():
    """Test that a constructed instance which is disabled is noted as such in
    the session
    """
    instance_name = "foo"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {instance_name: {"type": "one"}}
    }
    session = setup_session(app_config=app_config_overrides)
    test_one = TestFactory.get_component(session, instance_name, disabled=True)
    assert test_one is not None
    assert test_one.__class__.name == f"{TestDatastoreOne.name}-{instance_name}"
    enabled_components = session.get_components(disabled=False)
    assert not enabled_components
    disabled_components = session.get_components(disabled=True)
    assert disabled_components == [test_one]


def test_get_component_connection_in_cr():
    """Test that when asked for a component, but connection details are provided
    in the CR, no component is constructed
    """
    instance_name = "foo"
    input_config = {"foo": True, "bar": 2}
    deploy_config_overrides = {
        "datastores": {
            TestFactory.DATASTORE_TYPE: {
                instance_name: {constants.SPEC_DATASTORE_CONNECTION: input_config}
            }
        }
    }
    session = setup_session(deploy_config=deploy_config_overrides)
    test_one = TestFactory.get_component(session, instance_name, disabled=True)
    assert test_one is None


def test_get_component_config_overrides():
    """Test that overrides given in config_overrides take precidence over
    session.config
    """
    instance_name = "foo"
    app_config_overrides = {TestFactory.DATASTORE_TYPE: {"type": "one"}}
    session = setup_session(app_config=app_config_overrides)
    instance = TestFactory.get_component(
        session,
        config_overrides={
            "type": "two",
        },
    )
    assert isinstance(instance, TestDatastoreTwo)


def test_get_component_named_config_overrides():
    """Test that overrides given in config_overrides with instance name nesting
    take precidence over session.config
    """
    instance_name = "foo"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {instance_name: {"type": "one"}}
    }
    session = setup_session(app_config=app_config_overrides)
    instance = TestFactory.get_component(
        session,
        instance_name,
        config_overrides={
            instance_name: {"type": "two"},
        },
    )
    assert isinstance(instance, TestDatastoreTwo)


####################
## get_connection ##
####################


def test_get_connection_from_component():
    """Test that fetching a connection after constructing the component yields
    a connection which matches the one returned by the component
    """
    instance_name = "test_instance_one"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {instance_name: {"type": "one"}}
    }
    session = setup_session(app_config=app_config_overrides)
    instance = TestFactory.get_component(session, instance_name)
    connection = TestFactory.get_connection(session, instance_name)
    assert connection is not None
    assert connection.to_dict() == instance.get_connection().to_dict()


def test_get_connection_from_config():
    """Test that fetching a connection without instantiating a component pulls
    from the CR config
    """
    instance_name = "test_instance_one"
    input_config = {"foo": True, "bar": 2}
    deploy_config_overrides = {
        "datastores": {
            TestFactory.DATASTORE_TYPE: {
                instance_name: {constants.SPEC_DATASTORE_CONNECTION: input_config}
            }
        }
    }
    session = setup_session(deploy_config=deploy_config_overrides)
    connection = TestFactory.get_connection(session, instance_name)
    assert connection is not None
    assert connection.to_dict() == input_config


def test_get_connection_per_deploy_singleton():
    """Test that fetching a config multiple times results in the exact same
    instance within a single deploy, but different instances between deploys
    """
    instance_name = "test_instance_one"
    input_config_one = {"foo": True, "bar": 1}
    input_config_two = {"foo": False, "bar": 2}
    session = setup_session(
        deploy_config={
            "datastores": {
                TestFactory.DATASTORE_TYPE: {
                    instance_name: {
                        constants.SPEC_DATASTORE_CONNECTION: input_config_one
                    }
                }
            }
        },
    )
    connection_one = TestFactory.get_connection(session, instance_name)
    assert connection_one is not None
    assert connection_one.to_dict() == input_config_one
    connection_two = TestFactory.get_connection(session, instance_name)
    assert id(connection_one) == id(connection_two)

    session = setup_session(
        deploy_config={
            "datastores": {
                TestFactory.DATASTORE_TYPE: {
                    instance_name: {
                        constants.SPEC_DATASTORE_CONNECTION: input_config_two
                    }
                }
            }
        },
    )
    connection_three = TestFactory.get_connection(session, instance_name)
    assert connection_one is not None
    assert connection_three.to_dict() != connection_one.to_dict()


def test_get_connection_unnamed_from_config():
    """Test that fetching an unnamed connection from config works"""
    input_config = {"foo": True, "bar": 2}
    deploy_config_overrides = {
        "datastores": {
            TestFactory.DATASTORE_TYPE: {
                constants.SPEC_DATASTORE_CONNECTION: input_config
            }
        }
    }
    session = setup_session(deploy_config=deploy_config_overrides)
    connection = TestFactory.get_connection(session)
    assert connection is not None
    assert connection.to_dict() == input_config


def test_get_connection_unnamed_from_instance():
    """Test that fetching an unnamed connection from an unnamed instance works"""
    app_config_overrides = {TestFactory.DATASTORE_TYPE: {"type": "one"}}
    session = setup_session(app_config=app_config_overrides)
    instance = TestFactory.get_component(session)
    connection = TestFactory.get_connection(session)
    assert connection is not None
    assert connection.to_dict() == instance.get_connection().to_dict()


def test_get_connection_missing_config():
    """Test that trying to fetch a connection from config asserts if the config
    is missing and no instance was populated from the CR
    """
    session = setup_session()
    with pytest.raises(AssertionError):
        TestFactory.get_connection(session)
    with pytest.raises(AssertionError):
        TestFactory.get_connection(session, "foobar")


def test_get_connection_no_config_or_instance():
    """Test that attempting to fetch a connection where there is no config or
    instance available raises an assertion
    """
    instance_name = "test_instance_one"
    app_config_overrides = {
        TestFactory.DATASTORE_TYPE: {instance_name: {"type": "one"}}
    }
    session = setup_session(app_config=app_config_overrides)
    with pytest.raises(AssertionError):
        TestFactory.get_connection(session, instance_name)


def test_reregister_ok():
    """Make sure that a type can be re-registered without raising. This is
    needed when registration is done in a derived library that uses the PWM and
    therefore re-imports the derived implementation.
    """
    TestFactory.register_type(TestDatastoreOne)
