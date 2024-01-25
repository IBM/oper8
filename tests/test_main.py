"""
Tests for the __main__.py entrypoint to the library as an executable
"""

# Standard
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock
import os
import sys
import tempfile

# Third Party
import pytest
import yaml

# First Party
import alog

# Local
from oper8 import Component, Controller, component, config, controller, watch_manager
from oper8.__main__ import main
from oper8.test_helpers.helpers import (
    ModuleExit,
    TestRecorder,
    library_config,
    mock_sys_exit,
)
from oper8.watch_manager.python_watch_manager.threads.heartbeat import HeartbeatThread

log = alog.use_channel("TEST")


## Sample Application ##########################################################
#
# NOTE: We don't use DummyController so that main doesn't try to register a
#   watch for it automatically
##


@component("foo")
class FooComponent(Component):

    CONSTRUCTED = False
    FOUND_EXTERNAL_RESOURCE = False

    def __init__(self, session):
        super().__init__(session=session)
        self.add_resource(
            self.name, {"kind": "Foo", "apiVersion": "v1", "metadata": {"name": "foo"}}
        )
        self.__class__.CONSTRUCTED = True

    def build_chart(self, session):
        if session.spec.do_check:
            log.debug("Doing the external resource check")
            success, content = session.get_object_current_state(
                kind="External",
                name="external-thing",
            )
            assert success
            if content is not None:
                self.__class__.FOUND_EXTERNAL_RESOURCE = True

    @classmethod
    def reset(cls):
        cls.CONSTRUCTED = False
        cls.FOUND_EXTERNAL_RESOURCE = False


@component("bar")
class BarComponent(Component):

    CONSTRUCTED = False

    def __init__(self, session):
        super().__init__(session=session)
        self.add_resource(
            self.name, {"kind": "Bar", "apiVersion": "v1", "metadata": {"name": "bar"}}
        )
        self.__class__.CONSTRUCTED = True

    @classmethod
    def reset(cls):
        cls.CONSTRUCTED = False


@component("subsystem")
class SubsystemComponent(Component):

    CONSTRUCTED = False

    def __init__(self, session):
        super().__init__(session=session)
        self.add_resource(
            self.name,
            {
                "kind": "Subsystem",
                "apiVersion": "foo.bar/v1",
                "metadata": {"name": "subsystem", "namespace": "default"},
            },
        )
        self.__class__.CONSTRUCTED = True

    @classmethod
    def reset(cls):
        cls.CONSTRUCTED = False


@controller(group="foo.bar", version="v1", kind="Subsystem")
class SubsystemController(Controller):

    CONSTRUCTED = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("Setting up %s", self)
        self.__class__.CONSTRUCTED = True

    def setup_components(self, session):
        FooComponent(session)

    @classmethod
    def reset(cls):
        cls.CONSTRUCTED = False


@controller(group="foo.bar", version="v1", kind="Parent")
class ParentController(Controller):

    CONSTRUCTED = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("Setting up %s", self)
        self.__class__.CONSTRUCTED = True

    def setup_components(self, session):
        BarComponent(session)
        SubsystemComponent(session)

    @classmethod
    def reset(cls):
        cls.CONSTRUCTED = False


## Helpers #####################################################################


@pytest.fixture(autouse=True)
def reset_sys_argv():
    """All tests need to muck with sys.argv, so this fixture will reset it to an
    empty list before each runs
    """
    with mock.patch.object(sys, "argv", ["oper8"]):
        yield


@pytest.fixture(autouse=True)
def reset():
    """Between tests we want to reset the app classes"""
    SubsystemController.reset()
    ParentController.reset()
    FooComponent.reset()
    BarComponent.reset()
    SubsystemComponent.reset()
    watch_manager.WatchManagerBase._ALL_WATCHES = {}


@pytest.fixture
def recorder():
    """All error tests need to capture sys.exit with a TestRecorder"""
    recorder = TestRecorder()
    with mock_sys_exit(recorder):
        yield recorder


## Happy Path Tests ############################################################


def test_controller_types_found():
    """Make sure that all controller types are found within the module"""
    sys.argv.extend(["--module_name", __name__])
    with library_config(dry_run=True):
        main()
    assert ParentController.CONSTRUCTED
    assert SubsystemController.CONSTRUCTED
    assert not FooComponent.CONSTRUCTED
    assert not BarComponent.CONSTRUCTED
    assert not SubsystemComponent.CONSTRUCTED


def test_single_controller_type_found():
    """Make sure that only the provided controller type is found within the module"""
    sys.argv.extend(["--module_name", __name__])
    with library_config(dry_run=True, controller_name="SubsystemController"):
        main()
    assert not ParentController.CONSTRUCTED
    assert SubsystemController.CONSTRUCTED
    assert not FooComponent.CONSTRUCTED
    assert not BarComponent.CONSTRUCTED
    assert not SubsystemComponent.CONSTRUCTED


def test_dry_run_reconcile_recursion():
    """Make sure that a reconciliation can run with a --cr is given and that it
    will recurse to the subsystem
    """
    with tempfile.NamedTemporaryFile() as cr_file:
        cr_file.write(
            yaml.safe_dump(
                {
                    "apiVersion": "foo.bar/v1",
                    "kind": "Parent",
                    "metadata": {
                        "name": "test",
                        "namespace": "test",
                    },
                }
            ).encode("utf-8")
        )
        cr_file.flush()
        sys.argv.extend(["--module_name", __name__, "--cr", cr_file.name])
        with library_config(dry_run=True):
            main()
        assert ParentController.CONSTRUCTED
        assert SubsystemController.CONSTRUCTED
        assert FooComponent.CONSTRUCTED
        assert BarComponent.CONSTRUCTED
        assert SubsystemComponent.CONSTRUCTED


def test_resource_dir():
    """Make sure that a --resource_dir can be specified and that valid yaml
    files are parsed form the directory
    """
    with tempfile.TemporaryDirectory() as resource_dir:

        # Put a valid yaml file for the "external" resource in the resource dir
        with open(os.path.join(resource_dir, "external.yaml"), "w") as handle:
            handle.write(
                yaml.safe_dump(
                    {
                        "apiVersion": "something.external",
                        "kind": "External",
                        "metadata": {
                            "name": "external-thing",
                            "namespace": "test",
                        },
                    }
                )
            )

        # Put another non-yaml file in there too to make sure that only valid
        # yaml is read
        with open(os.path.join(resource_dir, "README.md"), "w") as handle:
            handle.write("# External Stuff\nThis has some external stuff!")

        with tempfile.NamedTemporaryFile("w") as cr_file:
            cr_file.write(
                yaml.safe_dump(
                    {
                        "apiVersion": "foo.bar/v1",
                        "kind": "Subsystem",
                        "metadata": {
                            "name": "test",
                            "namespace": "test",
                        },
                        "spec": {"do_check": True},
                    }
                )
            )
            cr_file.flush()
            sys.argv.extend(
                [
                    "--module_name",
                    __name__,
                    "--cr",
                    cr_file.name,
                    "--resource_dir",
                    resource_dir,
                ]
            )
            with library_config(dry_run=True):
                main()
            assert SubsystemController.CONSTRUCTED
            assert FooComponent.CONSTRUCTED
            assert FooComponent.FOUND_EXTERNAL_RESOURCE


def test_supported_versions():
    """Make sure that the list of supported versions can be set via a command
    line argument
    """
    sys.argv.extend(
        [
            "--module_name",
            __name__,
            "--supported_versions",
            "foo",
            "bar",
        ]
    )
    with library_config(dry_run=True):
        main()
        assert config.supported_versions == ["foo", "bar"]


def test_dry_run_arg():
    """Make sure that the --dry_run arg works as expected"""
    sys.argv.extend(
        [
            "--module_name",
            __name__,
            "--dry_run",
        ]
    )
    main()


## Error Case Tests ############################################################


def test_module_name_required(recorder):
    """Make sure the --module_name argument is required"""
    with pytest.raises(ModuleExit):
        main()
    assert recorder.EXITED in recorder.events
    assert recorder.retcode not in [None, 0]


def test_resource_dir_not_found(recorder):
    """Make sure the --resource_dir argument is required to point to a valid
    directory
    """
    sys.argv.extend(["--module_name", __name__, "--resource_dir", "some/bad/path"])
    with library_config(dry_run=True):
        with pytest.raises(AssertionError):
            main()


def test_resource_dir_to_file(recorder):
    """Make sure the --resource_dir argument cannot point to a file"""
    with tempfile.NamedTemporaryFile("w") as handle:
        handle.write("some stuff")
        handle.flush()
        sys.argv.extend(["--module_name", __name__, "--resource_dir", handle.name])
        with library_config(dry_run=True):
            with pytest.raises(AssertionError):
                main()


def test_cr_not_found(recorder):
    """Make sure the --cr argument must point to a file that exists"""
    sys.argv.extend(["--module_name", __name__, "--cr", "some/bad/file.yaml"])
    with library_config(dry_run=True):
        with pytest.raises(AssertionError):
            main()


def test_cr_not_yaml(recorder):
    """Make sure the --cr argument must point to a file that is valid yaml"""
    with tempfile.NamedTemporaryFile("w") as handle:
        handle.write("{not\nyaml\n  really")
        handle.flush()
        sys.argv.extend(["--module_name", __name__, "--cr", handle.name])
        with library_config(dry_run=True):
            with pytest.raises(yaml.parser.ParserError):
                main()


def test_cr_to_dir(recorder):
    """Make sure the --cr argument cannot point to a directory"""
    with tempfile.TemporaryDirectory() as some_dir:
        sys.argv.extend(["--module_name", __name__, "--cr", some_dir])
        with library_config(dry_run=True):
            with pytest.raises(AssertionError):
                main()


def test_cr_without_dry_run(recorder):
    """Make sure the --cr argument can only be specified in dry_run"""
    with tempfile.NamedTemporaryFile("w") as handle:
        handle.write(yaml.safe_dump({"foo": "bar"}))
        handle.flush()
        sys.argv.extend(["--module_name", __name__, "--cr", handle.name])
        with library_config(dry_run=False):
            with pytest.raises(AssertionError):
                main()


def test_resource_dir_without_dry_run(recorder):
    """Make sure the --resource_dir argument can only be specified in dry_run"""
    with tempfile.TemporaryDirectory() as some_dir:
        sys.argv.extend(["--module_name", __name__, "--resource_dir", some_dir])
        with library_config(dry_run=False):
            with pytest.raises(AssertionError):
                main()


def test_invalid_single_controller_type_not_found():
    """Ensure that an AttributeError is thrown when given an invalid controller"""
    sys.argv.extend(["--module_name", __name__])

    with library_config(dry_run=False, controller_name="FooBar"):
        with pytest.raises(AttributeError):
            main()
    assert not ParentController.CONSTRUCTED
    assert not SubsystemController.CONSTRUCTED
    assert not FooComponent.CONSTRUCTED
    assert not BarComponent.CONSTRUCTED
    assert not SubsystemComponent.CONSTRUCTED


############################### Health Check CMD ###############################
def test_valid_health_check():
    """Ensure that the health check"""
    with tempfile.NamedTemporaryFile() as named_temp_file:
        file_path = Path(named_temp_file.name)
        file_path.write_text(datetime.now().strftime(HeartbeatThread._DATE_FORMAT))

        sys.argv.extend(["check-heartbeat", "--file", str(file_path), "--delta", "120"])
        main()


def test_to_old_health_check():
    """Ensure that the health check"""
    with tempfile.NamedTemporaryFile() as named_temp_file:
        file_path = Path(named_temp_file.name)

        old_time = datetime.now() - timedelta(seconds=100)
        file_path.write_text(old_time.strftime(HeartbeatThread._DATE_FORMAT))

        sys.argv.extend(["check-heartbeat", "--file", str(file_path), "--delta", "10"])
        with pytest.raises(KeyError):
            main()


def test_no_file_health_check():
    """Ensure that the health check"""
    sys.argv.extend(["check-heartbeat", "--file", "/some/file", "--delta", "10"])
    with pytest.raises(FileNotFoundError):
        main()
