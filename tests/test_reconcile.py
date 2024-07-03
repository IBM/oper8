"""Tests for the Reconcile class"""

# Standard
from datetime import datetime, timedelta
from unittest import mock
import copy
import json
import os
import pathlib
import sys
import tempfile

# Third Party
import pytest

# First Party
import aconfig
import alog

# Local
from oper8 import Controller, config, constants, exceptions, status
from oper8.dag.completion_state import CompletionState
from oper8.dag.node import Node
from oper8.deploy_manager import DryRunDeployManager, OpenshiftDeployManager
from oper8.exceptions import ConfigError, RolloutError
from oper8.log_format import Oper8JsonFormatter
from oper8.patch import STRATEGIC_MERGE_PATCH
from oper8.reconcile import ReconcileManager, ReconciliationResult, RequeueParams
from oper8.status import ReadyReason, UpdatingReason
from oper8.test_helpers.helpers import (
    DummyController,
    MockDeployManager,
    library_config,
    make_patch,
    setup_cr,
    setup_session,
    setup_vcs_project,
)
from oper8.vcs import VCSMultiProcessError

log = alog.use_channel("TEST")

################################################################################
## Helpers #####################################################################
################################################################################


class AlogConfigureMock:
    def __init__(self):
        self.kwargs = None

    def __call__(self, **kwargs):
        self.kwargs = kwargs


def check_status(
    deploy_manager, cr, ready_reason=None, updating_reason=None, completion_state=None
):
    """Shared helper for checking status after reconcile"""
    obj = deploy_manager.get_obj(
        kind=cr.kind,
        name=cr.metadata.name,
        namespace=cr.metadata.namespace,
        api_version=cr.apiVersion,
    )
    assert obj is not None
    assert obj.get("status")

    ready_cond = status.get_condition(status.READY_CONDITION, obj["status"])
    if ready_reason:
        assert ready_cond
        assert ready_cond["reason"] == ready_reason.value
    else:
        assert not ready_cond

    update_cond = status.get_condition(status.UPDATING_CONDITION, obj["status"])
    if updating_reason:
        assert update_cond
        assert update_cond["reason"] == updating_reason.value
    else:
        assert not update_cond

    if completion_state:
        expected_component_state = status._make_component_state(completion_state)
        for key, values in expected_component_state.items():
            assert obj.status[status.COMPONENT_STATUS][key] == values


def create_temp_patch_annotation(patch_list=[]):
    combined_patch = {}
    for patch in patch_list:
        name = patch["name"]
        api_version = patch.get("api_version", "org.oper8/v1")
        timestamp = patch.get("timestamp", datetime.now())

        combined_patch[name] = {
            "timestamp": timestamp.isoformat(),
            "api_version": api_version,
        }

    return {constants.TEMPORARY_PATCHES_ANNOTATION_NAME: json.dumps(combined_patch)}


class ReconcileDummyController(DummyController):
    def __init__(self, config_defaults=None):
        super().__init__(
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
                {
                    "name": "baz",
                    "api_objects": [("baz", {"kind": "Baz", "apiVersion": "v3"})],
                    "disabled": True,
                },
            ],
            config_defaults=config_defaults,
        )

    def finalize_components(self, session):
        session.deploy_manager.deploy(
            [{"apiVersion": "v1", "kind": "Finalized", "metadata": {"name": "test"}}]
        )


# VCS Helpers
@pytest.fixture
def cleanup_vcs():
    dir = os.getcwd()
    path = sys.path

    yield

    # Reset the directory and sys.path
    os.chdir(dir)
    sys.path = path


def create_module_dir(dir):
    if not config.vcs.module_dir:
        return pathlib.Path(dir)

    mod_dir = pathlib.Path(dir) / config.vcs.module_dir
    mod_dir.mkdir(parents=True, exist_ok=True)
    return mod_dir


################################################################################
## Tests #######################################################################
################################################################################

##################
## Construction ##
##################


def test_construct_defaults():
    """Make sure that a reconcile manager can be constructed with its default args"""

    # Basic Construction
    with library_config():
        rm = ReconcileManager()
        assert rm.home_dir == os.getcwd()
        assert rm.vcs == None

    # VCS Enabled
    with library_config(
        vcs={
            "enabled": True,
            "repo": ".",
            "dest": "dest",
            "checkout_method": "worktree",
        }
    ):
        rm = ReconcileManager()
        assert rm.home_dir == "."
        assert rm.vcs is not None

    # Supported Versions Enabled
    with library_config(strict_versioning=True, supported_versions=[]):
        rm = ReconcileManager()


def test_construct_input_args():
    """Make sure that a reconcile manager can be constructed with given args"""
    ReconcileManager(
        home_dir="/tmp/test",
        deploy_manager=MockDeployManager(),
        enable_vcs=False,
    )


def test_construct_exceptions():
    """Make sure that the proper errors are raised by invalid args"""

    # VCS Exceptions
    with library_config(vcs={"enabled": True}):
        with pytest.raises(ConfigError):
            ReconcileManager()

    with library_config(vcs={"enabled": True, "dest": "str"}):
        with pytest.raises(ConfigError):
            ReconcileManager()

    with library_config(
        vcs={
            "enabled": True,
            "dest": "str",
            "repo": "test",
            "checkout_method": "invalid_checkout",
        }
    ):
        with pytest.raises(ConfigError):
            ReconcileManager()

    # Strict Versioning Exceptions
    with library_config(strict_versioning=True, supported_versions=None):
        with pytest.raises(ConfigError):
            ReconcileManager()

    with library_config(strict_versioning=True, supported_versions=["v1"], vcs={}):
        with pytest.raises(ConfigError):
            ReconcileManager()


########################
## parse_manifest ##
########################
@pytest.mark.parametrize(
    ["resource", "result", "raises"],
    [
        [
            {"apiVersion": "v1", "kind": "Pod"},
            aconfig.Config({"apiVersion": "v1", "kind": "Pod"}),
            False,
        ],
        [
            aconfig.Config({"apiVersion": "v1", "kind": "Pod"}),
            aconfig.Config({"apiVersion": "v1", "kind": "Pod"}),
            False,
        ],
        ["BadValue", None, True],
    ],
)
def test_parse_manifest(resource, result, raises):
    """Ensure the ReconcileManager can parse a manifest"""
    if raises:
        with pytest.raises(ValueError):
            ReconcileManager.parse_manifest(resource)
    else:
        manifest = ReconcileManager.parse_manifest(resource)
        assert manifest == result


########################
## configure_logging ##
########################


def test_configure_logging_no_annotations():
    """Make sure that the default logging configuration is applied"""
    rm = ReconcileManager()
    alog_mock = AlogConfigureMock()
    cr = aconfig.Config({})
    with mock.patch("alog.configure", alog_mock):
        rm.configure_logging(cr, "id")

    assert alog_mock.kwargs is not None
    assert alog_mock.kwargs.get("default_level") == "info"
    assert alog_mock.kwargs.get("filters") == ""
    assert alog_mock.kwargs.get("formatter") == "pretty"
    assert alog_mock.kwargs.get("thread_id") is True


def test_configure_logging_custom_json_formatter():
    """Make sure that the if json logging is enabled, the custom formatter is
    used
    """
    rm = ReconcileManager()
    alog_mock = AlogConfigureMock()
    cr = aconfig.Config(
        aconfig.Config(
            {"metadata": {"annotations": {constants.LOG_JSON_NAME: "true"}}}
        ),
        override_env_vars=False,
    )
    with mock.patch("alog.configure", alog_mock):
        rm.configure_logging(cr, "id")
    assert alog_mock.kwargs is not None
    assert alog_mock.kwargs.get("default_level") == "info"
    assert alog_mock.kwargs.get("filters") == ""
    assert isinstance(alog_mock.kwargs.get("formatter"), Oper8JsonFormatter)
    assert alog_mock.kwargs.get("thread_id") is True


def test_configure_logging_with_annotations():
    """Make sure that logging is properly configured if logging annotations given in the
    CR
    """
    rm = ReconcileManager()
    alog_mock = AlogConfigureMock()
    annos = {
        constants.LOG_DEFAULT_LEVEL_NAME: "debug3",
        constants.LOG_FILTERS_NAME: "FOO:debug",
        constants.LOG_JSON_NAME: "false",
        constants.LOG_THREAD_ID_NAME: "true",
    }
    cr = aconfig.Config({"metadata": {"annotations": annos}})
    with mock.patch("alog.configure", alog_mock):
        rm.configure_logging(cr, "id")
    assert alog_mock.kwargs is not None
    assert (
        alog_mock.kwargs.get("default_level") == annos[constants.LOG_DEFAULT_LEVEL_NAME]
    )
    assert alog_mock.kwargs.get("filters") == annos[constants.LOG_FILTERS_NAME]
    assert alog_mock.kwargs.get("formatter") == "pretty"
    assert alog_mock.kwargs.get("thread_id") is True


################################
## generate_id ##
################################


def test_generate_id_uniq():
    """Make sure that two reconciliation IDs don't match"""
    rm = ReconcileManager()
    assert rm.generate_id() != rm.generate_id()


################################
## setup_vcs ##
################################


@pytest.mark.parametrize(
    ["module_dir", "cr"],
    [
        ["src", setup_cr()],
        [None, setup_cr()],
    ],
)
def test_setup_vcs_success_path(cleanup_vcs, module_dir, cr):
    """Test that setup_vcs correctly updates the path/cwd"""

    # Setup required variables
    with tempfile.TemporaryDirectory() as vcs_directory:
        rm = ReconcileManager()

        # Patch setup_directory as that's tested separately and manually create
        # the module directory
        with library_config(vcs={"module_dir": module_dir, "field": "spec.version"}):
            mod_dir = create_module_dir(vcs_directory)
            rm._setup_directory = mock.Mock(return_value=pathlib.Path(vcs_directory))
            rm.setup_vcs(cr)

        # os.getcwd utilizes the full drive path e.g. /private/tmp/... while sys.path is just
        # the human readable /tmp/....
        assert os.getcwd() == str(mod_dir.resolve())
        assert sys.path[0] == str(mod_dir)


def test_setup_vcs_invalid_cr():
    """Ensure setup_vcs raises exceptions on invalid values"""

    # Setup temp directory and the module directory
    rm = ReconcileManager()
    with pytest.raises(ValueError):
        rm.setup_vcs({})


def test_setup_vcs_invalid_module_dir():
    """Ensure setup_vcs raises exception if improper directory setup"""

    # Setup temp directory and cr but not the module directory
    with tempfile.TemporaryDirectory() as vcs_directory:
        cr = setup_cr()

        rm = ReconcileManager()
        rm._setup_directory = mock.Mock(return_value=pathlib.Path(vcs_directory))

        with pytest.raises(ConfigError):
            rm.setup_vcs(cr)


######################
## setup_controller ##
######################


def test_setup_controller():
    """Test that setup_controller calls its internals correctly"""

    # Setup ReconcileManager with mocks
    rm = ReconcileManager()
    rm._import_controller = mock.Mock(return_value=DummyController)
    rm._configure_controller = mock.Mock(return_value=DummyController())
    rm.setup_controller(DummyController)
    assert rm._import_controller.called
    assert rm._configure_controller.called


##########################
## setup_deploy_manager ##
##########################


@pytest.mark.parametrize(
    ["dry_run", "expected"],
    [
        [True, DryRunDeployManager],
        [False, OpenshiftDeployManager],
    ],
)
def test_setup_deploy_manager(dry_run, expected):
    """Test config settings for deploy manager"""

    rm = ReconcileManager()
    cr = setup_cr()

    with library_config(dry_run=dry_run):
        assert isinstance(rm.setup_deploy_manager(cr), expected)


def test_setup_deploy_manager_override():
    """Ensure the deploy manager passed to the constructor is valid"""

    class TestDM(DryRunDeployManager):
        pass

    dm = TestDM()
    rm = ReconcileManager(deploy_manager=dm)
    cr = setup_cr()

    assert rm.setup_deploy_manager(cr) == dm


################################
## setup_session ##
################################


def test_setup_session():
    """Test setting up a default session"""

    class SessionController(DummyController):
        def get_cr_manifest_defaults(self):
            return aconfig.Config({"spec": {"controllertest": "controllertest"}})

    controller = SessionController()
    cr = setup_cr()
    rm = ReconcileManager()

    rm._get_reconcile_config = mock.Mock(return_value={"test": "test"})
    rm._get_temp_patches = mock.Mock(return_value=[{"test": "test"}])

    session = rm.setup_session(controller, cr, MockDeployManager(), "id")

    assert session.config["test"] == "test"
    assert session.spec["controllertest"] == "controllertest"
    assert session.temporary_patches == [{"test": "test"}]


################################
## run_controller ##
################################


def test_run_controller():
    """Test running a controller"""

    session = setup_session()

    rm = ReconcileManager()
    rm._update_reconcile_start_status = mock.Mock()
    rm._update_reconcile_completion_status = mock.Mock()

    controller = DummyController()
    controller.run_reconcile = mock.Mock(return_value=CompletionState())
    controller.should_requeue = mock.Mock(return_value=(True, None))

    reconcile_result = rm.run_controller(controller, session, is_finalizer=False)

    assert reconcile_result.requeue
    assert reconcile_result.requeue_params.requeue_after == timedelta(minutes=1)
    assert rm._update_reconcile_start_status.called
    assert rm._update_reconcile_completion_status.called


@pytest.mark.parametrize(
    ["should_requeue"],
    [
        [True],
        [False],
    ],
)
def test_run_controller_finalizer(should_requeue):
    """Test running a controller"""

    class FinalizerController(DummyController):
        # Override init to stop overwriting functions
        # with mocks
        def __init__(self):
            Controller.__init__(self)

        def finalize_components(
            self,
            session,
        ):
            pass

        def should_requeue(self, session):
            return should_requeue, None

    session = setup_session()

    rm = ReconcileManager()
    rm._update_reconcile_start_status = mock.Mock()
    rm._update_reconcile_completion_status = mock.Mock()

    controller = FinalizerController()
    controller.run_reconcile = mock.Mock(return_value=CompletionState())
    reconcile_result = rm.run_controller(controller, session, is_finalizer=True)

    assert reconcile_result.requeue == should_requeue
    if should_requeue:
        assert FinalizerController.finalizer in session.finalizers
    else:
        assert FinalizerController.finalizer not in session.finalizers


################################
## _is_paused ##
################################


def test_is_paused():
    """Test checking if a cr is paused"""
    rm = ReconcileManager()
    assert rm._is_paused(
        aconfig.Config(
            {"metadata": {"annotations": {constants.PAUSE_ANNOTATION_NAME: "true"}}}
        )
    )
    assert not rm._is_paused(aconfig.Config({"metadata": {}}))


################################
## _check_strict_versioning ##
################################


def test_check_strict_versioning():
    """Check basic strict versioning functionality"""

    # Create CR, and RM
    cr = setup_cr(version="1.0")
    rm = ReconcileManager()

    # Test valid and invalid paths
    with library_config(supported_versions=["1.0"]):
        rm._check_strict_versioning(cr)

    with library_config(supported_versions=["2.0"]):
        with pytest.raises(ConfigError):
            rm._check_strict_versioning(cr)

    with library_config(supported_versions=["2.0"], vcs={"field": "spec.invalid"}):
        with pytest.raises(ValueError):
            rm._check_strict_versioning(cr)


def test_check_strict_versioning_vcs():
    """Check strict versioning with VCS"""

    # Create CR, and RM
    cr = setup_cr(version="1.0")
    rm = ReconcileManager()
    rm.vcs = mock.Mock()

    # Test both valid and invalid VCS configurations
    with library_config(supported_versions=["1.0"]):
        rm.vcs.list_refs.return_value = ["1.0"]
        rm._check_strict_versioning(cr)

        rm.vcs.list_refs.return_value = ["2.0"]
        with pytest.raises(ConfigError):
            rm._check_strict_versioning(cr)


################################
## _setup_directory ##
################################


@pytest.mark.parametrize(
    ["cr", "version", "template_dir", "expected_dir", "exception"],
    [
        [setup_cr(version="1.0"), "1.0", "/test/{version}", "/test/1.0", None],
        [setup_cr(version="1.0"), None, "/test/{spec[version]}", "/test/1.0", None],
        [
            setup_cr(),
            "1.0",
            "/test/{version}/{kind}/{name}/",
            "/test/1.0/Widget/test_instance",
            None,
        ],
        [
            setup_cr(deploy_config={"list": [{"test": "expected"}, {"test": "wrong"}]}),
            None,
            "/test/{spec[list][0][test]}",
            "/test/expected",
            None,
        ],
        [setup_cr(version="1.0"), "1.0", "/test/{spec[invalidkey]}", None, ConfigError],
    ],
)
def test_setup_directory(cr, version, template_dir, expected_dir, exception):
    """Check that the setup_directory properly formats the vcs_dir"""
    rm = ReconcileManager()
    rm.vcs = mock.Mock()

    with library_config(vcs={"dest": template_dir, "checkout_method": "worktree"}):
        if exception:
            with pytest.raises(exception):
                rm._setup_directory(cr, version)
        else:
            assert str(rm._setup_directory(cr, version)) == expected_dir


################################
## _import_controller ##
################################


@pytest.mark.parametrize(
    ["controller_info", "exception"],
    [
        [DummyController, None],
        ["oper8.test_helpers.helpers.DummyController", None],
        ["abadcontrollerformat", ConfigError],
        ["oper8.test_helpers.helpers.NotPresentController", ConfigError],
        ["oper8.test_helpers.helpers.FailOnce", ConfigError],
        ["oper8.test_helpers.notpresent.FailOnce", ConfigError],
    ],
)
def test_import_controller(controller_info, exception):
    """Test importing a controller"""

    # Get the module name and expected Controller string from the controller_info
    if isinstance(controller_info, str):
        module_name = controller_info.rsplit(".", 1)[0]
        expected_str = controller_info
    else:
        module_name = controller_info.__module__
        expected_str = f"{controller_info.__module__}.{controller_info.__name__}"

    reimported_module = False
    if module_name in sys.modules:
        sys.modules[module_name].imported = True
        reimported_module = True

    # Import Module or ensure import fails
    rm = ReconcileManager()
    if exception:
        with pytest.raises(exception):
            rm._import_controller(controller_info)
        return

    imported_controller = rm._import_controller(controller_info)

    # Assert the class string is as expected and that the object is actually a controller
    assert issubclass(imported_controller, Controller)
    assert (
        f"{imported_controller.__module__}.{imported_controller.__name__}"
        == expected_str
    )

    if reimported_module:
        assert not hasattr(sys.modules[imported_controller.__module__], "imported")


@pytest.mark.parametrize(
    [
        "reimport_controller",
    ],
    [[True], [False]],
)
def test_reimport_controller(reimport_controller):
    """Test the reimport parameter"""

    rm = ReconcileManager(reimport_controller=reimport_controller)

    # There are some race conditions with all of the reimporting in other tests
    # and fixures, so we explicitly import it here to ensure that it's in
    # sys.modules
    # Local
    from oper8.test_helpers.helpers import DummyController

    # Add attribute to module to test if it was successfully reimported
    sys.modules[DummyController.__module__].imported = True

    imported_controller = rm._import_controller(DummyController)

    # If the module should have been reimported then ensure it doesn't have the "imported"
    # attribute
    if reimport_controller:
        assert not hasattr(sys.modules[imported_controller.__module__], "imported")
    else:
        assert sys.modules[imported_controller.__module__].imported


def test_unimport_controller_class():
    """Test that _unimport_controller_class correctly removes all parents,
    siblings, children, and cousins from sys.modules
    """
    mod_name = "foo.bar"
    mods = {
        # Self
        mod_name: 1,
        # Parent
        "foo": 2,
        # Child
        "foo.bar.bif": 3,
        # Sibling
        "foo.baz": 4,
        # Cousin
        "foo.baz.bonk": 5,
        # No relation!
        "asdf": 6,
    }
    with mock.patch("sys.modules", new=copy.copy(mods)):
        unimported_modules = ReconcileManager._unimport_controller_module(mod_name)
        assert unimported_modules == {mod for mod in mods if mod.startswith("foo")}
        assert "asdf" in sys.modules
        assert not any(mod in sys.modules for mod in unimported_modules)


################################
## _configure_controller ##
################################


def test_configure_controller():
    """Test configuring a normal controller"""
    rm = ReconcileManager()
    controller = rm._configure_controller(DummyController)
    assert isinstance(controller, DummyController)


###########################
## _get_reconcile_config ##
###########################


example_reconcile_config = {"foo": 1, "bar": {"baz": 2}}
example_reconcile_config_map = {
    "kind": "ConfigMap",
    "apiVersion": "v1",
    "metadata": {"name": "config-cm"},
    "data": example_reconcile_config,
}


@pytest.mark.parametrize(
    ["cr_manifest", "controller_defaults", "resources", "expected_config", "exception"],
    [
        # Test config with defaults, overrides, or both
        [aconfig.Config({}), aconfig.Config({}), [], aconfig.Config({}), None],
        [
            aconfig.Config({constants.CONFIG_OVERRIDES: example_reconcile_config}),
            aconfig.Config({}),
            [],
            aconfig.Config(example_reconcile_config),
            None,
        ],
        [
            aconfig.Config({}),
            aconfig.Config(example_reconcile_config),
            [],
            aconfig.Config(example_reconcile_config),
            None,
        ],
        [
            aconfig.Config({constants.CONFIG_OVERRIDES: {"foo": 3}}),
            aconfig.Config(example_reconcile_config),
            [],
            aconfig.Config({"foo": 3, "bar": {"baz": 2}}),
            None,
        ],
        # Test get config from configmaps
        [
            aconfig.Config(
                {
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    }
                }
            ),
            aconfig.Config({}),
            [example_reconcile_config_map],
            aconfig.Config(example_reconcile_config),
            None,
        ],
        [
            aconfig.Config(
                {
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    }
                }
            ),
            aconfig.Config({"foo": 3}),
            [example_reconcile_config_map],
            aconfig.Config(example_reconcile_config),
            None,
        ],
        [
            aconfig.Config(
                {
                    constants.CONFIG_OVERRIDES: {"foo": 3},
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    },
                }
            ),
            aconfig.Config({"foo": 0}),
            [example_reconcile_config_map],
            aconfig.Config({"foo": 3, "bar": {"baz": 2}}),
            None,
        ],
        [
            aconfig.Config(
                {
                    constants.CONFIG_OVERRIDES: {"foo": 3},
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    },
                }
            ),
            aconfig.Config({"baz": 0, "foo": 0}),
            [example_reconcile_config_map],
            aconfig.Config({"foo": 3, "baz": 0, "bar": {"baz": 2}}),
            None,
        ],
        # Test _get_reconcile config exceptions
        [
            aconfig.Config(
                {
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    }
                }
            ),
            aconfig.Config({}),
            [],
            aconfig.Config({}),
            ConfigError,
        ],
        [
            aconfig.Config(
                {
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    }
                }
            ),
            aconfig.Config({}),
            [
                {
                    "kind": "ConfigMap",
                    "apiVersion": "v1",
                    "metadata": {"name": "config-cm"},
                }
            ],
            aconfig.Config({}),
            ConfigError,
        ],
        [
            aconfig.Config(
                {
                    "metadata": {
                        "annotations": {
                            constants.CONFIG_DEFAULTS_ANNOTATION_NAME: "config-cm"
                        }
                    }
                }
            ),
            aconfig.Config({}),
            [
                {
                    "kind": "ConfigMap",
                    "apiVersion": "v1",
                    "metadata": {"name": "config-cm"},
                    "data": 123,
                }
            ],
            aconfig.Config({}),
            ConfigError,
        ],
    ],
)
def test_get_reconcile_config(
    cr_manifest, controller_defaults, resources, expected_config, exception
):
    """Test all permutations of generating the reconcile config"""
    dm = MockDeployManager(resources=resources)
    rm = ReconcileManager(deploy_manager=dm)

    if exception:
        with pytest.raises(exception):
            rm._get_reconcile_config(cr_manifest, dm, controller_defaults)
        return

    assert (
        rm._get_reconcile_config(cr_manifest, dm, controller_defaults)
        == expected_config
    )


#######################
## _get_temp_patches ##
#######################


@pytest.mark.parametrize(
    ["cr_manifest", "patches", "exception"],
    [
        # Test CR without patches
        [setup_cr(), [], None],
        # Test generic patch
        [
            setup_cr(
                metadata={
                    "annotations": create_temp_patch_annotation([{"name": "test"}])
                }
            ),
            [
                make_patch(
                    patch_type=STRATEGIC_MERGE_PATCH,
                    body={"foo": {"new": "value"}},
                    name="test",
                    target=setup_cr(),
                )
            ],
            None,
        ],
        # Test patches are parsed in the right order
        [
            setup_cr(
                metadata={
                    "annotations": create_temp_patch_annotation(
                        [
                            {
                                "name": "test-newer",
                                "timestamp": datetime.now() + timedelta(minutes=1),
                            },
                            {"name": "test"},
                        ]
                    )
                }
            ),
            [
                make_patch(
                    patch_type=STRATEGIC_MERGE_PATCH,
                    body={"foo": {"new": "value"}},
                    name="test",
                    target=setup_cr(),
                ),
                make_patch(
                    patch_type=STRATEGIC_MERGE_PATCH,
                    body={"foo": {"new": "value"}},
                    name="test-newer",
                    target=setup_cr(),
                ),
            ],
            None,
        ],
        # Test non json patch
        [
            setup_cr(
                metadata={
                    "annotations": {
                        constants.TEMPORARY_PATCHES_ANNOTATION_NAME: "notjson"
                    }
                }
            ),
            [],
            RolloutError,
        ],
        # Test non dict patch
        [
            setup_cr(
                metadata={
                    "annotations": {
                        constants.TEMPORARY_PATCHES_ANNOTATION_NAME: '[{"not": "a dict"}]'
                    }
                }
            ),
            [],
            RolloutError,
        ],
        # Test invalid timestamp
        [
            setup_cr(
                metadata={
                    "annotations": {
                        constants.TEMPORARY_PATCHES_ANNOTATION_NAME: '{"patch":{"api_version":"org.oper8/v1","timestamp":"invalid"}}'
                    }
                }
            ),
            [],
            RolloutError,
        ],
        # Test invalid api_version
        [
            setup_cr(
                metadata={
                    "annotations": {
                        constants.TEMPORARY_PATCHES_ANNOTATION_NAME: '{"patch":{"timestamp":"2000-01-01T00:00:00"}}'
                    }
                }
            ),
            [],
            RolloutError,
        ],
        # Test temporary patch not found
        [
            setup_cr(
                metadata={
                    "annotations": create_temp_patch_annotation([{"name": "test"}])
                }
            ),
            [],
            ConfigError,
        ],
        [
            setup_cr(
                metadata={
                    "annotations": create_temp_patch_annotation([{"name": "test"}])
                }
            ),
            [
                {
                    **make_patch(
                        patch_type=STRATEGIC_MERGE_PATCH,
                        body={"foo": {"new": "value"}},
                        name="test",
                        target=setup_cr(),
                    ),
                    # Forcibly remove spec from parameter
                    "spec": None,
                }
            ],
            ConfigError,
        ],
    ],
)
def test_get_temp_patches(cr_manifest, patches, exception):
    """Test getting temporary patches"""
    dm = MockDeployManager(resources=patches)
    rm = ReconcileManager(deploy_manager=dm)

    if exception:
        with pytest.raises(exception):
            rm._get_temp_patches(dm, cr_manifest)
        return

    gathered_patches = rm._get_temp_patches(dm, cr_manifest)
    assert len(gathered_patches) == len(patches)
    for index in range(len(patches)):
        assert gathered_patches[index] == patches[index]


#############################
## _update_resource_status ##
#############################


def test_update_resource_status():
    """Test updating resource status"""
    dm = MockDeployManager()
    the_api_version = "my.api/v123alpha0"
    cr = setup_cr(api_version=the_api_version)
    rm = ReconcileManager(deploy_manager=dm)
    with mock.patch("oper8.status.update_resource_status") as Mock:
        rm._update_resource_status(dm, cr, current_status={"test": "status"})
        Mock.assert_called_with(
            dm,
            cr.kind,
            the_api_version,
            cr.metadata.name,
            cr.metadata.namespace,
            current_status={"test": "status"},
        )


####################################
## _update_reconcile_start_status ##
####################################


@pytest.mark.parametrize(
    ["cr", "ready_reason", "updating_reason"],
    [
        # Test initializing condition
        [setup_cr(), ReadyReason.INITIALIZING, None],
        # Test version update conditions
        [
            setup_cr(
                version="2.0",
                status={
                    "conditions": [
                        status._make_ready_condition(
                            ReadyReason.STABLE, "", datetime.now()
                        )
                    ],
                    "versions": {"reconciled": "1.0"},
                },
            ),
            ReadyReason.IN_PROGRESS,
            UpdatingReason.VERSION_CHANGE,
        ],
        # Test that the RM passes through conditions if not version_change/initializing
        [
            setup_cr(
                version="1.0",
                status={
                    "conditions": [
                        status._make_ready_condition(
                            ReadyReason.STABLE, "", datetime.now()
                        ),
                        status._make_updating_condition(
                            UpdatingReason.STABLE, "", datetime.now()
                        ),
                    ],
                    "versions": {"reconciled": "1.0"},
                },
            ),
            ReadyReason.STABLE,
            UpdatingReason.STABLE,
        ],
        [
            setup_cr(
                version="1.0",
                status={
                    "conditions": [
                        status._make_ready_condition(
                            ReadyReason.CONFIG_ERROR, "", datetime.now()
                        ),
                        status._make_updating_condition(
                            UpdatingReason.ERRORED, "", datetime.now()
                        ),
                    ],
                    "versions": {"reconciled": "1.0"},
                },
            ),
            ReadyReason.CONFIG_ERROR,
            UpdatingReason.ERRORED,
        ],
    ],
)
def test_update_reconcile_start_status(cr, ready_reason, updating_reason):
    """Test correctly updating reconcile start status"""
    dm = MockDeployManager(resources=[cr])
    rm = ReconcileManager(deploy_manager=dm)
    session = setup_session(full_cr=cr, deploy_manager=dm)

    rm._update_reconcile_start_status(session)

    check_status(dm, cr, ready_reason, updating_reason)


###############
## _update_reconcile_completion_status ##
###############


@pytest.mark.parametrize(
    ["cr", "completion_state", "ready_reason", "updating_reason"],
    [
        # Test stable on successful reconcile
        [
            setup_cr(),
            CompletionState(verified_nodes=[Node("test")]),
            ReadyReason.STABLE,
            UpdatingReason.STABLE,
        ],
        # Test conditions on incomplete verification
        [
            setup_cr(),
            CompletionState(unverified_nodes=[Node("test")]),
            ReadyReason.IN_PROGRESS,
            UpdatingReason.VERIFY_WAIT,
        ],
        [
            setup_cr(
                status={
                    "conditions": [
                        status._make_ready_condition(
                            ReadyReason.INITIALIZING, "", datetime.now()
                        )
                    ]
                }
            ),
            CompletionState(unverified_nodes=[Node("test")]),
            ReadyReason.INITIALIZING,
            UpdatingReason.VERIFY_WAIT,
        ],
    ],
)
def test_update_reconcile_completion_status(
    cr, completion_state, ready_reason, updating_reason
):
    """Test updating a resource status with a completion state"""
    dm = MockDeployManager(resources=[cr])
    rm = ReconcileManager(deploy_manager=dm)
    session = setup_session(full_cr=cr, deploy_manager=dm)

    rm._update_reconcile_completion_status(session, completion_state)

    check_status(dm, cr, ready_reason, updating_reason, completion_state)


###############
## _update_error_status ##
###############


@pytest.mark.parametrize(
    ["cr", "exception", "ready_reason", "updating_reason"],
    [
        # Test stable on successful reconcile
        [
            setup_cr(),
            exceptions.PreconditionError(),
            None,
            UpdatingReason.PRECONDITION_WAIT,
        ],
        [setup_cr(), exceptions.VerificationError(), None, UpdatingReason.VERIFY_WAIT],
        [setup_cr(), exceptions.Oper8ExpectedError(), None, UpdatingReason.VERIFY_WAIT],
        [
            setup_cr(),
            exceptions.ConfigError(),
            ReadyReason.CONFIG_ERROR,
            UpdatingReason.ERRORED,
        ],
        [setup_cr(), exceptions.ClusterError(), None, UpdatingReason.CLUSTER_ERROR],
        [
            setup_cr(),
            exceptions.RolloutError(),
            ReadyReason.ERRORED,
            UpdatingReason.ERRORED,
        ],
        [
            setup_cr(),
            exceptions.Oper8FatalError(),
            ReadyReason.ERRORED,
            UpdatingReason.ERRORED,
        ],
        [setup_cr(), ValueError(), ReadyReason.ERRORED, UpdatingReason.ERRORED],
    ],
)
def test_update_error_status(cr, exception, ready_reason, updating_reason):
    """Test updating the CR status on error"""
    dm = MockDeployManager(resources=[cr])
    rm = ReconcileManager(deploy_manager=dm)
    rm._update_error_status(cr, exception)
    check_status(dm, cr, ready_reason, updating_reason)


###############
## reconcile ##
###############


@pytest.mark.parametrize(
    ["controller_info", "cr", "is_finalizer"],
    [
        # Test controller_info as both strings and type
        [ReconcileDummyController, setup_cr(), False],
        [
            f"{ReconcileDummyController.__module__}.{ReconcileDummyController.__name__}",
            setup_cr(),
            False,
        ],
        # Test finalizer
        [
            ReconcileDummyController,
            setup_cr(
                metadata={
                    "finalizers": [ReconcileDummyController.finalizer],
                    "deletionTimestamp": datetime.now(),
                }
            ),
            True,
        ],
    ],
)
def test_reconcile(controller_info, cr, is_finalizer):
    """Test that a reconciliation with several components that complete cleanly
    exists correctly and sets status conditions to STABLE
    """
    dm = MockDeployManager(resources=[cr])
    rm = ReconcileManager(deploy_manager=dm)

    result = rm.reconcile(controller_info, cr, is_finalizer)
    assert not result.requeue

    if is_finalizer:
        assert dm.has_obj(kind="Finalized", name="test", api_version="v1")
        assert not dm.has_obj(
            kind=cr.kind,
            api_version=cr.apiVersion,
            name=cr.metadata.name,
            namespace=cr.metadata.namespace,
        )
    else:
        assert dm.has_obj(
            kind="Foo", name="foo", api_version="v1", namespace=cr.metadata.namespace
        )
        assert dm.has_obj(
            kind="Bar", name="bar", api_version="v2", namespace=cr.metadata.namespace
        )

        # make sure status gets STABLE
        #   and requeue won't be required if status reaches STABLE
        check_status(dm, cr, ReadyReason.STABLE, UpdatingReason.STABLE)


def test_reconcile_paused():
    """Reconcile is not ran for a paused CR"""
    cr = setup_cr(metadata={"annotations": {constants.PAUSE_ANNOTATION_NAME: "true"}})
    dm = MockDeployManager(resources=[cr])
    rm = ReconcileManager(deploy_manager=dm)
    rm.run_controller = mock.Mock()

    result = rm.reconcile(ReconcileDummyController, cr, False)

    assert result == ReconciliationResult(False, RequeueParams())
    assert not rm.run_controller.called


@pytest.mark.parametrize(
    ["cr", "supported_versions", "exception"],
    [
        [setup_cr(version="1.0"), ["1.0"], None],
        [setup_cr(version="2.0"), ["1.0"], ConfigError],
    ],
)
def test_reconcile_strict_versioning(cr, supported_versions, exception):
    """Test reconcile is stopped by strict versioning"""

    with library_config(strict_versioning=True, supported_versions=supported_versions):
        dm = MockDeployManager(resources=[cr])
        rm = ReconcileManager(deploy_manager=dm)

        if exception:
            with pytest.raises(exception):
                rm.reconcile(ReconcileDummyController, cr)
        else:
            rm.reconcile(ReconcileDummyController, cr)


@pytest.mark.parametrize(
    ["module_dir"],
    [["spec"], ["adifferentdir"]],
)
def test_reconcile_vcs(cleanup_vcs, module_dir):
    """Full VCS Integration Test utilizing a temporary git directory"""

    with tempfile.TemporaryDirectory() as vcs_directory:
        checkout_path = f"{vcs_directory}/versions/{{version}}"
        setup_vcs_project(
            components=ReconcileDummyController().component_specs,
            directory=vcs_directory,
            module_dir=module_dir,
            module_name="test_module",
        )

        cr = setup_cr()

        with library_config(
            vcs={
                "enabled": True,
                "field": "spec.version",
                "repo": vcs_directory,
                "dest": checkout_path,
                "module_dir": module_dir,
                "checkout_method": "worktree",
            }
        ):
            dm = MockDeployManager(resources=[cr])
            rm = ReconcileManager(deploy_manager=dm)

            result = rm.reconcile("test_module.Controller", cr)

        assert dm.has_obj(
            kind="Foo", name="foo", api_version="v1", namespace=cr.metadata.namespace
        )
        assert dm.has_obj(
            kind="Bar", name="bar", api_version="v2", namespace=cr.metadata.namespace
        )

        # make sure status gets STABLE
        #   and requeue won't be required if status reaches STABLE
        check_status(dm, cr, ReadyReason.STABLE, UpdatingReason.STABLE)
        assert not result.requeue


####################
## Safe Reconcile ##
####################


def test_safe_reconcile():
    """Test that a safe_reconcile handles all exception types"""
    cr = setup_cr()
    rm = ReconcileManager()
    rm.reconcile = mock.Mock()

    with library_config(manage_status=False):
        # Test a successful reconcile
        good_result = ReconciliationResult(False, RequeueParams())
        rm.reconcile.return_value = good_result
        assert good_result == rm.safe_reconcile(DummyController, cr)

        # Test a VCS error
        rm.reconcile.side_effect = VCSMultiProcessError()
        result = rm.safe_reconcile(DummyController, cr)
        assert result.requeue
        assert result.requeue_params.requeue_after > timedelta(
            seconds=5
        ) and result.requeue_params.requeue_after < timedelta(seconds=10)

        # Test a generic exception
        rm.reconcile.side_effect = RolloutError()
        result = rm.safe_reconcile(DummyController, cr)
        assert result.requeue
        assert result.requeue_params.requeue_after == timedelta(seconds=60)


def test_safe_reconcile_status():
    """Test a safe reconcile correctly updates the error status"""
    cr = setup_cr()
    dm = MockDeployManager(resources=[cr])
    rm = ReconcileManager(deploy_manager=dm)
    rm.reconcile = mock.Mock(side_effect=RolloutError())
    rm._update_error_status = mock.Mock()

    # Test successful status update
    assert rm.safe_reconcile(DummyController, cr)

    # Test safe reconcile when error in updating status
    rm._update_error_status.side_effect = RolloutError()
    assert rm.safe_reconcile(DummyController, cr)


#################
## Data models ##
#################


def test_create_requeue_params_default():
    """Make sure RequeueParams can be constructed by empty args"""
    p = RequeueParams()
    assert p.requeue_after == timedelta(seconds=60)

    with library_config(requeue_after_seconds=1):
        p = RequeueParams()
        assert p.requeue_after == timedelta(seconds=1)


def test_create_requeue_params_args():
    """Make sure requeue parameters can be constructed with args"""
    p = RequeueParams(requeue_after=timedelta(seconds=100))
    assert p.requeue_after == timedelta(seconds=100)


def test_create_reconcile_request_defaults():
    """Make sure reconcile request can be constructed by empty args"""
    rr = ReconciliationResult(True)
    assert rr.requeue
    assert rr.requeue_params.requeue_after == timedelta(seconds=60)
    assert rr.exception is None

    with library_config(requeue_after_seconds=1):
        rr = ReconciliationResult(False)

    assert not rr.requeue
    assert rr.requeue_params.requeue_after == timedelta(seconds=1)
    assert rr.exception is None


def test_create_reconcile_request_args():
    """Make sure reconcile request can be constructed with args"""
    p = RequeueParams(requeue_after=timedelta(seconds=100))
    rr = ReconciliationResult(True, p, ValueError())

    assert rr.requeue
    assert rr.requeue_params.requeue_after == timedelta(seconds=100)
    assert isinstance(rr.exception, ValueError)
