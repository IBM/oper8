"""
Test the top-level ansible module
"""

# Standard
from contextlib import contextmanager
from unittest import mock
import functools
import os
import tempfile

# Third Party
from openshift.dynamic import DynamicClient, EagerDiscoverer
import pytest

# First Party
import alog

# Local
from oper8 import Controller, constants
from oper8.log_format import Oper8JsonFormatter
from oper8.test_helpers.helpers import (
    DummyController,
    ModuleExit,
    TestRecorder,
    maybe_temp_dir,
    mock_config_file,
    mock_sys_exit,
)
from oper8.test_helpers.kub_mock import mock_kub_client_constructor
from oper8.utils import merge_configs

## Helpers #####################################################################

log = alog.use_channel("TEST")


DEFAULT_CONTROLLER_IMPORT = "test_k8s_application.TestController"


class AppTestRecorder(TestRecorder):
    def __init__(self, mock_client=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_client = (
            DynamicClient(mock_client, discoverer=EagerDiscoverer)
            if mock_client
            else None
        )


@contextmanager
def mock_app_calls(recorder, module_params, patch_controller_class):
    if not patch_controller_class:
        yield
    else:
        import_name = module_params.get("controller_class", DEFAULT_CONTROLLER_IMPORT)

        # Disable reimporting the controller so mocks work
        with mock.patch(
            "oper8.reconcile.ReconcileManager.__init__.__defaults__",
            (None, None, None, False),
        ):

            with mock.patch(
                ".".join([import_name, "log_constructed"]),
                lambda _, config_defaults: recorder.add_event(
                    recorder.CONSTRUCTED, config_defaults
                ),
            ):
                with mock.patch(
                    ".".join([import_name, "setup_components"]),
                    lambda _, deploy_config: recorder.add_event(
                        recorder.SETUP_COMPONENTS, deploy_config
                    ),
                ):
                    with mock.patch(
                        ".".join([import_name, "finalize_components"]),
                        lambda _, deploy_config: recorder.add_event(
                            recorder.FINALIZE_COMPONENTS, deploy_config
                        ),
                    ):
                        log.debug("Yielding from patched app calls")
                        yield


DEFAULT_VERSION = "v1.2.3.test"


def make_cr(overrides=None):
    overrides = overrides or {}
    return merge_configs(
        {
            "apiVersion": "test.foo.bar.com/v1",
            "kind": "Foo",
            "metadata": {
                "name": "foo",
                "namespace": "bar",
                "uid": "12345678",
            },
            "spec": {"version": DEFAULT_VERSION},
        },
        overrides,
    )


class TestController(Controller):
    """We aren't testing the controller stack, so this is a minimal stub"""

    group = "test.com"
    version = "v2"
    kind = "Test"

    # This is not a test class!
    __test__ = False

    def __init__(self, config_defaults=None, **kwargs):
        super().__init__(config_defaults=config_defaults, **kwargs)
        self.log_constructed(config_defaults)

    def log_constructed(self, config_defaults):
        pass

    def setup_components(self, session):
        pass

    def finalize_components(self, session):
        pass


class BrokenTestController(TestController):
    """Test controller that will fail during rollout"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def reconcile(self, *args, **kwargs):
        raise RuntimeError("Oh no!")

    def log_constructed(self, config_defaults):
        pass

    def setup_components(self, session):
        pass

    def finalize_components(self, session):
        pass


class TestControllerWithResources(DummyController):
    """Dummy controller that deploys some api resources"""

    # This is not a test class!
    __test__ = False

    def log_constructed(self, config_defaults):
        pass

    def setup_components(self, session):
        pass

    def finalize_components(self, session):
        pass

    def __init__(self, **kwargs):
        super().__init__(
            components=[
                {
                    "name": "foo-comp",
                    "api_objects": [
                        ("foo", {"kind": "Foo", "metadata": {"namespace": "bar"}})
                    ],
                }
            ],
            **kwargs,
        )

    def setup_components(self, session):
        log.debug("TestControllerWithResources.setup_components")
        super().setup_components(session)


def mock_alog_configure(rec, *args, **kwargs):
    rec.add_event("alog_configure", (args, kwargs))


def run_module(
    module_params=None,
    add_module_param_defaults=True,
    cr_overrides=None,
    config_defaults=None,
    recorder=None,
    cluster_state=None,
    mock_logging=True,
    patch_controller_class=True,
):
    # Local
    from oper8.watch_manager.ansible_watch_manager.modules.k8s_application import main

    with maybe_temp_dir() as temp_dir:
        log.info("Temp Dir: %s", temp_dir)

        cr = make_cr(cr_overrides)

        # Add the CR to the default cluster state
        cluster_state = cluster_state or {}
        cluster_state.setdefault(cr["metadata"].get("namespace"), {}).setdefault(
            cr["kind"], {}
        ).setdefault(cr["apiVersion"], {}).setdefault(cr["metadata"].get("name"), cr)

        # If this is a dry run, go with it, otherwise mock the k8s api
        with mock_kub_client_constructor(cluster_state=cluster_state) as mock_client:

            # Mock sys.exit and keep track of the exit code
            log.info("Mocking sys.exit")
            recorder = recorder or AppTestRecorder(mock_client)
            with mock_sys_exit(recorder):

                # Add the defaults to send output to the temp dir
                module_params = module_params or {}
                log.info("Module Params: %s", module_params)
                if add_module_param_defaults:
                    module_params["working_dir"] = os.path.join(temp_dir, "working")
                    module_params.setdefault(
                        "controller_class",
                        DEFAULT_CONTROLLER_IMPORT,
                    )
                    module_params.setdefault("version", DEFAULT_VERSION)
                    module_params.setdefault("full_cr", str(cr))

                # Wire the recorder into the test app
                log.info("Mocking app calls")
                with mock_app_calls(recorder, module_params, patch_controller_class):

                    # Create the config file for the module
                    config_object = {"ANSIBLE_MODULE_ARGS": module_params}

                    # Inject the config file as sys.argv[1]
                    log.info("Mocking the config file input")
                    with mock_config_file(config_object):

                        # run it!
                        try:
                            log.info("------------------")
                            if mock_logging:
                                with mock.patch(
                                    "alog.configure",
                                    functools.partial(mock_alog_configure, recorder),
                                ):
                                    main()
                            else:
                                main()
                        except ModuleExit:
                            pass

                        # Merge events from controller
                        return recorder


## Happy Path ##################################################################


@pytest.mark.ansible
def test_run_module_reconcile():
    """Test that a straightforward execution of the module works as expected"""
    recorder = run_module()
    assert recorder.retcode == 0
    assert recorder.events == [
        "alog_configure",
        AppTestRecorder.CONSTRUCTED,
        AppTestRecorder.SETUP_COMPONENTS,
        AppTestRecorder.EXITED,
    ]


@pytest.mark.ansible
def test_run_module_finalize():
    """Test that a straightforward execution of the module as a finalizer works
    as expected
    """
    recorder = run_module(module_params={"operation": "remove"})
    assert recorder.retcode == 0
    assert recorder.events == [
        "alog_configure",
        AppTestRecorder.CONSTRUCTED,
        AppTestRecorder.FINALIZE_COMPONENTS,
        AppTestRecorder.EXITED,
    ]


@pytest.mark.ansible
def test_paused():
    """Test if the paused annotation is present, the controller is not executed"""
    recorder = run_module(
        cr_overrides={
            "metadata": {"annotations": {constants.PAUSE_ANNOTATION_NAME: "true"}}
        }
    )
    assert recorder.retcode == 0
    assert recorder.events == ["alog_configure", AppTestRecorder.EXITED]


@pytest.mark.ansible
def test_log_config():
    recorder = AppTestRecorder()
    annotations = {
        constants.LOG_DEFAULT_LEVEL_NAME: "debug1",
        constants.LOG_FILTERS_NAME: "FOO:debug4",
        constants.LOG_THREAD_ID_NAME: "true",
        constants.LOG_JSON_NAME: "true",
    }

    with mock.patch("alog.configure", functools.partial(mock_alog_configure, recorder)):
        recorder = run_module(
            recorder=recorder, cr_overrides={"metadata": {"annotations": annotations}}
        )
        assert recorder.retcode == 0
        assert "alog_configure" in recorder.events
        config_args, config_kwargs = recorder.event_details["alog_configure"]
        assert not config_args
        assert (
            config_kwargs["default_level"]
            == annotations[constants.LOG_DEFAULT_LEVEL_NAME]
        )
        assert config_kwargs["filters"] == annotations[constants.LOG_FILTERS_NAME]
        assert config_kwargs["thread_id"] == True
        assert isinstance(config_kwargs["formatter"], Oper8JsonFormatter)


@pytest.mark.ansible
def test_log_file():
    """Make sure that setting a log_file results in a callable handler_generator"""
    recorder = AppTestRecorder()
    with mock.patch("alog.configure", functools.partial(mock_alog_configure, recorder)):
        with tempfile.TemporaryDirectory() as temp_log_dir:
            recorder = run_module(
                recorder=recorder,
                module_params={"log_file": os.path.join(temp_log_dir, "foo.bar.log")},
            )
            assert recorder.retcode == 0
            assert "alog_configure" in recorder.events
            config_args, config_kwargs = recorder.event_details["alog_configure"]
            assert not config_args
            assert config_kwargs["handler_generator"] is not None
            assert callable(config_kwargs["handler_generator"])


## Error Cases #################################################################


@pytest.mark.ansible
def test_required_args():
    """Test missing required arguments are caught and cause a non-zero exit"""
    recorder = run_module(
        add_module_param_defaults=False, module_params={"full_cr": "{}"}
    )
    assert recorder.retcode != 0


@pytest.mark.ansible
def test_unparsable_full_cr():
    """Test an un-parsable value for a full_cr exits with an error"""
    recorder = run_module(module_params={"full_cr": "not a dict"})
    assert recorder.retcode != 0


@pytest.mark.ansible
def test_incomplete_full_cr():
    """Test a CR which doesn't have the kubernetes basics"""
    recorder = run_module(module_params={"full_cr": "{}"})
    assert recorder.retcode == 0


@pytest.mark.ansible
def test_bad_controller_module():
    """Test that a bad module in controller_class causes an error exit"""
    recorder = run_module(
        module_params={"controller_class": "foo.bar.Baz"},
        patch_controller_class=False,
    )
    assert recorder.retcode == 0


@pytest.mark.ansible
def test_missing_controller_class():
    """Test that a class name that is missing from the module causes an error
    exit
    """
    recorder = run_module(
        module_params={"controller_class": "test_k8s_controller.NotThere"},
        patch_controller_class=False,
    )
    assert recorder.retcode == 0


@pytest.mark.ansible
def test_bad_controller_class():
    """Test that a class name that is not a class causes an error exit"""
    recorder = run_module(
        module_params={
            "controller_class": "test_k8s_controller.test_bad_controller_class",
        },
        patch_controller_class=False,
    )
    assert recorder.retcode == 0


@pytest.mark.ansible
def test_controller_exception():
    """Test that an exception in the Controller causes an error exit"""
    recorder = run_module(
        module_params={"controller_class": "test_k8s_application.BrokenTestController"}
    )
    assert recorder.retcode == 0


@pytest.mark.ansible
def test_owner_references():
    """Test that running the module creates resources with owner references"""
    recorder = run_module(
        cluster_state={"bar": {"Foo": {"v1": {}}}},
        module_params={
            "controller_class": "test_k8s_application.TestControllerWithResources"
        },
        # We don't patch the controller class here because we want the kube mock
        # to be able to fetch the deployed resources and they don't get deployed
        # if the patches take effect
        patch_controller_class=False,
    )
    assert recorder.retcode == 0

    # Fetch the content of the deployed component
    foos = recorder.mock_client.resources.get(kind="Foo", api_version="v1")
    resource = foos.get(name="foo", namespace="bar").to_dict()
    assert resource
    log.debug(resource)
    assert len(resource["metadata"]["ownerReferences"]) == 1
