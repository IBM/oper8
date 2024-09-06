"""
This module holds common helper functions for making testing easy
"""

# Standard
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache
from unittest import mock
import copy
import hashlib
import inspect
import json
import os
import pathlib
import subprocess
import tempfile
import uuid

# Third Party
from kubernetes.dynamic.client import DynamicClient
import pytest

# First Party
import aconfig
import alog

# Local
from oper8 import Component, Controller, controller
from oper8.cmd.run_operator_cmd import RunOperatorCmd
from oper8.config import library_config as config_detail_dict
from oper8.deploy_manager.dry_run_deploy_manager import DryRunDeployManager
from oper8.deploy_manager.openshift_deploy_manager import OpenshiftDeployManager
from oper8.session import Session
from oper8.test_helpers.kub_mock import MockKubClient
from oper8.utils import merge_configs

log = alog.use_channel("TEST")


def configure_logging():
    alog.configure(
        os.environ.get("LOG_LEVEL", "off"),
        os.environ.get("LOG_FILTERS", ""),
        formatter="json"
        if os.environ.get("LOG_JSON", "").lower() == "true"
        else "pretty",
        thread_id=os.environ.get("LOG_THREAD_ID", "").lower() == "true",
    )


configure_logging()

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

TEST_INSTANCE_NAME = "test_instance"
TEST_INSTANCE_UID = "12345678-1234-1234-1234-123456789012"
TEST_NAMESPACE = "test"
SOME_OTHER_NAMESPACE = "somewhere"

PULL_SECRET_NAME = "REPLACE_PULL_SECRET"


def setup_cr(
    kind="Widget",
    api_version="foo.bar.com/v123",
    deploy_config=None,
    version="1.2.3",
    name=TEST_INSTANCE_NAME,
    namespace=TEST_NAMESPACE,
    **kwargs,
):
    deploy_config = deploy_config or {}
    cr_dict = kwargs or {}
    cr_dict.setdefault("kind", kind)
    cr_dict.setdefault("apiVersion", api_version)
    cr_dict.setdefault("metadata", {}).setdefault("name", name)
    cr_dict.setdefault("metadata", {}).setdefault("namespace", namespace)
    cr_dict.setdefault("metadata", {}).setdefault("uid", TEST_INSTANCE_UID)
    cr_dict.setdefault("spec", {}).update(copy.deepcopy(deploy_config))
    cr_dict["spec"].setdefault("version", version)
    return aconfig.Config(cr_dict)


def setup_session(
    version="1.2.3",
    app_config=None,
    deploy_config=None,
    full_cr=None,
    deploy_manager=None,
    namespace=TEST_NAMESPACE,
    deploy_initial_cr=True,
    **kwargs,
):
    app_config = app_config or aconfig.Config({}, override_env_vars=False)
    deploy_config = deploy_config or aconfig.Config({}, override_env_vars=False)
    full_cr = full_cr or setup_cr(
        deploy_config=deploy_config, version=version, namespace=namespace
    )
    if not deploy_manager:
        deploy_manager = (
            MockDeployManager(resources=[full_cr])
            if deploy_initial_cr
            else MockDeployManager()
        )

    return Session(
        reconciliation_id=str(uuid.uuid4()),
        cr_manifest=full_cr,
        config=app_config,
        deploy_manager=deploy_manager,
        **kwargs,
    )


@contextmanager
def setup_session_ctx(*args, **kwargs):
    """Context manager wrapper around setup_session. This simplifies the porting
    process from WA and really provides no functional benefit.
    """
    yield setup_session(*args, **kwargs)


@contextmanager
def library_config(**config_overrides):
    """This context manager sets library config values temporarily and reverts
    them on completion
    """
    # Override the configs and hang onto the old values
    old_vals = {}
    for key, val in config_overrides.items():
        if key in config_detail_dict:
            old_vals[key] = config_detail_dict[key]
        config_detail_dict[key] = val

    # Yield to the context
    yield

    # Revert to the old values
    for key in config_overrides:
        if key in old_vals:
            config_detail_dict[key] = old_vals[key]
        else:
            del config_detail_dict[key]


def get_failable_method(fail_flag, method, failure_return=False):
    log.debug4(
        "Setting up failable mock of [%s] with fail flag: %s", str(method), fail_flag
    )

    def failable_method(*args, **kwargs):
        log.debug4(
            "Running failable mock of [%s] with fail flag: %s", str(method), fail_flag
        )
        if isinstance(fail_flag, Exception) or (
            inspect.isclass(fail_flag) and issubclass(fail_flag, Exception)
        ):
            log.debug4("Raising in failable mock")
            raise fail_flag
        elif callable(fail_flag):
            log.debug4("Calling callable fail flag")
            res = fail_flag()
            if res is not None:
                return res
        elif fail_flag == "assert":
            log.debug4("Asserting in failable mock")
            raise AssertionError(f"You told me to fail {method}!")
        elif fail_flag:
            log.debug4("Returning %s", failure_return)
            return failure_return
        log.debug4("Passing through (%s, **%s)", args, kwargs)
        res = method(*args, **kwargs)
        log.debug4("Passthrough res: %s", res)
        return res

    return failable_method


class FailOnce:
    """Helper callable that will fail once on the N'th call"""

    def __init__(self, fail_val, fail_number=1):
        self.call_count = 0
        self.fail_number = fail_number
        self.fail_val = fail_val

    def __call__(self, *_, **__):
        self.call_count += 1
        if self.call_count == self.fail_number:
            log.debug("Failing on call %d with %s", self.call_count, self.fail_val)
            if isinstance(self.fail_val, type) and issubclass(self.fail_val, Exception):
                raise self.fail_val("Raising!")
            return self.fail_val
        log.debug("Not failing on call %d", self.call_count)
        return


class MockDeployManager(DryRunDeployManager):
    """The MockDeployManager wraps a standard DryRunDeployManager and adds
    configuration options to simulate failures in each of its operations.
    """

    def __init__(
        self,
        deploy_fail=False,
        deploy_raise=False,
        disable_fail=False,
        disable_raise=False,
        get_state_fail=False,
        get_state_raise=False,
        watch_fail=False,
        watch_raise=False,
        generate_resource_version=True,
        set_status_fail=False,
        set_status_raise=False,
        auto_enable=True,
        resources=None,
        resource_dir=None,
        **kwargs,
    ):
        """This DeployManager can be configured to have various failure cases
        and will mock the state of the cluster so that get_object_current_state
        will pull its information from the local dict.
        """

        # Add apiVersion to resources that are missing it, then initialize the
        # dry run manager

        resources = resources or []
        # Parse pre-populated resources if needed
        resources = resources + (RunOperatorCmd._parse_resource_dir(resource_dir))

        for resource in resources:
            resource.setdefault("apiVersion", "v1")
        super().__init__(
            resources, generate_resource_version=generate_resource_version, **kwargs
        )

        self.watch_fail = "assert" if watch_raise else watch_fail
        self.deploy_fail = "assert" if deploy_raise else deploy_fail
        self.disable_fail = "assert" if disable_raise else disable_fail
        self.get_state_fail = "assert" if get_state_raise else get_state_fail
        self.set_status_fail = "assert" if set_status_raise else set_status_fail

        # If auto-enabling, turn the mocks on now
        if auto_enable:
            self.enable_mocks()

    #######################
    ## Helpers for Tests ##
    #######################

    def enable_mocks(self):
        """Turn the mocks on"""
        self.deploy = mock.Mock(
            side_effect=get_failable_method(
                self.deploy_fail, super().deploy, (False, False)
            )
        )
        self.disable = mock.Mock(
            side_effect=get_failable_method(
                self.disable_fail, super().disable, (False, False)
            )
        )
        self.get_object_current_state = mock.Mock(
            side_effect=get_failable_method(
                self.get_state_fail, super().get_object_current_state, (False, None)
            )
        )
        self.set_status = mock.Mock(
            side_effect=get_failable_method(
                self.set_status_fail, super().set_status, (False, False)
            )
        )
        self.watch_objects = mock.Mock(
            side_effect=get_failable_method(self.watch_fail, super().watch_objects, [])
        )

    def get_obj(self, kind, name, namespace=None, api_version=None):
        return self.get_object_current_state(kind, name, namespace, api_version)[1]

    def has_obj(self, *args, **kwargs):
        return self.get_obj(*args, **kwargs) is not None


default_dummy_metadata = {"labels": {"foo": "bar"}}


dummy_instance_spec = {
    "kind": "unicorn",
    "metadata": {"name": "instance", "namespace": TEST_NAMESPACE},
    "labels": {"label": "instance-label"},
}


def make_patch(
    patch_type,
    body,
    name="test",
    target=None,
    namespace=TEST_NAMESPACE,
    api_version="org.oper8/v1",
    kind="TemporaryPatch",
):
    """Make a sample TemporaryPatch resource body"""
    target = target or {}
    patch_obj = {
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {"name": name},
        "spec": {
            "apiVersion": target.get("apiVersion", "fake"),
            "kind": target.get("kind", "fake"),
            "name": target.get("metadata", {}).get("name", "fake"),
            "patchType": patch_type,
            "patch": body,
        },
    }
    if namespace is not None:
        patch_obj["metadata"]["namespace"] = namespace
    return aconfig.Config(
        patch_obj,
        override_env_vars=False,
    )


## DummyComponent ##############################################################


class DummyComponentBase(Component):
    """This base class provides all of the common functionality for
    DummyComponent and DummyLegacyComponent
    """

    def __init__(
        self,
        name=None,
        session=None,
        api_objects=None,
        api_object_deps=None,
        render_chart_fail=False,
        deploy_fail=False,
        disable_fail=False,
        verify_fail=False,
        build_chart_fail=False,
        disabled=False,
        **kwargs,
    ):
        # Hang onto config inputs
        self.api_objects = api_objects or []
        self.api_object_deps = api_object_deps or {}

        # Mock passthroughs to the base class
        self.render_chart_fail = render_chart_fail
        self.render_chart = mock.Mock(
            side_effect=get_failable_method(self.render_chart_fail, self.render_chart)
        )
        self.deploy_fail = deploy_fail
        self.deploy = mock.Mock(
            side_effect=get_failable_method(self.deploy_fail, super().deploy)
        )
        self.disable_fail = disable_fail
        self.disable = mock.Mock(
            side_effect=get_failable_method(self.disable_fail, super().disable)
        )
        self.verify_fail = verify_fail
        self.verify = mock.Mock(
            side_effect=get_failable_method(self.verify_fail, super().verify)
        )
        self.build_chart_fail = build_chart_fail
        self.build_chart = mock.Mock(
            side_effect=get_failable_method(self.build_chart_fail, self.build_chart)
        )

        # Initialize Component
        if name is None:
            super().__init__(session=session, disabled=disabled)
        else:
            super().__init__(name=name, session=session, disabled=disabled)

    @alog.logged_function(log.debug2)
    def _add_resources(self, scope, session):
        """This will be called in both implementations in their respective
        places
        """
        api_objs = self._gather_dummy_resources(scope, session)

        # Add dependencies between objects
        for downstream_name, upstreams in self.api_object_deps.items():
            assert downstream_name in api_objs, "Bad test config"
            downstream = api_objs[downstream_name]
            for upstream_name in upstreams:
                assert upstream_name in api_objs, "Bad test config"
                upstream = api_objs[upstream_name]
                downstream.add_dependency(upstream)

    def _gather_dummy_resources(self, scope, session):
        api_objs = {}
        for api_obj in self.api_objects:
            log.debug3("Creating api object: %s", api_obj)

            # Create api object from tuple or generate one if callable
            if isinstance(api_obj, tuple):
                object_name, object_def = api_obj

                object_def = merge_configs(
                    {
                        "apiVersion": "v1",
                        "metadata": {"name": object_name, "namespace": TEST_NAMESPACE},
                    },
                    object_def,
                )
            elif isinstance(api_obj, dict):
                object_def = api_obj
                object_name = api_obj.get("metadata", {}).get("name")
            else:
                object_def = api_obj(self, session)
                object_name = api_obj.name

            resource_node = self.add_resource(object_name, object_def)
            if resource_node is not None:
                api_objs[resource_node.get_name()] = resource_node
        return api_objs

    def get_rendered_configs(self):
        configs = []
        for obj in self.managed_objects:
            configs.append(aconfig.Config(obj.definition))
        return configs


@pytest.fixture(autouse=True)
def version_safe_md5():
    real_md5 = hashlib.md5

    UNDEFINED = "__undefined__"

    def md5_wrapper(first_arg=UNDEFINED, *_, **__):
        if first_arg is UNDEFINED:
            return real_md5()
        return real_md5(first_arg)

    with mock.patch.object(hashlib, "md5", md5_wrapper):
        yield


class MockedOpenshiftDeployManager(OpenshiftDeployManager):
    """Override class that uses the mocked client"""

    def __init__(self, manage_ansible_status=False, owner_cr=None, *args, **kwargs):
        self._mock_args = args
        self._mock_kwargs = kwargs
        super().__init__(manage_ansible_status, owner_cr)

    def _setup_client(self):
        mock_client = MockKubClient(*self._mock_args, **self._mock_kwargs)
        return DynamicClient(mock_client)


class DummyNodeComponent(DummyComponentBase):
    """
    Configurable dummy component which will create an abritrary set of
    resource node instances.
    """

    def __init__(self, session, *args, **kwargs):
        """Construct with the additional option to fail build_chart"""
        super().__init__(*args, session=session, **kwargs)
        self._add_resources(self, session)


class MockComponent(DummyNodeComponent):
    """Dummy component with a valid mock name"""

    name = "mock"


## DummyController #############################################################


class DummyController(Controller):
    """Configurable implementation of a controller that can be used in unit
    tests to simulate Controller behavior
    """

    ##################
    ## Construction ##
    ##################

    group = "foo.bar.com"
    version = "v42"
    kind = "Foo"

    def __init__(
        self,
        components=None,
        after_deploy_fail=False,
        after_verify_fail=False,
        setup_components_fail=False,
        finalize_components_fail=False,
        should_requeue_fail=False,
        component_type=DummyNodeComponent,
        **kwargs,
    ):
        # Initialize parent
        super().__init__(**kwargs)

        # Set up objects that this controller will manage directly
        self.component_specs = components or []
        self.component_type = component_type

        # Set up mocks
        self.after_deploy_fail = after_deploy_fail
        self.after_verify_fail = after_verify_fail
        self.setup_components_fail = setup_components_fail
        self.finalize_components_fail = finalize_components_fail
        self.should_requeue_fail = should_requeue_fail
        self.after_deploy = mock.Mock(
            side_effect=get_failable_method(
                self.after_deploy_fail, super().after_deploy
            )
        )
        self.after_verify = mock.Mock(
            side_effect=get_failable_method(
                self.after_verify_fail, super().after_verify
            )
        )
        self.setup_components = mock.Mock(
            side_effect=get_failable_method(
                self.setup_components_fail, self.setup_components
            )
        )
        self.finalize_components = mock.Mock(
            side_effect=get_failable_method(
                self.finalize_components_fail, self.finalize_components
            )
        )
        self.should_requeue = mock.Mock(
            side_effect=get_failable_method(
                self.should_requeue_fail, super().should_requeue
            )
        )

    ##############################
    ## Interface Implementation ##
    ##############################

    def setup_components(self, session: Session):
        """Set up the components based on the component specs passed in"""

        # Add the components
        for component in self.component_specs:
            name = component["name"]
            log.debug2("Adding component %s (kwargs: %s)", name, component)
            comp = self._make_dummy_component(
                session=session,
                **component,
            )
            log.debug2("Component name: %s", comp.name)
            log.debug2(
                "Components in session [%s]: %s",
                session.id,
                [
                    comp.name
                    for comp in session.get_components()
                    + session.get_components(disabled=True)
                ],
            )

        # Add the dependencies after the nodes (so that we can look up by name)
        component_map = {
            comp.name: comp
            for comp in session.get_components() + session.get_components(disabled=True)
        }
        for component in self.component_specs:
            comp = component_map[component["name"]]
            upstreams = component.get("upstreams", [])
            for upstream in upstreams:
                session.add_component_dependency(comp, upstream)

        # Hang onto the components so that they can be checked
        self.components = component_map

    ############################
    ## Implementation Details ##
    ############################

    def _make_dummy_component(self, name="dummy", session=None, **kwargs):
        """This helper wraps any DummyComponent class so that the name class
        attribute is not overwritten by the next instance.
        """

        class WrappedDummyComponent(self.component_type):
            pass

        WrappedDummyComponent.name = name
        return WrappedDummyComponent(session=session, **kwargs)


@controller(
    group="unit.test.com",
    version="v42",
    kind="MockTopApp",
)
class MockTopApp(Controller):
    """Mock implementation of a top-level Controller to allow subsystems to be
    tested as "children"
    """

    def __init__(self, config_defaults=None, component_types=None):
        super().__init__(config_defaults=config_defaults)
        self.component_types = component_types or []

    def setup_components(self, session: Session):
        for component_type in self.component_types:
            component_type(session=session)

    def do_rollout(self, session):
        try:
            return self.run_reconcile(session)
        except Exception as err:
            log.debug("Caught error in rollout: %s", err, exc_info=True)


## Ansible Tests ###############################################################


class ModuleExit(Exception):
    """Exception we'll use to break out when sys.exit was called"""


class TestRecorder:
    # This is not a test class!
    __test__ = False

    CONSTRUCTED = "constructed"
    SETUP_COMPONENTS = "setup_components"
    FINALIZE_COMPONENTS = "finalize_components"
    RECONCILE = "reconcile"
    EXITED = "exited"

    def __init__(self, raise_on_success=True):
        self.retcode = None
        self.events = []
        self.event_details = {}
        self.raise_on_success = raise_on_success

    def set_exit_code(self, code=0):
        self.events.append(self.EXITED)
        self.retcode = code
        if code != 0 or self.raise_on_success:
            raise ModuleExit()

    def add_event(self, event, details=None):
        self.events.append(event)
        if details is not None:
            self.event_details[event] = details


@contextmanager
def mock_config_file(config_object):
    """Yuck! Ansible makes it tough to actually inject parameters in since it
    expects that modules will only be run by its parent runner.
    """
    # Third Party
    import ansible.module_utils.basic

    ansible.module_utils.basic._ANSIBLE_ARGS = json.dumps(config_object).encode("utf-8")
    yield
    ansible.module_utils.basic._ANSIBLE_ARGS = None


@contextmanager
def mock_sys_exit(recorder):
    with mock.patch("sys.exit", recorder.set_exit_code):
        yield


## General Helpers #############################################################


@lru_cache(maxsize=1)
def default_branch_name() -> str:
    """Helper to get the current git context's default branch name"""
    try:
        return (
            subprocess.run(
                "git config --get init.defaultBranch".split(),
                check=True,
                stdout=subprocess.PIPE,
            )
            .stdout.decode("utf-8")
            .strip()
        )
    except subprocess.CalledProcessError:
        return "master"


def setup_vcs_project(
    components: list,
    directory: str,
    controller_name="Controller",
    module_dir="src",
    module_name="test_module",
    versions=None,
):
    # Make the parent directory and module directory
    versions = versions or ["1.2.3"]
    parent_path = pathlib.Path(directory)
    parent_path.mkdir(exist_ok=True)
    module_path = parent_path / module_dir / module_name
    module_path.mkdir(parents=True)

    # Open and template controller file
    template_file = pathlib.Path(TEST_DATA_DIR) / "controller.template"
    with open(template_file) as controller_template_file:
        controller_template = controller_template_file.read()

    # Create controller file from template
    with open(module_path / "controller.py", "w") as controller_file:
        controller_file.write(
            controller_template.format(
                controller_name=controller_name,
                components=str(components),
            )
        )

    # Create import file
    with open(module_path / "__init__.py", "w") as import_file:
        import_file.write(f"from .controller import {controller_name}")

    config_path = pathlib.Path.home() / ".gitconfig"
    if not config_path.exists():
        config_path.touch(exist_ok=True)

    # Initialize git repo in parent directory
    subprocess.run(["git", "init", directory], check=True)
    subprocess.run(["git", "-C", directory, "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name='Oper8'",
            "-c",
            "user.email='my@email.org'",
            "-C",
            directory,
            "commit",
            "-m",
            "Initial Commit",
        ],
        check=True,
    )

    # For each version create a branch and test file. This ensures each version has a different
    # commit hash
    for version in versions:
        subprocess.run(["git", "-C", directory, "checkout", "-b", version], check=True)
        (parent_path / version).touch(exist_ok=True)
        subprocess.run(["git", "-C", directory, "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name='Oper8'",
                "-c",
                "user.email='my@email.org'",
                "-C",
                directory,
                "commit",
                "-m",
                "Annother Commit",
            ],
            check=True,
        )
        subprocess.run(["git", "-C", directory, "tag", version], check=True)
        subprocess.run(
            ["git", "-C", directory, "checkout", default_branch_name()], check=True
        )


@pytest.fixture()
def vcs_project():
    with tempfile.TemporaryDirectory() as vcs_directory:
        setup_vcs_project(
            components=[],
            directory=vcs_directory,
            module_name="test_module",
            versions=["1.2.3"],
            module_dir="src",
        )

        yield vcs_directory


@contextmanager
def maybe_temp_dir():
    working_dir = os.environ.get("WORKING_DIR")
    if working_dir:
        working_dir = f"{working_dir}.{datetime.now().timestamp()}"
        os.makedirs(working_dir, exist_ok=True)
        yield working_dir
    else:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir


def deep_merge(a, b):
    """NOTE: This should really be eliminated in favor of just using
    merge_configs
    """
    return merge_configs(a, b)
