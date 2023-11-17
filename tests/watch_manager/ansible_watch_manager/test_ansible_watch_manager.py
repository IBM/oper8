"""
Tests for the ansible watch manager.

NOTE: Without access to the actual ansible-operator executable, these tests are
    primarily intended to ensure the logic in the class works. To validate that
    the watches actually work, we need to build and run an operator!
"""

# Standard
import json
import os
import shutil
import sys
import tempfile
import time

# Third Party
import dill
import pytest
import yaml

# First Party
import alog

# Local
from oper8.test_helpers.helpers import DummyController, library_config
from oper8.watch_manager import WatchManagerBase
from oper8.watch_manager.ansible_watch_manager import AnsibleWatchManager

## Helpers #####################################################################

log = alog.use_channel("TEST")


def get_entrypoint(func):
    """Get an entrypoint command that will run this file as a main and pass it
    the pickled version of the given function
    """
    this_file = os.path.realpath(__file__)
    func_str = dill.dumps(func).hex()
    return f'{sys.executable} {this_file} "{func_str}"'


# This main is used to simulate the ansible operator executable and run various
# tests against it
if __name__ == "__main__":
    # Grab the first argument as a pickled string and deserialize it into a
    # function
    func = dill.loads(bytes.fromhex(sys.argv[1]))

    # Run the function, letting failures crash as needed
    func(*sys.argv[2:])


class FinalizerController(DummyController):
    kind = "Finalizable"

    def finalize_components(self, session):
        pass


def reset_globals(workdir=None):
    """This helper is only used in tests to "reset" the state of the global
    ansible process so that tests can run independently
    """
    AnsibleWatchManager.ANSIBLE_PROCESS = None
    WatchManagerBase._ALL_WATCHES = {}
    if workdir is not None:
        shutil.rmtree(workdir)
        os.mkdir(workdir)


@pytest.fixture(autouse=True)
def reset_globals_fxtr():
    """This helper is only used in tests to "reset" the state of the global
    ansible process so that tests can run independently
    """
    AnsibleWatchManager.ANSIBLE_PROCESS = None
    WatchManagerBase._ALL_WATCHES = {}


@pytest.fixture
def workdir():
    """All the tests need a temporary workdir, so we do this as a fixture"""
    with tempfile.TemporaryDirectory() as workdir:
        yield workdir


## Tests #######################################################################

#################
## Constructor ##
#################


@pytest.mark.ansible
def test_constructor_playbook(workdir):
    """Test that the playbook gets created with all passed-in parameters"""
    playbook_params = {
        "foo": True,
        "bar": "baz",
    }
    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        playbook_parameters=playbook_params,
    )

    # Make sure the playbook exists
    playbook_path = os.path.join(workdir, "playbook-foo.yaml")
    assert os.path.exists(playbook_path)

    # Read the playbook
    with open(playbook_path, "r") as handle:
        parsed_playbook = yaml.safe_load(handle)
    log.debug3(parsed_playbook)

    # Make sure passthrough args are passed through
    vars = parsed_playbook[0]["tasks"][0]["vars"]
    for key, val in playbook_params.items():
        assert vars.get(key) == val

    # Check the necessary additions made to wire up the controller
    assert vars["full_cr"] == "{{ _foo_bar_com_foo }}"
    assert "controller_class" in vars
    assert "strict_versioning" in vars


@pytest.mark.ansible
def test_constructor_watch_entry(workdir):
    """Test that a watch entry is added by the constructor"""
    playbook_params = {
        "foo": True,
        "bar": "baz",
    }
    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        manage_status=True,
        watch_dependent_resources=True,
        reconcile_period="3m",
        playbook_parameters=playbook_params,
    )

    # Make sure the watches file exists
    watches_path = os.path.join(workdir, "watches.yaml")
    assert os.path.exists(watches_path)

    # Read the watches file
    with open(watches_path, "r") as handle:
        parsed_watches = yaml.safe_load(handle)
    log.debug3(parsed_watches)

    # Make sure all elements are set correctly
    assert len(parsed_watches) == 1
    watch = parsed_watches[0]
    assert watch.get("group") == wm.group
    assert watch.get("version") == wm.version
    assert watch.get("kind") == wm.kind
    assert watch.get("playbook") == os.path.join(workdir, "playbook-foo.yaml")
    assert watch.get("manageStatus")
    assert watch.get("watchDependentResources")
    assert watch.get("reconcilePeriod") == "3m"
    assert watch.get("vars", {}).get("operation") == "add"
    assert "finalizer" not in watch


@pytest.mark.ansible
def test_constructor_watch_entry_finalizer(workdir):
    """Test that a watch entry for a controller with a finalier has the needed
    watches.yaml entries.
    """
    wm = AnsibleWatchManager(FinalizerController, ansible_base_path=workdir)

    # Make sure the watches file exists
    watches_path = os.path.join(workdir, "watches.yaml")
    assert os.path.exists(watches_path)

    # Read the watches file
    with open(watches_path, "r") as handle:
        parsed_watches = yaml.safe_load(handle)
    log.debug3(parsed_watches)

    # Make sure all elements are set correctly
    assert len(parsed_watches) == 1
    watch = parsed_watches[0]
    assert watch.get("vars", {}).get("operation") == "add"
    assert watch.get("finalizer", {}).get("name") == (
        f"finalizers.{FinalizerController.kind.lower()}.{FinalizerController.group}"
    )
    assert watch.get("finalizer", {}).get("vars", {}).get("operation") == "remove"


@pytest.mark.ansible
def test_constructor_multiple_watches(workdir):
    """Test that multiple watches can be added to the same watches.yaml by
    different managers
    """

    class DummyController2(DummyController):
        group = "foo.bar"
        version = "v2"
        kind = "Bar"

    # Create two watch managers
    wm1 = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
    )
    wm2 = AnsibleWatchManager(
        DummyController2,
        ansible_base_path=workdir,
    )

    # Make sure the watches file exists
    watches_path = os.path.join(workdir, "watches.yaml")
    assert os.path.exists(watches_path)

    # Read the watches file
    with open(watches_path, "r") as handle:
        parsed_watches = yaml.safe_load(handle)
    log.debug3(parsed_watches)

    # Make sure there are two entries in the watches file
    assert len(parsed_watches) == 2


@pytest.mark.ansible
def test_constructor_no_duplicate_watches(workdir):
    """Test that an assertion is raised if two managers attempt to watch the
    same group/version/kind
    """
    # Create two watch managers
    wm1 = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
    )
    with pytest.raises(AssertionError):
        AnsibleWatchManager(
            DummyController,
            ansible_base_path=workdir,
        )


@pytest.mark.ansible
def test_constructor_watch_disable_vcs(workdir):
    """Test that a watched controller with disable_vcs set will add the
    correct variable to the watch.yaml section
    """

    class NonVersionedController(DummyController):
        group = "foo.bar"
        version = "v2"
        kind = "Bar"
        disable_vcs = True

    wm = AnsibleWatchManager(
        NonVersionedController,
        ansible_base_path=workdir,
    )

    # Make sure the watches file exists
    watches_path = os.path.join(workdir, "watches.yaml")
    assert os.path.exists(watches_path)

    # Read the watches file
    with open(watches_path, "r") as handle:
        parsed_watches = yaml.safe_load(handle)
    log.debug3(parsed_watches)

    # Make sure the enable_ansible_vcs variable is set correctly
    assert len(parsed_watches) == 1
    assert parsed_watches[0]["vars"]["enable_ansible_vcs"] == "false"


@pytest.mark.ansible
def test_constructor_log_dir(workdir):
    """Test that if a log_dir is given in config, it is used in the playbook"""
    log_dir = "/logs"
    with library_config(ansible_watch_manager={"log_file_dir": log_dir}):
        wm = AnsibleWatchManager(
            DummyController,
            ansible_base_path=workdir,
        )

        # Make sure the playbook exists
        playbook_path = os.path.join(workdir, "playbook-foo.yaml")
        assert os.path.exists(playbook_path)

        # Read the playbook
        with open(playbook_path, "r") as handle:
            parsed_playbook = yaml.safe_load(handle)
        log.debug3(parsed_playbook)

        # Make sure log_file starts with the log dir
        vars = parsed_playbook[0]["tasks"][0]["vars"]
        assert vars.get("log_file", "").startswith(log_dir)


@pytest.mark.ansible
def test_constructor_arg_override_precedence(workdir):
    """Test that all of the kwargs in the initializer adhere to the correct
    override precedence
    """
    # Set up config overrides with the correct types and with all strings to
    # simulate reading from the env
    config_overrides = {
        "ansible_base_path": workdir,
        "ansible_entrypoint": "foobar.sh",
        "ansible_args": "--ansible-args string",
        "manage_status": True,
        "watch_dependent_resources": True,
        "reconcile_period": "120m",
        "playbook_parameters": {"foo": "bar"},
    }
    env_config_overrides = {
        key: (str(val) if not isinstance(val, dict) else json.dumps(val))
        for key, val in config_overrides.items()
    }

    # Set up overrides that will be ignored by overrides given in kwargs
    ignored_config_overrides = {
        "ansible_base_path": "SOMETHING BAD",
        "ansible_entrypoint": "yikes",
        "manage_status": "this can't even convert to a bool",
        "watch_dependent_resources": "neither can this!",
        "reconcile_period": "120m!!!!!!!!!!!!",
        "playbook_parameters": "no dict here",
    }

    # Test that config overrides take precedence over defaults
    for override_set in [
        config_overrides,
        env_config_overrides,
        ignored_config_overrides,
    ]:
        with library_config(ansible_watch_manager=override_set):
            # Reset the globals for this override set
            reset_globals(workdir)

            # Make the watch manager using kwargs only if overrides not used
            # from config
            kwargs = {}
            if override_set is ignored_config_overrides:
                kwargs = config_overrides
            wm = AnsibleWatchManager(DummyController, **kwargs)

            # Make sure the entrypoint and base path were set correctly
            assert wm._ansible_base_path == config_overrides["ansible_base_path"]
            assert wm._ansible_entrypoint == config_overrides["ansible_entrypoint"]
            assert wm._ansible_args == config_overrides["ansible_args"]

            # Make sure the playbook exists
            playbook_path = os.path.join(
                config_overrides["ansible_base_path"], "playbook-foo.yaml"
            )
            assert os.path.exists(playbook_path)

            # Read the playbook
            with open(playbook_path, "r") as handle:
                parsed_playbook = yaml.safe_load(handle)
            log.debug3(parsed_playbook)

            # Make sure the playbook params got added
            mod_vars = parsed_playbook[0]["tasks"][0]["vars"]
            for key, val in config_overrides["playbook_parameters"].items():
                assert mod_vars.get(key) == val

            # Make sure the watches file exists
            watches_path = os.path.join(
                config_overrides["ansible_base_path"], "watches.yaml"
            )
            assert os.path.exists(watches_path)

            # Read the watches file
            with open(watches_path, "r") as handle:
                parsed_watches = yaml.safe_load(handle)
            log.debug3(parsed_watches)

            # Make sure the watch entry got the overrides
            assert len(parsed_watches) == 1
            watch = parsed_watches[0]
            assert watch.get("manageStatus") == config_overrides["manage_status"]
            assert (
                watch.get("watchDependentResources")
                == config_overrides["watch_dependent_resources"]
            )
            assert watch.get("reconcilePeriod") == config_overrides["reconcile_period"]


#####################
## Watch/Stop/Wait ##
#####################


@pytest.mark.ansible
def test_watch_start_single_process(workdir):
    """Test that starting multiple managers starts a single process"""

    def test_func():
        while True:
            time.sleep(0.01)

    entrypoint = get_entrypoint(test_func)
    log.debug2(entrypoint)

    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        ansible_entrypoint=entrypoint,
    )

    # Run the watch and make sure the subprocess starts up successfully
    assert wm.watch()
    log.debug("Watch started")
    assert AnsibleWatchManager.ANSIBLE_PROCESS is not None
    assert AnsibleWatchManager.ANSIBLE_PROCESS.poll() is None

    # Stop the process
    wm.stop()
    wm.wait()


@pytest.mark.ansible
def test_watch_environment_passthrough(workdir):
    """Make sure that all external environment variables are passed through to
    the child process when the watch starts
    """

    def test_func():
        assert os.environ.get("_TEST_VAR") == "this is set"

    entrypoint = get_entrypoint(test_func)
    log.debug2(entrypoint)

    os.environ["_TEST_VAR"] = "this is set"
    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        ansible_entrypoint=entrypoint,
    )

    # Run the watch and wait until the process completes
    assert wm.watch()
    wm.wait()

    # Make sure the process terminated cleanly
    assert AnsibleWatchManager.ANSIBLE_PROCESS.returncode == 0


@pytest.mark.ansible
def test_watch_ansible_env(workdir):
    """Make sure that the ansible environment looks right to the spawned process"""

    def test_func():
        log.debug3(os.environ)
        ansible_library = os.environ.get("ANSIBLE_LIBRARY")
        ansible_roles_path = os.environ.get("ANSIBLE_ROLES_PATH")
        assert os.path.exists(ansible_library)
        assert os.path.exists(ansible_roles_path)
        assert "k8s_application.py" in os.listdir(ansible_library)
        assert "oper8_app" in os.listdir(ansible_roles_path)

    entrypoint = get_entrypoint(test_func)
    log.debug2(entrypoint)

    os.environ["_TEST_VAR"] = "this is set"
    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        ansible_entrypoint=entrypoint,
    )

    # Run the watch and wait until the process completes
    assert wm.watch()
    wm.wait()

    # Make sure the process terminated cleanly
    assert AnsibleWatchManager.ANSIBLE_PROCESS.returncode == 0


@pytest.mark.ansible
def test_watch_ansible_args(workdir):
    """Make sure that additional flags passed by `ansible_args` parameter
    are used in the command to spawned process
    """

    def test_func(*args):
        assert args[0] == "--ansible-args"
        assert args[1] == "foo=bar"

    entrypoint = get_entrypoint(test_func)
    log.debug2(entrypoint)

    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        ansible_entrypoint=entrypoint,
        ansible_args="--ansible-args foo=bar ",
    )

    # Run the watch and make sure the subprocess starts up successfully
    assert wm.watch()
    wm.wait()

    # Make sure the assertion in test_func passed
    assert AnsibleWatchManager.ANSIBLE_PROCESS.returncode == 0


@pytest.mark.ansible
def test_watch_subprocess_failure(workdir):
    """Make sure that if the process fails to run, it's handled correctly by the
    watch implementation
    """

    def test_func():
        sys.exit(1)

    entrypoint = get_entrypoint(test_func)
    log.debug2(entrypoint)

    os.environ["_TEST_VAR"] = "this is set"
    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        ansible_entrypoint=entrypoint,
    )

    # Run the watch and wait until the process completes
    # NOTE: The return value of watch is not very useful here because
    #   there's an arbitrary time delta between launching the subprocess and
    #   the process terminating with a non-zero exit code.
    wm.watch()
    wm.wait()

    # Make sure the process terminated cleanly
    assert AnsibleWatchManager.ANSIBLE_PROCESS.returncode == 1


@pytest.mark.ansible
def test_watch_no_new_managers(workdir):
    """Validate that an error is raised if a new manager is created after others
    have been started
    """

    def test_func():
        while True:
            time.sleep(0.01)

    entrypoint = get_entrypoint(test_func)
    log.debug2(entrypoint)

    wm = AnsibleWatchManager(
        DummyController,
        ansible_base_path=workdir,
        ansible_entrypoint=entrypoint,
    )

    # Run the watch and make sure the subprocess starts up successfully
    assert wm.watch()
    log.debug("Watch started")
    assert AnsibleWatchManager.ANSIBLE_PROCESS is not None
    assert AnsibleWatchManager.ANSIBLE_PROCESS.poll() is None

    # Attempt to create a second manager and make sure it errors out
    with pytest.raises(AssertionError):
        AnsibleWatchManager(DummyController)

    # Shut down the running manager
    wm.stop()
    wm.wait()
