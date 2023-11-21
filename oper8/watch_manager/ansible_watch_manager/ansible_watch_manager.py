"""
Ansible-based implementation of the WatchManager
"""

# Standard
from typing import Optional, Type
import copy
import json
import os
import shlex
import subprocess
import time

# Third Party
import yaml

# First Party
import alog

# Local
from ... import config
from ...controller import Controller
from ...exceptions import assert_config
from ..base import WatchManagerBase

log = alog.use_channel("ANSBL")

DEFAULT_ENTRYPOINT = "/usr/local/bin/ansible-operator run --watches-file=./watches.yaml"


class AnsibleWatchManager(WatchManagerBase):
    """The AnsibleWatchManager uses the core of an ansible-based operator to
    manage watching resources. The key elements are:

    1. Manage a `watches.yaml` file for all watched resources
    2. Manage a playbook for each watched resource
    3. Manage the ansible operator's executable as a subprocess
    """

    # Shared singleton process used to manage all watches via ansible
    ANSIBLE_PROCESS = None

    # Defaults for initialization args held separately to allow for override
    # precedence order
    _DEFAULT_INIT_KWARGS = {
        "ansible_base_path": "/opt/ansible",
        "ansible_entrypoint": DEFAULT_ENTRYPOINT,
        "ansible_args": "",
        "manage_status": False,
        "watch_dependent_resources": False,
        "reconcile_period": "10m",
        "playbook_parameters": None,
    }

    def __init__(
        self,
        controller_type: Type[Controller],
        *,
        ansible_base_path: Optional[str] = None,
        ansible_entrypoint: Optional[str] = None,
        ansible_args: Optional[str] = None,
        manage_status: Optional[bool] = None,
        watch_dependent_resources: Optional[bool] = None,
        reconcile_period: Optional[str] = None,
        playbook_parameters: Optional[dict] = None,
    ):
        """Construct with the core watch binding and configuration args for the
        watches.yaml and playbook.yaml files.

        NOTE: All args may be overridden in the `ansible_watch_manager` section
            of the library config. The precedence order is:

        1. Directly passed arguments
        2. Config values
        3. Default values from code

        A passed None value in any of these is considered "unset"

        Args:
            controller_type:  Type[Controller],
                The Controller type that will manage this group/version/kind

        Kwargs:
            ansible_base_path:  str
                The base path where the ansible runtime will be run. This is
                also used to determine where the watches.yaml and playbooks will
                be managed.
            ansible_entrypoint:  str
                The command to use to run ansible
            ansible_args: str
                Additional flags to be passed to `ansible_entrypoint`
            manage_status:  bool
                Whether or not to let ansible manage status on the CR
            watch_dependent_resources:  bool
                Whether or not to trigger a reconciliation on change to
                dependent resources.
            reconcile_period:  str
                String representation of the time duration to use for periodic
                reconciliations
            playbook_parameters:  dict
                Parameters to use to configure the k8s_application module in the
                playbook
        """
        # Make sure that the shared ansible process is not already started
        assert (
            self.ANSIBLE_PROCESS is None
        ), "Cannot create an AnsibleWatchManager after starting another AnsibleWatchManager"

        # Set up the function arguments based on override precedence
        ansible_base_path = self._init_arg("ansible_base_path", ansible_base_path, str)
        ansible_entrypoint = self._init_arg(
            "ansible_entrypoint", ansible_entrypoint, str
        )
        ansible_args = self._init_arg("ansible_args", ansible_args, str)
        manage_status = self._init_arg("manage_status", manage_status, bool)
        watch_dependent_resources = self._init_arg(
            "watch_dependent_resources", watch_dependent_resources, bool
        )
        reconcile_period = self._init_arg("reconcile_period", reconcile_period, str)
        playbook_parameters = self._init_arg(
            "playbook_parameters", playbook_parameters, dict
        )

        super().__init__(controller_type)
        self._ansible_base_path = ansible_base_path
        self._ansible_entrypoint = ansible_entrypoint
        self._ansible_args = ansible_args

        # Create the playbook
        playbook_path = self._add_playbook(playbook_parameters)

        # Create the entry in the watches.yaml
        self._add_watch_entry(
            playbook_path=playbook_path,
            manage_status=manage_status,
            watch_dependent_resources=watch_dependent_resources,
            reconcile_period=reconcile_period,
            add_finalizer=controller_type.has_finalizer,
            disable_vcs=getattr(controller_type, "disable_vcs", None),
        )

    ## Interface ###############################################################

    def watch(self) -> bool:
        """Start the global ansible process if not already started

        NOTE: This is intentionally not thread safe! The watches should all be
            managed from the primary entrypoint thread.

        Returns:
            success:  bool
                True if the asible process is running correctly
        """
        cls = self.__class__
        if cls.ANSIBLE_PROCESS is None:
            log.info("Starting ansible watch process")
            env = copy.deepcopy(os.environ)
            env["ANSIBLE_LIBRARY"] = self._ansible_library_path()
            env["ANSIBLE_ROLES_PATH"] = self._ansible_roles_path()
            cls.ANSIBLE_PROCESS = (
                subprocess.Popen(  # pylint: disable=consider-using-with
                    shlex.split(
                        " ".join((self._ansible_entrypoint, self._ansible_args)).strip()
                    ),
                    cwd=self._ansible_base_path,
                    env=env,
                )
            )

        # If the process does not have a returncode on poll, it's up. This is a
        # point-in-time statement. There's no way for this code to actually
        # validate the state of the process since it may crash at any
        # indeterminate time after starting.
        return self.ANSIBLE_PROCESS.poll() is None

    def wait(self):
        """Wait for the ansible process to terminate"""
        if self.ANSIBLE_PROCESS is not None:
            self.ANSIBLE_PROCESS.wait()

    def stop(self):
        """Attempt to terminate the ansible process. This asserts that the
        process has been created in order to avoid race conditions with a None
        check.
        """
        assert self.ANSIBLE_PROCESS is not None, "Cannot stop before watching"
        log.info("Killing shared ansible process")
        self.ANSIBLE_PROCESS.terminate()
        kill_start_time = time.time()
        while (
            self.ANSIBLE_PROCESS.poll() is None
            and time.time() - kill_start_time
            < config.ansible_watch_manager.kill_max_wait
        ):
            time.sleep(0.001)
        assert (
            self.ANSIBLE_PROCESS.poll() is not None
        ), "The only way to shut down ansible is with a sledge hammer!"

    ## Implementation Details ##################################################

    @classmethod
    def _init_arg(cls, arg_name, passed_value, arg_type):
        """Helper to enforce init arg precedence"""
        if passed_value is not None:
            return passed_value
        config_value = config.ansible_watch_manager.get(arg_name)
        if config_value is not None:
            if arg_type is not None and not isinstance(config_value, arg_type):
                assert_config(
                    isinstance(config_value, str),
                    f"Invalid type for ansible_watch_manager.{arg_name}: "
                    + "{type(config_value)} should be {arg_type}",
                )
                if arg_type is bool:
                    config_value = config_value.lower() == "true"
                elif arg_type is dict:
                    config_value = json.loads(config_value)
                assert_config(
                    isinstance(config_value, arg_type),
                    f"Cannot convert ansible_watch_manager.{arg_name} from str to {arg_type}",
                )
            return config_value
        assert (
            arg_name in cls._DEFAULT_INIT_KWARGS
        ), f"Programming Error: Unsupported init kwarg: {arg_name}"
        return cls._DEFAULT_INIT_KWARGS[arg_name]

    def _add_playbook(self, playbook_parameters):
        """Create a playbook for this watch"""

        # Open the base template for playbooks
        playbook_base_path = os.path.join(
            self._resources_path(),
            "playbook-base.yaml",
        )
        with open(playbook_base_path, encoding="utf-8") as handle:
            playbook_base = yaml.safe_load(handle)

        # Add the provided variables
        module_vars = playbook_parameters or {}
        module_vars.setdefault("strict_versioning", False)
        kind = self.controller_type.kind.lower()
        log_file = f"{kind}.{{{{ ansible_operator_meta.name }}}}.log"
        log_dir = config.ansible_watch_manager.log_file_dir
        if log_dir is not None:
            log.debug2("Adding log dir: %s", log_dir)
            log_file = os.path.join(log_dir, log_file)
        module_vars.setdefault("log_file", log_file)
        playbook_base[0]["tasks"][0]["vars"] = module_vars

        # Add the controller_class
        controller_class = (
            f"{self.controller_type.__module__}.{self.controller_type.__name__}"
        )
        log.debug3("controller_class: %s", controller_class)
        module_vars["controller_class"] = controller_class

        # Add the full_cr template
        group_template = self.group.lower().replace(".", "_").replace("-", "_")
        cr_template = f"{{{{ _{group_template}_{self.kind.lower()} }}}}"
        module_vars["full_cr"] = cr_template

        # Write it out to the right place
        log.debug3(
            "%s/%s/%s playbook vars: %s",
            self.group,
            self.version,
            self.kind,
            module_vars,
        )
        playbook_path = os.path.join(
            self._ansible_base_path, f"playbook-{self.kind.lower()}.yaml"
        )
        with open(playbook_path, "w", encoding="utf-8") as handle:
            yaml.dump(playbook_base, handle)
        return playbook_path

    def _add_watch_entry(  # pylint: disable=too-many-arguments
        self,
        playbook_path: str,
        manage_status: bool,
        watch_dependent_resources: bool,
        reconcile_period: str,
        add_finalizer: bool,
        disable_vcs: Optional[bool],
    ):
        """Add an entry to the watches.yaml file, creating it if needed"""

        # Load the current watches.yaml content, or start fresh
        watches_path = os.path.join(self._ansible_base_path, "watches.yaml")
        if os.path.exists(watches_path):
            with open(watches_path, encoding="utf-8") as handle:
                watches = yaml.safe_load(handle)
        else:
            watches = []

        # Make sure there is not already an entry for this watch
        matches = [
            (
                watch_entry["group"] == self.group
                and watch_entry["version"] == self.version
                and watch_entry["kind"] == self.kind
            )
            for watch_entry in watches
        ]
        assert True not in matches, (
            "Can't have multiple watch entries for the same group/version/kind! "
            + f"{self.group}/{self.version}/{self.kind}"
        )
        log.debug2("Adding new watch for %s", self)
        watch_entry = {
            "group": self.group,
            "version": self.version,
            "kind": self.kind,
            "vars": {"operation": "add"},
        }
        if disable_vcs is not None:
            str_val = str(not disable_vcs).lower()
            log.debug(
                "Adding watch variable [enable_ansible_vcs = '%s'] for %s/%s/%s",
                str_val,
                self.group,
                self.version,
                self.kind,
            )
            watch_entry["vars"]["enable_ansible_vcs"] = str_val
        watches.append(watch_entry)

        # Update the watch entry with the configuration for this watch
        watch_entry["playbook"] = playbook_path
        watch_entry["manageStatus"] = manage_status
        watch_entry["watchDependentResources"] = watch_dependent_resources
        watch_entry["reconcilePeriod"] = reconcile_period

        # If requested, add a version of the watch that manages the finalizer
        if add_finalizer:
            finalizer_name = self.controller_type.finalizer
            log.debug2("Adding finalizer: %s", finalizer_name)
            watch_entry["finalizer"] = {
                "name": finalizer_name,
                "vars": {"operation": "remove"},
            }

        # Write the watches.yaml file back out
        with open(watches_path, "w", encoding="utf-8") as handle:
            yaml.dump(watches, handle)

    @staticmethod
    def _resources_path():
        """Get the path to the static resources for ansible"""
        return os.path.realpath(os.path.join(os.path.dirname(__file__), "resources"))

    @staticmethod
    def _ansible_library_path():
        """Get the absolute path to the ansible library with the k8s_applicaiton
        module
        """
        return os.path.realpath(os.path.join(os.path.dirname(__file__), "modules"))

    @classmethod
    def _ansible_roles_path(cls):
        """Get the absolute path to the ansible roles"""
        return os.path.join(cls._resources_path(), "roles")
