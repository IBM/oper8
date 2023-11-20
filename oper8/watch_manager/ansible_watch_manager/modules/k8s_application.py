#!/usr/bin/env python

## Imports #####################################################################

# Standard
import ast
import logging.handlers
import pathlib

# COMPATIBILITY
# With >= 3.12, the vendored ansible six functionality does not import cleanly,
# so we patch sys.modules to work around this
import sys

if sys.version_info.major > 3 or sys.version_info.minor > 11:
    # Third Party
    from six import moves

    sys.modules["ansible.module_utils.six.moves"] = moves

# Third Party
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.k8s.common import K8sAnsibleMixin

# First Party
import alog

# Local
from oper8 import config
from oper8.reconcile import ReconcileManager
from oper8.watch_manager.ansible_watch_manager.modules.log_rotator import (
    AutoRotatingFileHandler,
)

## Module Doc Vars #############################################################

ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "oper8.org",
}

DOCUMENTATION = """
---
module: k8s_application

short_description: Ansible module which manages a Controller using the
    oper8 python library

version_added: "2.4"

description:
    - "TODO"

options:

    version:
        description:
            - This dictates the version of the application library which will
              be used to service the CR
        type: str
        required: true
    controller_class:
        description:
            - This is the fully-qualified name of the python Controller class
              that is bound to this operator.
        type: str
        required: true
    operation:
        description:
            - Which operation this execution should perform (add or remove)
        type: str ("add" or "remove")
        required: false
    full_cr:
        description:
            - This template passes the content of the full CR which triggered
              the deploy into the library so that the deploy parameters can be
              parsed there. It should be populated with a template expression
              based on the group and kind of the CR being managed.
        type: str (template)
        required: true
    manage_ansible_status:
        description:
            - If true, oper8 will emulate the status management done natively by
              ansible based on the readiness values of oper8's native status
              management
        type: str
        required: false
    enable_ansible_vcs:
        description: Whether to enable or disable ansible-vcs
        type: bool
        default: False
    vcs_dir:
        description: The directory holding the git repo used with ansible-vcs
        type: str
        required: false
    vcs_dest:
        description: The destination directory for the checkedout version used with ansible-vcs
        type: str
        required: false
    strict_versioning:
        description:
            - If true, oper8 will reject spec.version values that are not in the
              list of supported_versions.
        required: false

    log_level:
        description:
            - This sets the default verbosity level of the logging from inside
              of the oper8 library.
        type: str
        required: false
    log_filters:
        description:
            - This sets per-channel verbosity levels for the logging from inside
              of the oper8 library.
        type: str
        required: false
    log_json:
        description:
            - This sets the output formatter for oper8 logging to be
              'json' rather than 'pretty'
        type: bool
        required: false
    log_file:
        description:
            - This adds a file path to send log output to
        type: str
        required: false
    log_thread_id:
        description:
            - This adds the unique thread id to each log message
        type: bool
        required: false

    dry_run:
        description:
            - This sets the operator to render-only mode and will not actually
              manage the downstream resources.
        type: bool
        required: false
    standalone:
        description:
            - This sets the oper8 library to operate outside of
              'operator' mode, enabling the playbook to be run directly from the
              command line rather than inside of a deployed operator.
        type: bool
        required: false
    working_dir:
        description:
            - This sets a named working directory to be used by the
              oper8 library rather than letting it create a random
              ephemeral working dir internally.
            - WARNING: Setting a working_dir is for debugging ONLY! It will
              leave the generated resource files behind which can cause
              problems if the set of files and/or names changes between deploys.
        type: str
        required: false

author:
    - Gabe Goodhart (@gabe.l.hart)
"""

EXAMPLES = """
# Deploy the mlflow application
- name: MLFlow Controller
  k8s_application:
    controller_class: mlflow_application.application.mlflow.MLFlowController
    full_cr: "{{ _org_oper8_mlflow_mlflow }}"

"""

RETURN = """
success:
    description: Whether or not the deployment rolled out successfully
    type: bool
    returned: always
log_output:
    description: Full string of log messages produced during the run
    type: str
    returned: always
should_requeue:
    description: Whether or not the reconcile request should be re-queued
    type: bool
    returned: always
requeue_after:
    description: Tells the controller runtime to requeue the reconcile request after the duration
    type: string
    returned: always
    sample: '60s'
"""


## Module Class ################################################################

log = alog.use_channel("K8S-APP")


class KubernetesAnsibleApplicationModule(AnsibleModule, K8sAnsibleMixin):
    """This ansible module implements management of a full application via
    the oper8 modules.
    """

    @property
    def argspec(self):
        """Inherit the argspec for a K8sAnsibleMixin and add any necessary
        overrides
        """
        spec = {}

        # Add an argument for the version. This is needed so that the operator
        # can pass its version through to sub-CRs
        spec["version"] = dict(type="str", required=True)

        # Add an argument to specify the python class to use for the application
        spec["controller_class"] = dict(type="str", required=True)

        # Determine whether this is running as a finalizer (remove) or
        # reconciler (add)
        spec["operation"] = dict(type="str", default="add")

        # If true, oper8 will emulate the status management done natively by
        # ansible based on the readiness values of oper8's native status
        # management
        spec["manage_ansible_status"] = dict(type="bool", default=False)

        # The directory where ansible-vcs sources its version checkouts
        spec["vcs_dir"] = dict(type="str", default="/opt/ansible/app/")
        spec["vcs_dest"] = dict(
            type="str", default="/opt/ansible/app/version/{version}/{kind}/{name}/"
        )
        spec["enable_ansible_vcs"] = dict(type="bool", default=False)

        # If true, oper8 will require that a list of supported_versions is found
        # and that the spec.version value is in the list
        spec["strict_versioning"] = dict(type="bool", default=False)

        # Add arguments to dry run the deploy and specify the working dir
        spec["dry_run"] = dict(type="bool", default=False)
        spec["standalone"] = dict(type="bool", default=False)
        spec["working_dir"] = dict(type="str", default=config.working_dir or None)

        # Add arguments for alog configuration
        spec["log_level"] = dict(type="str", default="")
        spec["log_filters"] = dict(type="str", default="")
        spec["log_json"] = dict(type="bool", default=False)
        spec["log_thread_id"] = dict(type="bool", default=False)
        spec["log_file"] = dict(type="str", default=None)

        # This parameter is where the full CR spec will live
        spec["full_cr"] = dict(type="str", required=True)

        # Return the fully constructed spec
        return spec

    def __init__(self, *args, **kwargs):
        """Passthrough constructor"""
        kwargs["argument_spec"] = self.argspec
        super().__init__(*args, **kwargs)

    def execute_module(self):
        """Execute the constructed module by parsing the resource config"""

        # Parse the full CR content
        try:
            full_cr = ast.literal_eval(self.params["full_cr"])
        except (ValueError, SyntaxError) as e:
            return self.fail_json(
                msg="Failed to parse full_cr", err=str(e), success=False
            )

        # BACKWARDS COMPATIBILITY -- For most parameters the configurations are
        # now library-level via the config package. To avoid breaking existing
        # workflows, we allow them to be overridden for each reconciliation. In
        # reality, they will always be set the same since they are generally
        # static for a given invocation of the operator.
        config.dry_run = self.params["dry_run"]
        config.standalone = self.params["standalone"]
        config.working_dir = self.params.get("working_dir", config.working_dir)
        config.strict_versioning = self.params["strict_versioning"]

        # Configure VCS config
        if self.params["enable_ansible_vcs"]:
            config.vcs.enabled = True
            if not config.vcs.version_override and self.params["version"]:
                config.vcs.version_override = self.params["version"]

        # If VCS dir or dest weren't overridden by config than supply the
        # default ansible directory and destination
        if not config.vcs.repo:
            config.vcs.repo = self.params["vcs_dir"]
        if not config.vcs.dest:
            config.vcs.dest = self.params["vcs_dest"]

        # Configure Logging values
        if self.params["log_level"]:
            config.log_level = self.params["log_level"]
        if self.params["log_filters"]:
            config.log_filters = self.params["log_filters"]
        if self.params["log_json"]:
            config.log_json = self.params["log_json"]
        if self.params["log_thread_id"]:
            config.log_thread_id = self.params["log_thread_id"]

        # Add the Ansible log formatter
        if self.params["log_file"] is not None:
            # Ensure log directory exists
            log_file_path = pathlib.Path(self.params["log_file"])
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Remove all of the current handlers as STDOUT is captured by ansible
            # thus can't be used for logging
            root_logger = logging.getLogger()
            root_logger.handlers.clear()

            handler = AutoRotatingFileHandler(self.params["log_file"])
            handler.setFormatter(alog.AlogPrettyFormatter())
            root_logger.addHandler(handler)

        # Invoke the reconcile
        is_finalizer = self.params["operation"] == "remove"
        reconcile_manager = ReconcileManager()
        reconcile_result = reconcile_manager.safe_reconcile(
            self.params["controller_class"], full_cr, is_finalizer
        )

        # Return the success value
        requeue_seconds = reconcile_result.requeue_params.requeue_after.total_seconds()
        return self.exit_json(
            success=True,
            should_requeue=reconcile_result.requeue,
            requeue_after=f"{round(requeue_seconds)}s",
        )


## Main ########################################################################


def main():
    KubernetesAnsibleApplicationModule().execute_module()


if __name__ == "__main__":  # pragma: no cover
    main()
