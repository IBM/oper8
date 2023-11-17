"""
This is the main entrypoint command for running the operator
"""
# Standard
from typing import List, Optional, Type
import argparse
import importlib
import os
import signal

# Third Party
import yaml

# First Party
import alog

# Local
from .. import config, watch_manager
from ..controller import Controller
from ..deploy_manager import DryRunDeployManager
from ..exceptions import ConfigError
from .base import CmdBase

log = alog.use_channel("MAIN")


class RunOperatorCmd(CmdBase):
    __doc__ = __doc__

    ## Interface ##

    def add_subparser(
        self,
        subparsers: argparse._SubParsersAction,
    ) -> argparse.ArgumentParser:
        parser = subparsers.add_parser("run", help=__doc__)
        runtime_args = parser.add_argument_group("Runtime Configuration")
        runtime_args.add_argument(
            "--module_name",
            "-m",
            required=True,
            help="The module to import that holds the operator code",
        )
        runtime_args.add_argument(
            "--cr",
            "-c",
            default=None,
            help="(dry run) A CR manifest yaml to apply directly ",
        )
        runtime_args.add_argument(
            "--resource_dir",
            "-r",
            default=None,
            help="(dry run) Path to a directory of yaml files that should exist in the cluster",
        )
        return parser

    def cmd(self, args: argparse.Namespace):
        # Validate args
        assert args.cr is None or (
            config.dry_run and os.path.isfile(args.cr)
        ), "Can only specify --cr with dry run and it must point to a valid file"
        assert args.resource_dir is None or (
            config.dry_run and os.path.isdir(args.resource_dir)
        ), "Can only specify --resource_dir with dry run and it must point to a valid directory"

        # Find all controllers in the operator library
        controller_types = self._get_controller_types(
            args.module_name, args.controller_name
        )

        # Parse pre-populated resources if needed
        resources = self._parse_resource_dir(args.resource_dir)

        # Create the watch managers
        deploy_manager = self._setup_watches(controller_types, resources)

        # Register the signal handler to stop the watches
        def do_stop(*_, **__):  # pragma: no cover
            watch_manager.stop_all()

        signal.signal(signal.SIGINT, do_stop)

        # Run the watch manager
        log.info("Starting Watches")
        watch_manager.start_all()

        # If given, apply the CR directly
        if args.cr:
            log.info("Applying CR [%s]", args.cr)
            with open(args.cr, encoding="utf-8") as handle:
                cr_manifest = yaml.safe_load(handle)
                cr_manifest.setdefault("metadata", {}).setdefault(
                    "namespace", "default"
                )
                log.debug3(cr_manifest)
                deploy_manager.deploy([cr_manifest])

        # All done!
        log.info("SHUTTING DOWN")

    ## Impl ##

    @staticmethod
    def _is_controller_type(attr_val: str):
        """Determine if a given attribute value is a controller type"""
        return (
            isinstance(attr_val, type)
            and issubclass(attr_val, Controller)
            and attr_val is not Controller
        )

    @classmethod
    def _get_controller_types(cls, module_name: str, controller_name=""):
        """Import the operator library and either extract all Controllers,
        or just extract the provided Controller"""
        module = importlib.import_module(module_name)
        log.debug4(dir(module))
        controller_types = []

        if controller_name:
            # Confirm that the class exists and that it is a controller type
            try:
                controller_attr_val = getattr(module, controller_name)
                is_valid_controller = cls._is_controller_type(controller_attr_val)
            except AttributeError:
                is_valid_controller = False

            if is_valid_controller:
                log.debug3("Provided controller, %s, is valid", controller_name)
                controller_types.append(controller_attr_val)
            else:
                raise AttributeError(
                    f"Provided controller, {controller_name}, is invalid"
                )
        else:
            log.debug3("Searching for all controllers...")
            for attr in dir(module):
                attr_val = getattr(module, attr)
                if cls._is_controller_type(attr_val):
                    log.debug2("Found Controller: %s", attr_val)
                    controller_types.append(attr_val)

        assert controller_types, f"No Controllers found in [{module_name}]"
        return controller_types

    @staticmethod
    def _parse_resource_dir(resource_dir: Optional[str]):
        """If given, this will parse all yaml files found in the given directory"""
        all_resources = []
        if resource_dir is not None:
            for fname in os.listdir(resource_dir):
                if fname.endswith(".yaml") or fname.endswith(".yml"):
                    resource_path = os.path.join(resource_dir, fname)
                    log.debug3("Reading resource file [%s]", resource_path)
                    with open(resource_path, encoding="utf-8") as handle:
                        all_resources.extend(yaml.safe_load_all(handle))
        return all_resources

    @staticmethod
    def _setup_watches(
        controller_types: List[Type[Controller]],
        resources: List[dict],
    ) -> Optional[DryRunDeployManager]:
        """Set up watches for all controllers. If in dry run mode, the
        DryRunDeployManager will be returned.
        """
        deploy_manager = None
        extra_kwargs = {}
        if config.dry_run:
            log.info("Running DRY RUN")
            deploy_manager = DryRunDeployManager(resources=resources)
            wm_type = watch_manager.DryRunWatchManager
            extra_kwargs["deploy_manager"] = deploy_manager
        elif config.watch_manager == "ansible":  # pragma: no cover
            log.info("Running Ansible Operator")
            wm_type = watch_manager.AnsibleWatchManager
        elif config.watch_manager == "python":  # pragma: no cover
            log.info("Running Python Operator")
            wm_type = watch_manager.PythonWatchManager
        else:
            raise ConfigError(f"Unknown watch manager {config.watch_manager}")

        for controller_type in controller_types:
            log.debug("Registering watch for %s", controller_type)
            wm_type(controller_type=controller_type, **extra_kwargs)
        return deploy_manager
