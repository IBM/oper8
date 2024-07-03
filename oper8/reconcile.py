"""
The ReconcileManager class manages an individual reconcile of a controller.
This setups up the session, constructs the controller, and runs its reconcile
"""

# Standard
from dataclasses import dataclass, field
from typing import List, Optional, Set, Type, Union
import base64
import copy
import datetime
import importlib
import json
import logging
import os
import pathlib
import random
import sys
import uuid

# Third Party
import dateutil

# First Party
import aconfig
import alog

# Local
from . import config, constants, status
from .dag import CompletionState
from .deploy_manager import (
    DeployManagerBase,
    DryRunDeployManager,
    OpenshiftDeployManager,
)
from .exceptions import (
    ClusterError,
    ConfigError,
    Oper8ExpectedError,
    Oper8FatalError,
    PreconditionError,
    RolloutError,
    VerificationError,
    assert_cluster,
    assert_config,
)
from .log_format import Oper8JsonFormatter
from .session import Session
from .utils import add_finalizer, get_manifest_version, merge_configs, remove_finalizer
from .vcs import VCS, VCSCheckoutMethod, VCSMultiProcessError

log = alog.use_channel("RECONCILE")


## Data models #################################################################


@dataclass
class RequeueParams:
    """RequeueParams holds parameters for requeue request"""

    requeue_after: datetime.timedelta = field(
        default_factory=lambda: datetime.timedelta(
            seconds=float(config.requeue_after_seconds)
        )
    )


@dataclass
class ReconciliationResult:
    """ReconciliationResult is the result of a reconciliation session"""

    # Flag to control requeue of current reconcile request
    requeue: bool
    # Parameters for requeue request
    requeue_params: RequeueParams = field(default_factory=RequeueParams)
    # Flag to identify if the reconciliation raised an exception
    exception: Exception = None


# Forward declarations of Controller
CONTROLLER_TYPE = "Controller"
CONTROLLER_CLASS_TYPE = Type[CONTROLLER_TYPE]

# Type helper for describing a controller. CONTROLLER_INFO
# can be a str in the form "module.class", a Controller class that
# will be initialized, or an already initialized Controller. While the
# first two methods are preferred, an already created Controller can
# be useful for tests and backwards compatibility
CONTROLLER_INFO = Union[str, CONTROLLER_CLASS_TYPE, CONTROLLER_TYPE]

## ReconcileManager #################################################################


class ReconcileManager:  # pylint: disable=too-many-lines
    """This class manages reconciliations for an instance of Oper8. It's
    primary function is to run reconciles given a CR manifest, Controller,
    and the current cluster state via a DeployManager.
    """

    ## Construction ############################################################

    def __init__(
        self,
        home_dir: str = None,
        deploy_manager: Optional[DeployManagerBase] = None,
        enable_vcs: Optional[bool] = None,
        reimport_controller: Optional[bool] = True,
    ):
        """The constructor sets up the properties used across every
        reconcile and checks that the current config is valid.

        Args:
            home_dir:  Optional[str]=None
                The root directory for importing controllers or VCS checkout
            deploy_manager:  Optional[DeployManager]=None
                Deploy manager to use. If not given, a new DeployManager will
                be created for each reconcile.
            enable_vcs:  Optional[bool]=True
                Parameter to manually control the state of VCS on a per instance
                basis
            reimport_controller:  Optional[bool]=None
                Parameter to manually control if a controller needs to be reimported each
                reconcile.
        """

        if home_dir:
            self.home_dir = home_dir
        elif config.vcs.enabled:
            self.home_dir = config.vcs.repo
        else:
            self.home_dir = os.getcwd()

        self.vcs = None

        # If enable_vcs is not provided than default to
        # config
        if enable_vcs is None:
            enable_vcs = config.vcs.enabled

        if enable_vcs:
            assert_config(
                config.vcs.repo,
                "Can not enable vcs without supply source repo at vcs.repo",
            )
            assert_config(
                config.vcs.dest,
                "Cannot require enable vcs without providing a destination",
            )
            vcs_checkout_methods = [method.value for method in VCSCheckoutMethod]
            assert_config(
                config.vcs.checkout_method in vcs_checkout_methods,
                f"VCS checkout method must be one of the following {vcs_checkout_methods}",
            )

            self.vcs = VCS(self.home_dir)

        # Ensure config is setup correctly for strict_versioning
        if config.strict_versioning:
            assert_config(
                config.supported_versions is not None,
                "Must provide supported_versions with strict_versioning=True",
            )
            assert_config(
                config.vcs.field is not None,
                "Must provide vcs.field with strict_versioning=True",
            )

        self.deploy_manager = deploy_manager
        self.reimport_controller = reimport_controller

    ## Reconciliation ############################################################

    @alog.logged_function(log.info)
    @alog.timed_function(log.info, "Reconcile finished in: ")
    def reconcile(
        self,
        controller_info: CONTROLLER_INFO,
        resource: Union[dict, aconfig.Config],
        is_finalizer: bool = False,
    ) -> ReconciliationResult:
        """This is the main entrypoint for reconciliations and contains the
        core implementation. The general reconcile path is as follows:

            1. Parse the raw CR manifest
            2. Setup logging based on config with overrides from CR
            3. Check if the CR is paused and for strict versioning
            4. Setup directory if VCS is enabled
            5. Import and construct the Controller
            6. Setup the DeployManager and Session objects
            7. Run the Controller reconcile

        Args:
            controller_info: CONTROLLER_INFO
                The description of a controller. See CONTROLLER_INFO for
                more information
            resource: Union[dict, aconfig.Config]
                A raw representation of the resource to be reconciled
            is_finalizer: bool=False
                Whether the resource is being deleted

        Returns:
            reconcile_result:  ReconciliationResult
                The result of the reconcile
        """

        # Parse the full CR content
        cr_manifest = self.parse_manifest(resource)

        # generate id unique to this session
        reconcile_id = self.generate_id()

        # Initialize logging prior to any other work
        self.configure_logging(cr_manifest, reconcile_id)

        # If paused, do nothing and don't requeue
        if self._is_paused(cr_manifest):
            log.info("CR is paused. Exiting reconciliation")
            result = ReconciliationResult(requeue=False, requeue_params=RequeueParams())
            return result

        # Check strict versioning before continuing
        if config.strict_versioning:
            self._check_strict_versioning(cr_manifest)

        # Check if VCS is enabled and then attempt to checkout
        if config.vcs.enabled:
            self.setup_vcs(cr_manifest)

        # Import controller and setup the instance
        controller = self.setup_controller(controller_info)

        # Configure deploy manager on a per reconcile basis for
        # owner references unless a manager is provided on initialization
        deploy_manager = self.setup_deploy_manager(cr_manifest)

        # Setup Session
        session = self.setup_session(
            controller, cr_manifest, deploy_manager, reconcile_id
        )

        # Run the controller reconcile
        result = self.run_controller(controller, session, is_finalizer)

        return result

    def safe_reconcile(
        self,
        controller_info: CONTROLLER_INFO,
        resource: dict,
        is_finalizer: bool = False,
    ) -> ReconciliationResult:
        """
        This function calls out to reconcile but catches any errors thrown. This
        function guarantees a safe result which is needed by some Watch Managers

        Args:
            controller_info: CONTROLLER_INFO
                The description of a controller. See CONTROLLER_INFO for
                more information
            resource: Union[dict, aconfig.Config]
                A raw representation of the reconcile
            is_finalize: bool=False
                Whether the resource is being deleted

        Returns:
            reconcile_result:  ReconciliationResult
                The result of the reconcile

        """

        try:
            return self.reconcile(controller_info, resource, is_finalizer)

        # VCSMultiProcessError is an expected error caused by oper8 which should
        # not be handled by the exception handling code
        except VCSMultiProcessError as exc:
            # Requeue after ~7.5 seconds. Add randomness to avoid
            # repeated conflicts
            requeue_time = 5 + random.uniform(0, 5)
            params = RequeueParams(
                requeue_after=datetime.timedelta(seconds=requeue_time)
            )
            log.debug("VCS Multiprocessing Error Detected: {%s}", exc, exc_info=True)
            log.warning(
                "VCS Setup failed due to other process. Requeueing in %ss",
                requeue_time,
            )
            return ReconciliationResult(
                requeue=True, requeue_params=params, exception=exc
            )

        # Capture all generic exceptions
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Handling caught error in reconcile: %s", exc, exc_info=True)
            error = exc

        if config.manage_status:
            try:
                self._update_error_status(resource, error)
                log.debug("Update CR status with error message")
            except Exception as exc:  # pylint: disable=broad-except
                log.error("Failed to update status: %s", exc, exc_info=True)

        # If we got to this return it means there was an
        # exception during reconcile and we should requeue
        # with the default backoff period
        log.info("Requeuing CR due to error during reconcile")
        return ReconciliationResult(
            requeue=True, requeue_params=RequeueParams(), exception=error
        )

    ## Reconciliation Stages ############################################################

    @classmethod
    def parse_manifest(cls, resource: Union[dict, aconfig.Config]) -> aconfig.Config:
        """Parse a raw resource into an aconfig Config

        Args:
            resource: Union[dict, aconfig.Config])
                The resource to be parsed into a manifest

        Returns
            cr_manifest: aconfig.Config
                The parsed and validated config
        """
        try:
            cr_manifest = aconfig.Config(resource, override_env_vars=False)
        except (ValueError, SyntaxError, AttributeError) as exc:
            raise ValueError("Failed to parse full_cr") from exc

        return cr_manifest

    @classmethod
    def configure_logging(cls, cr_manifest: aconfig.Config, reconciliation_id: str):
        """Configure the logging for a given reconcile

        Args:
            cr_manifest: aconfig.Config
                The resource to get annotation overrides from
            reconciliation_id: str
                The unique id for the reconciliation
        """

        # Fetch the annotations for logging
        # NOTE: We use safe fetching here because this happens before CR
        #   verification in the Session constructor
        annotations = cr_manifest.get("metadata", {}).get("annotations", {})
        default_level = annotations.get(
            constants.LOG_DEFAULT_LEVEL_NAME, config.log_level
        )

        filters = annotations.get(constants.LOG_FILTERS_NAME, config.log_filters)
        log_json = annotations.get(constants.LOG_JSON_NAME, str(config.log_json))
        log_thread_id = annotations.get(
            constants.LOG_THREAD_ID_NAME, str(config.log_thread_id)
        )

        # Convert boolean args
        log_json = (log_json or "").lower() == "true"
        log_thread_id = (log_thread_id or "").lower() == "true"

        # Keep the old handler. This is useful if running with ansible as
        # it will preserve the handler generator set up to log to a file
        # since ansible captures all logging output
        handler_generator = None
        if logging.root.handlers:
            old_handler = logging.root.handlers[0]

            def handler_generator():
                return old_handler

        alog.configure(
            default_level=default_level,
            filters=filters,
            formatter=Oper8JsonFormatter(cr_manifest, reconciliation_id)
            if log_json
            else "pretty",
            thread_id=log_thread_id,
            handler_generator=handler_generator,
        )

    @classmethod
    def generate_id(cls) -> str:
        """Generates a unique human readable id for this reconciliation

        Returns:
            id: str
                A unique base32 encoded id
        """
        uuid4 = uuid.uuid4()
        base32_str = base64.b32encode(uuid4.bytes).decode("utf-8")
        reconcile_id = base32_str[:22]
        log.debug("Generated reconcile id: %s", reconcile_id)
        return reconcile_id

    def setup_vcs(self, cr_manifest: aconfig.Config):
        """Setups the VCS directory and sys.path for a reconcile.
        This function also ensures that the version is valid if
        config.strict_versioning is enabled.

        Args:
            cr_manifest: aconfig.Config
                The cr manifest to pull the requested version from.
        """
        version = get_manifest_version(cr_manifest)
        if not version:
            raise ValueError("CR Manifest has no version")

        log.debug(
            "Setting up working directory with src: %s and version: %s",
            self.home_dir,
            version,
        )
        working_dir = self._setup_directory(cr_manifest, version)

        # Construct working dir path from vcs and git directory
        if config.vcs.module_dir:
            module_path = pathlib.Path(config.vcs.module_dir)
            working_dir = working_dir / module_path

        if not working_dir.is_dir():
            log.error(
                "Working directory %s could not be found. Invalid module path",
                working_dir,
            )
            raise ConfigError(
                f"Module path: '{module_path}' could not be found in repository"
            )

        log.debug4("Changing working directory to %s", working_dir)
        os.chdir(working_dir)
        sys.path.insert(0, str(working_dir))

    def setup_controller(
        self, controller_info: CONTROLLER_INFO
    ) -> CONTROLLER_CLASS_TYPE:
        """
        Import the requested Controller class and enable any compatibility layers

        Args:
            controller_info:CONTROLLER_INFO
                The description of a controller. See CONTROLLER_INFO for
                more information
        Returns:
            controller:
                The required Controller Class
        """

        # Local
        from .controller import (  # pylint: disable=import-outside-toplevel, cyclic-import
            Controller,
        )

        # If controller info is already a constructed controller then
        # skip importing
        if isinstance(controller_info, Controller):
            return controller_info

        controller_class = self._import_controller(controller_info)
        return self._configure_controller(controller_class)

    def setup_deploy_manager(self, cr_manifest: aconfig.Config) -> DeployManagerBase:
        """
        Configure a deploy_manager for a reconcile given a manifest

        Args:
            cr_manifest: aconfig.Config
                The resource to be used as an owner_ref

        Returns:
            deploy_manager: DeployManagerBase
                The deploy_manager to be used during reconcile
        """
        if self.deploy_manager:
            return self.deploy_manager

        if config.dry_run:
            log.debug("Using DryRunDeployManager")
            return DryRunDeployManager()

        log.debug("Using OpenshiftDeployManager")
        return OpenshiftDeployManager(owner_cr=cr_manifest)

    def setup_session(
        self,
        controller: CONTROLLER_TYPE,
        cr_manifest: aconfig.Config,
        deploy_manager: DeployManagerBase,
        reconciliation_id: str,
    ) -> Session:
        """Construct the session, including gathering the backend config and any temp patches

        Args:
            controller: Controller
                The controller class being reconciled
            cr_manifest: aconfig.Config
                The resource being reconciled
            deploy_manager: DeployManagerBase
                The deploy manager used in the cluster
            reconciliation_id: str
                The id for the reconcile

        Return:
            session: Session
                The session for reconcile
        """
        # Get backend config for reconciliation
        controller_defaults = controller.get_config_defaults()
        reconciliation_config = self._get_reconcile_config(
            cr_manifest=cr_manifest,
            deploy_manager=deploy_manager,
            controller_defaults=controller_defaults,
        )
        log.debug4("Gathered Config: %s", reconciliation_config)

        # Get Temporary patches
        patches = self._get_temp_patches(deploy_manager, cr_manifest)
        log.debug3("Found %d patches", len(patches))

        # Get the complete CR Manifest including defaults
        cr_manifest_defaults = controller.get_cr_manifest_defaults()
        full_cr_manifest = merge_configs(
            aconfig.Config(cr_manifest_defaults),
            cr_manifest,
        )

        return Session(
            reconciliation_id=reconciliation_id,
            cr_manifest=full_cr_manifest,
            config=reconciliation_config,
            deploy_manager=deploy_manager,
            temporary_patches=patches,
        )

    def run_controller(
        self, controller: CONTROLLER_TYPE, session: Session, is_finalizer: bool
    ) -> ReconciliationResult:
        """Run the Controller's reconciliation or finalizer with the constructed Session.
        This function also updates the CR status and handles requeue logic.

        Args:
            controller: Controller
                The Controller being reconciled
            session: Session
                The current Session state
            is_finalizer:
                Whether the resource is being deleted

        Returns:
            reconciliation_result: ReconciliationResult
                The result of the reconcile
        """
        log.info(
            "%s resource %s/%s/%s",
            "Finalizing" if is_finalizer else "Reconciling",
            session.kind,
            session.namespace,
            session.name,
        )

        # Ensure the resource has the proper finalizers
        if controller.has_finalizer:
            add_finalizer(session, controller.finalizer)

        # Update the Resource status
        if config.manage_status:
            self._update_reconcile_start_status(session)

        # Reconcile the controller
        completion_state = controller.run_reconcile(
            session,
            is_finalizer=is_finalizer,
        )

        if config.manage_status:
            self._update_reconcile_completion_status(session, completion_state)

        # Check if the controller session should requeue
        requeue, requeue_params = controller.should_requeue(session)
        if not requeue_params:
            requeue_params = RequeueParams()

        # Remove managed finalizers if not requeuing
        if not requeue and is_finalizer and controller.has_finalizer:
            remove_finalizer(session, controller.finalizer)

        return ReconciliationResult(requeue=requeue, requeue_params=requeue_params)

    ## Implementation Details ############################################################

    @classmethod
    def _is_paused(cls, cr_manifest: aconfig.Config) -> bool:
        """Check if a manifest has a paused annotation

        Args:
            cr_manifest: aconfig.Config
                The manifest becking checked

        Returns:
            is_paused: bool
                If the manifest contains the paused annotation
        """
        annotations = cr_manifest.metadata.get("annotations", {})
        paused = annotations.get(constants.PAUSE_ANNOTATION_NAME)
        return paused and paused.lower() == "true"

    def _check_strict_versioning(self, cr_manifest: aconfig.Config):
        """Check the version against config and vcs directory

        Args:
            version_directory: str
                The repo directory to check
            version: str
                The version from the manifest
        """
        version = get_manifest_version(cr_manifest)
        if not version:
            raise ValueError("CR Manifest has no version")

        # Ensure version is in list of supported versions
        assert_config(
            version in config.supported_versions,
            f"Unsupported version: {version}",
        )

        # If VCS is enabled ensure the branch or tag exists
        if self.vcs:
            repo_versions = self.vcs.list_refs()
            assert_config(
                version in repo_versions,
                f"Version not found in repo: {version}",
            )
            log.debug3("Supported VCS Versions: %s", repo_versions)

    def _setup_directory(
        self, cr_manifest: aconfig.Config, version: str
    ) -> pathlib.Path:
        """Construct the VCS directory from the cr_manifest and version. Then
        checkout the directory

        Args:
            cr_manifest: aconfig.Config
                The manifest to be used for the checkout path
            version: str
                The version to checkout

        Returns:
            destination_directory: pathlib.Path
                The destination directory for the checkout
        """

        # Generate checkout directory and ensure path exists
        def sanitize_for_path(path):
            keepcharacters = (" ", ".", "_")
            return "".join(
                c for c in path if c.isalnum() or c in keepcharacters
            ).rstrip()

        # Setup destination templating to allow for CR specific checkout paths
        # The entirety of the cr_manifest is included as a dict as well as some
        # custom keys
        template_mappings = {
            # Include the entire dict first so that the sanitized default values
            # take precedence
            **cr_manifest,
            "version": version,
            "kind": sanitize_for_path(cr_manifest.kind),
            "apiVersion": sanitize_for_path(cr_manifest.apiVersion),
            "namespace": sanitize_for_path(cr_manifest.metadata.namespace),
            "name": sanitize_for_path(cr_manifest.metadata.name),
        }

        # Get the checkout directory and method
        try:
            formatted_path = config.vcs.dest.format(**template_mappings)
        except KeyError as exc:
            log.warning(
                "Invalid key: %s found in vcs destination template", exc, exc_info=True
            )
            raise ConfigError("Invalid Key found in vcs destination template") from exc

        checkout_dir = pathlib.Path(formatted_path)
        checkout_method = VCSCheckoutMethod(config.vcs.checkout_method)

        log.debug2(
            "Checking out into directory %s with method %s",
            checkout_dir,
            checkout_method.value,
        )
        self.vcs.checkout_ref(version, checkout_dir, checkout_method)
        return checkout_dir

    @staticmethod
    def _unimport_controller_module(module_name: str) -> Set[str]:
        """Helper to un-import the given module and its parents/siblings/
        children

        Args:
            module_name: str
                The name of the module that holds the Controller

        Returns:
            reimport_modules: Set[str]
                All modules that were unimported and will need to be reimported
        """
        reimport_modules = set()
        if module_name in sys.modules:
            log.debug2("UnImporting controller module: %s", module_name)
            sys.modules.pop(module_name)
            reimport_modules.add(module_name)

        # UnImport the controller and any parent/sibling/child modules so
        # controller can be reimported from the most recent sys path
        module_parts = module_name.split(".")
        for i in range(1, len(module_parts)):
            parent_module = ".".join(module_parts[:-i])
            if parent_module in sys.modules:
                log.debug3("UnImporting module: %s", parent_module)
                if sys.modules.pop(parent_module, None):
                    reimport_modules.add(parent_module)
        for child_module in [
            mod_name
            for mod_name in sys.modules
            if mod_name.startswith(f"{module_parts[0]}.")
        ]:
            log.debug3("UnImporting child module: %s", child_module)
            if sys.modules.pop(child_module, None):
                reimport_modules.add(child_module)
        return reimport_modules

    def _import_controller(
        self, controller_info: CONTROLLER_INFO
    ) -> CONTROLLER_CLASS_TYPE:
        """Parse the controller info and reimport the controller

        Args:
            controller_info:CONTROLLER_INFO
                The description of a controller. See CONTROLLER_INFO for
                more information
        Returns:
            controller_class: Type[Controller]
                The reimported Controller

        """
        log.debug2("Parsing controller_info")
        if isinstance(controller_info, str):
            class_module_parts = controller_info.rsplit(".", maxsplit=1)
            assert_config(
                len(class_module_parts) == 2,
                f"Invalid controller_class [{controller_info}]. Format is <module>.<class>",
            )
            module_name, class_name = class_module_parts
        else:
            class_name = controller_info.__name__
            module_name = controller_info.__module__

        # Reimport module if reimporting is enabled and if it already exists
        log.debug3(
            "Running controller %s from module %s [reimport? %s, in sys.modules? %s]",
            class_name,
            module_name,
            self.reimport_controller,
            module_name in sys.modules,
        )
        reimport_modules = {module_name}
        if self.reimport_controller:
            reimport_modules = reimport_modules.union(
                self._unimport_controller_module(module_name)
            )

        # Attempt to import the modules
        log.debug2("Attempting to import [%s.%s]", module_name, class_name)
        for reimport_name in reimport_modules:
            try:
                app_module = importlib.import_module(reimport_name)
                if reimport_name == module_name:
                    if not hasattr(app_module, class_name):
                        raise ConfigError(
                            f"Invalid controller_class [{class_name}]."
                            f" Class not found in module [{reimport_name}]"
                        )
                    controller_class = getattr(app_module, class_name)

                    # Import controller in function to avoid circular imports
                    # Local
                    from .controller import (  # pylint: disable=import-outside-toplevel
                        Controller,
                    )

                    if not issubclass(controller_class, Controller):
                        raise ConfigError(
                            f"Invalid controller_class [{module_name}.{class_name}]."
                            f" [{class_name}] is not a Controller"
                        )

            except ImportError as exc:
                # If this is the module that holds the controller, it _needs_ to
                # be reimported
                if reimport_name == module_name:
                    log.error(
                        "Failed to import [%s.%s]. Failed to import [%s]",
                        reimport_name,
                        class_name,
                        reimport_name,
                        exc_info=True,
                    )
                    raise ConfigError("Invalid Controller Class Specified") from exc
                # Otherwise, it's ok for import to fail
                else:
                    log.debug("Not able to reimport %s", reimport_name)

        log.debug(
            "Imported Controller %s from file %s",
            controller_class,
            sys.modules[controller_class.__module__].__file__,
        )

        return controller_class

    def _configure_controller(
        self, controller_class: CONTROLLER_CLASS_TYPE
    ) -> CONTROLLER_TYPE:
        """Construct the Controller Class

        Args:
            controller_class: CONTROLLER_CLASS_TYPE
                The Controller class to be reconciled

        Returns:
            controller: Controller
                The constructed Controller

        """
        log.debug3("Constructing controller")
        controller = controller_class()
        return controller

    def _get_reconcile_config(
        self,
        cr_manifest: aconfig.Config,
        deploy_manager: DeployManagerBase,
        controller_defaults: aconfig.Config,
    ) -> aconfig.Config:
        """Construct the flattened backend config for this reconciliation
        starting with a deepcopy of the base and merge in overrides from the CR

        Args:
            cr_manifest: aconfig.Config:
                The manifest to get overrides from
            deploy_manager: DeployManagerBase:
                The deploy manager to get the default configmap config
            controller_defaults: aconfig.Config:
                The config defaults provided by the controller class

        Returns:
            reconcile_config: aconfig.Config
                The reconciliation config
        """
        metadata = cr_manifest.get("metadata", {})
        annotations = metadata.get("annotations", {})
        namespace = metadata.get("namespace")
        cr_config_defaults = cr_manifest.get(constants.CONFIG_OVERRIDES, {})
        annotation_config_defaults = {}
        if constants.CONFIG_DEFAULTS_ANNOTATION_NAME in annotations:
            log.debug("Pulling config_defaults based on annotation")
            config_defaults_name = annotations[
                constants.CONFIG_DEFAULTS_ANNOTATION_NAME
            ]

            # Allow sub-keys to be deliniated by "/"
            parts = config_defaults_name.split("/")
            config_defaults_cm_name = parts[0]

            log.debug2(
                "Reading config_defaults from ConfigMap [%s]", config_defaults_cm_name
            )
            success, content = deploy_manager.get_object_current_state(
                kind="ConfigMap",
                name=config_defaults_cm_name,
                namespace=namespace,
                api_version="v1",
            )
            assert_cluster(success, "Failed to look up config defaults form ConfigMap")
            assert_config(
                content is not None,
                f"Did not find configured config defaults ConfigMap: {config_defaults_cm_name}",
            )
            assert_config("data" in content, "Got ConfigMap content with out 'data'")
            config_defaults_content = content["data"]
            assert_config(
                isinstance(config_defaults_content, dict),
                f"Incorrectly formatted config_defaults ConfigMap: {config_defaults_cm_name}",
            )

            # Parse as a Config
            log.debug2("Parsing app config dict")
            annotation_config_defaults = aconfig.Config(
                config_defaults_content, override_env_vars=False
            )

        return merge_configs(
            copy.deepcopy(controller_defaults),
            merge_configs(annotation_config_defaults, cr_config_defaults),
        )

    def _get_temp_patches(  # pylint: disable=too-many-locals
        self, deploy_manager: DeployManagerBase, cr_manifest: aconfig.Config
    ) -> List[aconfig.Config]:
        """Fetch the ordered list of temporary patches that should apply to this
        rollout.

        Args:
            deploy_manager: DeployManagerBase
                The DeployManager used to get the current temporary patches
            cr_manifest: aconfig.Config
                The manifest of this reconciliation
        """

        # Look for patch annotations on the CR
        patch_annotation = (
            cr_manifest.get("metadata", {})
            .get("annotations", {})
            .get(constants.TEMPORARY_PATCHES_ANNOTATION_NAME, "{}")
        )
        log.debug3("Raw patch annotation: %s", patch_annotation)
        try:
            raw_patches = json.loads(patch_annotation)
            if not isinstance(raw_patches, dict):
                msg = f"Patches annotation not a dict: {raw_patches}"
                log.error(msg)
                raise RolloutError(msg)
            patches = {}
            for patch_name, patch_meta in raw_patches.items():
                patch_meta["timestamp"] = dateutil.parser.parse(patch_meta["timestamp"])
                patches[patch_name] = patch_meta
                if "api_version" not in patch_meta:
                    raise KeyError("api_version")
        except json.decoder.JSONDecodeError as err:
            msg = f"Could not parse patches from annotation [{patch_annotation}]"
            log.error(msg)
            raise RolloutError(msg) from err
        except dateutil.parser.ParserError as err:
            msg = f"Failed to parse patch timestamp [{patch_annotation}]"
            log.error(msg)
            raise RolloutError(msg) from err
        except KeyError as err:
            msg = f"Patch meta incorrectly formatted [{patch_annotation}]"
            log.error(msg)
            raise RolloutError(msg) from err

        # Fetch the state of each patch and add it to the output, sorted by
        # timestamp with the earliest first
        temporary_patches = []
        for patch_name, patch_meta in sorted(
            list(patches.items()), key=lambda x: x[1]["timestamp"]
        ):
            # Do the fetch
            log.debug2("Fetching patch [%s/%s]", patch_name, patch_meta["timestamp"])
            namespace = cr_manifest.get("metadata", {}).get("namespace")
            patch_api_version = patch_meta["api_version"]
            patch_kind = patch_meta.get("kind", "TemporaryPatch")
            success, content = deploy_manager.get_object_current_state(
                kind=patch_kind,
                name=patch_name,
                api_version=patch_api_version,
                namespace=namespace,
            )
            assert_cluster(success, f"Failed to fetch patch content for [{patch_name}]")
            assert_config(content is not None, f"Patch not found [{patch_name}]")

            # Pull the patch spec and add it to the list
            assert_config(
                content.get("spec") is not None,
                f"No spec found in patch [{patch_name}]",
            )
            temporary_patches.append(aconfig.Config(content, override_env_vars=False))

        return temporary_patches

    ## Status Details ############################################################

    def _update_resource_status(
        self, deploy_manager: DeployManagerBase, manifest: aconfig.Config, **kwargs
    ) -> dict:
        """Helper function to update the status of a resource given a deploy_manager, manifest
        and status kwargs

        Args:
            deploy_manager: DeployManagerBase
                The DeployManager used to update the resource
            manifest: aconfig.Config
                The manifest of the resource being updated
            **kwargs:
                The key word arguments passed to update_resource_status

        Returns:
            updated_status: dict
                The updated status applied to the resource
        """
        return status.update_resource_status(
            deploy_manager,
            manifest.kind,
            manifest.apiVersion,
            manifest.metadata.name,
            manifest.metadata.namespace,
            **kwargs,
        )

    def _update_reconcile_start_status(self, session: Session):
        """Update the status for a resource at the start of a reconciliation

        Args:
            session: Session
                The session of the reconcile which includes the DeployManager and resource

        """
        ready_condition = status.get_condition(status.READY_CONDITION, session.status)
        ready_reason = ready_condition.get("reason")
        if ready_reason is None or session.current_version is None:
            ready_reason = status.ReadyReason.INITIALIZING

        optional_kwargs = {}
        if session.current_version and session.version != session.current_version:
            log.debug(
                "Version change detected: %s -> %s",
                session.current_version,
                session.version,
            )
            optional_kwargs = {
                "updating_reason": status.UpdatingReason.VERSION_CHANGE,
                "updating_message": "Version Change Started: "
                f"[{session.current_version}] -> [{session.version}]",
            }
            ready_reason = status.ReadyReason.IN_PROGRESS

        self._update_resource_status(
            session.deploy_manager,
            session.cr_manifest,
            ready_reason=ready_reason,
            ready_message=ready_condition.get("message", "Initial Rollout Started"),
            supported_versions=config.supported_versions,
            **optional_kwargs,
        )

    def _update_reconcile_completion_status(
        self, session: Session, completion_state: CompletionState
    ):
        """Perform CR status updates based on the results of the rollout steps. The status logic is
        as follows:
          1. Initial Rollout: Ready-INITIALIZING, Updating-VERIFY_WAIT
          2. Everything complete: Ready-STABLE, Updating-STABLE
          3. Everything except after_verify: Ready-IN_PROGRESS, Updating-STABLE
          4. other: Updating-VERIFY_WAIT

          Args:
            session: Session
                The session of the reconcile which includes the DeployManager and resource
            completion_state: CompletionState
                The result of the rollout
        """
        status_update = {"component_state": completion_state}

        # If everything completed and verified, set ready and updating to STABLE
        # and set the status's reconciled version to the desired version
        if completion_state.verify_completed():
            status_update["ready_reason"] = status.ReadyReason.STABLE
            status_update["ready_message"] = "Verify Complete"
            status_update["updating_reason"] = status.UpdatingReason.STABLE
            status_update["updating_message"] = "Rollout Complete"
            status_update["version"] = session.version

        # If the completion_state didn't fail then update the ready condition with
        # in_progress and the updating condition with verification incomplete
        else:
            current_status = session.get_status()

            # If not initializing then update the ready condition with in_progress
            current_ready_cond = status.get_condition(
                status.READY_CONDITION, current_status
            )
            if (
                current_ready_cond.get("reason")
                != status.ReadyReason.INITIALIZING.value
            ):
                status_update["ready_reason"] = status.ReadyReason.IN_PROGRESS
                status_update["ready_message"] = "Verify InProgress"

            status_update["updating_reason"] = status.UpdatingReason.VERIFY_WAIT
            status_update["updating_message"] = "Component verification incomplete"

        log.debug3("Updating status after reconcile: %s", status_update)
        self._update_resource_status(
            session.deploy_manager, session.cr_manifest, **status_update
        )

    def _update_error_status(
        self, resource: Union[dict, aconfig.Config], error: Exception
    ) -> dict:
        """Update the status of a resource after an error occurred. This function
        setups up it's own deploy manager and parses the resource. This way errors at any
        phase of reconciliation can still get updated

        Args:
            resource: Union[dict, aconfig.Config]
                The resource that's status is being updated
            error: Exception
                The exception that stopped the reconciliation

        Returns:
            status: dict
                The updated status after the error message
        """
        cr_manifest = self.parse_manifest(resource)
        deploy_manager = self.setup_deploy_manager(resource)

        # Get the completion state if possible
        component_state = getattr(error, "completion_state", None)

        # Expected Oper8 Errors
        if isinstance(error, PreconditionError):
            status_update = {
                "updating_reason": status.UpdatingReason.PRECONDITION_WAIT,
                "updating_message": str(error),
                "component_state": component_state,
            }
        elif isinstance(error, (VerificationError, Oper8ExpectedError)):
            status_update = {
                "updating_reason": status.UpdatingReason.VERIFY_WAIT,
                "updating_message": str(error),
                "component_state": component_state,
            }
        elif isinstance(error, ConfigError):
            status_update = {
                "ready_reason": status.ReadyReason.CONFIG_ERROR,
                "ready_message": str(error),
                "updating_reason": status.UpdatingReason.ERRORED,
                "updating_message": str(error),
                "component_state": component_state,
            }
        elif isinstance(error, ClusterError):
            status_update = {
                "updating_reason": status.UpdatingReason.CLUSTER_ERROR,
                "updating_message": str(error),
                "component_state": component_state,
            }

        elif isinstance(error, (RolloutError, Oper8FatalError)):
            status_update = {
                "ready_reason": status.ReadyReason.ERRORED,
                "ready_message": str(error),
                "updating_reason": status.UpdatingReason.ERRORED,
                "updating_message": str(error),
                "component_state": component_state,
            }

        # Catchall for non oper8 errors
        else:
            status_update = {
                "ready_reason": status.ReadyReason.ERRORED,
                "ready_message": str(error),
                "updating_reason": status.UpdatingReason.ERRORED,
                "updating_message": str(error),
            }

        return self._update_resource_status(
            deploy_manager, cr_manifest, **status_update
        )
