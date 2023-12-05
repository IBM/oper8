"""
The Controller class manages a collection of Components and associates them with
a CustomResource in the cluster.
"""

# Standard
from typing import Optional, Tuple, Union
import abc

# First Party
import aconfig
import alog

# Local
from .dag import CompletionState
from .exceptions import Oper8Error, RolloutError
from .reconcile import RequeueParams
from .rollout_manager import RolloutManager
from .session import Session
from .utils import abstractclassproperty, classproperty
from .verify_resources import verify_subsystem

## Globals #####################################################################

log = alog.use_channel("CTRLR")


## Application #################################################################


class Controller(abc.ABC):
    """This class represents a controller for a single kubernetes custom
    resource kind. Its primary functionality is to perform a reconciliation of a
    given CR manifest for an instance of the resource kind against the current
    state of the cluster. To accomplish this, its reconciliation logic is:

    1. Construct a Directed Acyclic Graph of all Components that this kind
        needs to manage.
    2. Execute the Graph in dependency order where each node of the graph first
        renders the manifests for all kubernetes resources managed by the
        Component, then applies them to the cluster.
    3. Execute a secondary Graph with verification logic for each Component,
        terminating verification for downstream nodes if any node is not yet
        verified.

    To do this, the main operations of the class are to construct a DAG of
    Components, then walk them through the primary lifecycle phases:

    1. Run the Component's deploy() function to completion and verify that the
        actual deployment operations succeeded
    2. Run the Component's verify() function to run component-specific tests
        that will verify if the deployment is rolled out in a successful state
    """

    ## Class Properties ########################################################

    # Derived classes must have class properties for group, version, and kind.
    # To enforce this, we set defaults for all of these and then validate that
    # they are present, we define them as classproperty and raise when accessed
    # from the base implementation.

    # NOTE: pylint is very confused by the use of these property decorators, so
    #   we need to liberally ignore warnings.

    @abstractclassproperty  # noqa: B027
    def group(cls) -> str:
        """The apiVersion group for the resource this controller manages"""

    @abstractclassproperty  # noqa: B027
    def version(cls) -> str:
        """The apiVersion version for the resource this controller manages"""

    @abstractclassproperty  # noqa: B027
    def kind(cls) -> str:
        """The kind for the resource this controller manages"""

    @classproperty
    def finalizer(cls) -> Optional[str]:  # pylint: disable=no-self-argument
        """The finalizer used by this Controller"""
        if cls.has_finalizer:  # pylint: disable=using-constant-test
            return f"finalizers.{cls.kind.lower()}.{cls.group}"  # pylint: disable=no-member
        return None

    @classproperty
    def has_finalizer(cls) -> bool:  # pylint: disable=no-self-argument
        """If the derived class has an implementation of finalize_components, it
        has a finalizer and can be registered for finalize events
        """
        return cls.finalize_components is not Controller.finalize_components

    ## Construction ############################################################

    def __init__(self, config_defaults: Optional[aconfig.Config] = None):
        """The constructor sets up all of the properties of the controller which
        are constant across reconciliations.

        Args:
            config_defaults:  Optional[aconfig.Config]
                Default values for the backend controller config

        """
        # Make sure the class properties are present and not empty
        assert self.group, "Controller.group must be a non-empty string"
        assert self.version, "Controller.version must be a non-empty string"
        assert self.kind, "Controller.kind must be a non-empty string"
        self.config_defaults = config_defaults or aconfig.Config({})

    @classmethod
    def __str__(cls):
        """Stringify with the GVK"""
        return f"Controller({cls.group}/{cls.version}/{cls.kind})"

    ## Abstract Interface ######################################################
    #
    # These functions must be implemented by child classes
    ##

    @abc.abstractmethod
    def setup_components(self, session: Session):
        """Given the session for an individual reconciliation, construct the set
        of Components that will be deployed.

        Error Semantics: Child classes should throw ConfigError if config is
        not valid and include the portion of config where the problem occurred.

        Args:
            session:  Session
                The current session containing the per-event configs
        """

    ## Base Class Interface ####################################################
    #
    # These methods MAY be implemented by children, but contain default
    # implementations that are appropriate for simple cases.
    #
    # NOTE: We liberally use pylint disables here to make the base interface
    #   clear to deriving classes.
    ##

    def finalize_components(self, session: Session):  # noqa: B027
        """When performing a finalizer operation, this function will be called
        to perform custom finalizer logic for this Controller.

        Error Semantics: Child classes should throw ConfigError if config is
        not valid and include the portion of config where the problem occurred.

        NOTE: This method is not abstract since the standard controller usecase
            does not require finalizing

        Args:
            session:  Session
                The current session containing the per-event configs
        """

    def after_deploy(self, session: Session) -> bool:
        """This allows children to inject logic that will run when the
        controller has finished deploying all components, but not necessarily
        verifying all of them. The default behavior is a no-op.

        Args:
            session:  Session
                The current reconciliation session

        Returns:
            success:  bool
                True if custom hook code executed successfully and lifecycle
                should continue
        """
        return True

    def after_verify(
        self,
        session: Session,  # pylint: disable=unused-argument
    ) -> bool:
        """This allows children to inject logic that will run when the
        controller has finished verifying all components. The default behavior
        is a no-op.

        Args:
            session:  Session
                The current reconciliation session

        Returns:
            success:  bool
                True if custom hook code executed successfully and lifecycle
                should continue
        """
        return True

    def should_requeue(self, session: Session) -> Tuple[bool, Optional[RequeueParams]]:
        """should_requeue determines if current reconcile request should be re-queued.

        Children can override default implementation to provide custom logic.
        Default implementation re-queues the request if the reconciling CR status
        hasn't been reached stable state.

        Args:
            session: Session
                The current reconciliation session

        Returns:
            requeue: bool
                True if the reconciliation request should be re-queued
            config: RequeueParams
                 Parameters of requeue request. Can be None if requeue is False.
        """
        api_version = session.api_version
        kind = session.kind
        name = session.name
        namespace = session.namespace
        requeue_params = RequeueParams()
        # Fetch the current status from the cluster
        success, current_state = session.deploy_manager.get_object_current_state(
            api_version=api_version,
            kind=kind,
            name=name,
            namespace=namespace,
        )
        if not success:
            log.warning(
                "Failed to fetch current state for %s/%s/%s", namespace, kind, name
            )
            return True, requeue_params
        # Do not requeue if resource was deleted
        if not current_state:
            log.warning("Resource not found: %s/%s/%s", namespace, kind, name)
            return False, requeue_params

        log.debug3("Current CR manifest for requeue check: %s", current_state)

        verified = verify_subsystem(current_state, session.version)
        return not verified, requeue_params

    def get_cr_manifest_defaults(
        self,
    ) -> Union[dict, aconfig.Config]:
        """This allows children to provide default values for their cr_manifest
        that will be injected where no override is provided in the user-provided
        cr_manifest.

        Returns:
            cr_manifest_defaults:  Union[dict, aconfig.Config]
                The cr defaults. Raw dicts will be converted to Config objects.
        """
        return aconfig.Config({})

    def get_config_defaults(self):
        """This function allows children to override the default values for the session
        config. This value can also be set via the controllers __init__ function.
        """
        return self.config_defaults

    ## Public Interface ########################################################
    #
    # These functions should be used by the reconciliation manager or in
    # tests
    ##

    def run_reconcile(
        self, session: Session, is_finalizer: bool = False
    ) -> CompletionState:
        """Perform a reconciliation iteration for this controller on given a session.
        This function should only be called once per session. The general logic for a
        controller reconcile is as follows:

        1. Set up the set of Components and their dependencies that will be
            managed in this reconciliation based on the CR and config
        2. Invoke the rollout to render each component and apply it to the
            cluster (if not in dry-run), then verify the DAG of components

        Args:
            session:  Session
                The full structured content of the CR manifest for this operand
            is_finalizer:  bool
                If true, the logic in finalize_components is run, otherwise the
                logic in setup_components is called

        Returns:
            result: ReconciliationResult
                The result of reconcile
        """
        # Check if session has already been reconciled
        if not session.graph.empty():
            raise RolloutError("Session has already been reconciled")

        self._manage_components(session, is_finalizer)
        completion_state = self._rollout_components(session)
        return completion_state

    ## Implementation Details ##################################################

    def _manage_components(self, session: Session, is_finalizer: bool):
        """Delegate logic to child's finalize_components or setup_components

        Args:
            session: Session
                The current session being reconciled
            is_finalizer: bool
                Weather the current CR is being deleted

        """

        # If this is a finalizer, run finalize_components
        if is_finalizer:
            log.debug("[%s] Running as finalizer", session.id)
            self.finalize_components(session)

        # Otherwise run setup_components
        else:
            self.setup_components(session)

    @alog.logged_function(log.debug)
    def _rollout_components(self, session: Session):
        """Deploy all dependent components according to the configured
        dependencies between them
        """
        log.debug("Rolling out %s", str(self))

        # Set up the deployment manager and run the rollout
        rollout_manager = RolloutManager(
            session=session,
            after_deploy=self.after_deploy,
            after_verify=self.after_verify,
        )
        completion_state = rollout_manager.rollout()
        rollout_failed = completion_state.failed()
        log.info("Final rollout state: %s", completion_state)

        # Get Rollout Status
        deploy_completed = completion_state.deploy_completed()
        verify_completed = completion_state.verify_completed()
        log.debug2(
            "Deploy Completed: %s, Verify Completed: %s, Deploy Failed: %s",
            deploy_completed,
            verify_completed,
            rollout_failed,
        )

        # If an oper8 error occurred in the rollout, decorate it with a reference
        # to the completion state itself and then raise it to be handled by the
        # top-level ReconcileManager handler.
        if isinstance(completion_state.exception, Oper8Error):
            log.debug("Handling Oper8Error from rollout")
            completion_state.exception.completion_state = completion_state
            raise completion_state.exception

        # If the deploy failed but didn't trigger an Oper8Error, we'll make one
        # ourselves
        if rollout_failed:
            raise RolloutError(
                "Deploy phase failed", completion_state=completion_state
            ) from completion_state.exception

        return completion_state
