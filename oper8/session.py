"""
This module holds the core session state for an individual reconciliation
"""

# Standard
from functools import partial
from typing import Callable, List, Optional, Tuple, Union
import hashlib

# First Party
import aconfig
import alog

# Local
from .dag import Graph
from .deploy_manager import DeployManagerBase
from .exceptions import assert_cluster
from .status import get_version
from .utils import get_manifest_version

log = alog.use_channel("SESSION")

HASH_CHARSET = "0123456789abcdef"


# Maximum length for a kubernetes name
MAX_NAME_LEN = 63

# Type definition for the signature of a component verify function
# NOTE: I'm not sure why pylint dislikes this name. In my view, this is a shared
#   global which should have all-caps casing.
COMPONENT_VERIFY_FUNCTION = Callable[["Session"], bool]  # pylint: disable=invalid-name

# Helper Definition to define when a session should use its own namespace
# or the one passed in as an argument
_SESSION_NAMESPACE = "__SESSION_NAMESPACE__"

# Forward declaration for Component
COMPONENT_INSTANCE_TYPE = "Component"


class Session:  # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """A session is the core context manager for the state of an in-progress
    reconciliation
    """

    # We strictly define the set of attributes that a Session can have to
    # disallow arbitrary assignment
    __slots__ = [
        "__components",
        "__component_dependencies",
        "__enabled_components",
        "__disabled_components",
        "__id",
        "__cr_manifest",
        "__config",
        "__temporary_patches",
        "__deploy_manager",
        "__status",
        "__current_version",
        "__graph",
        # _app is retained for backwards compatibility
        "_app",
    ]

    def __init__(  # pylint: disable=too-many-arguments
        self,
        reconciliation_id: str,
        cr_manifest: aconfig.Config,
        config: aconfig.Config,
        deploy_manager: DeployManagerBase,
        temporary_patches: Optional[List[dict]] = None,
    ):
        """Construct a session object to hold the state for a reconciliation

        Args:
            reconciliation_id:  str
                The unique ID for this reconciliation
            cr_manifest:  aconfig.Config
                The full value of the CR mainfest that triggered this
                reconciliation
            config:  aconfig.Config
                The compiled backend config for this reconciliation
            deploy_manager:  DeployManagerBase
                The preconfigured DeployManager in charge of running the actual
                deploy operations for this deployment
            temporary_patches:  list(dict)
                List of temporary patch object to apply to resources managed by
                this rollout
        """

        ##################################################################
        # Private Members: These members will be hidden from client code #
        ##################################################################

        # Mapping from component name to Component instance
        self.__graph = Graph()

        ###################################################
        # Properties: These properties will be exposed as #
        # @property members to be used by client code     #
        ###################################################

        self.__id = reconciliation_id
        if not isinstance(cr_manifest, aconfig.Config):
            cr_manifest = aconfig.Config(cr_manifest, override_env_vars=False)
        self._validate_cr(cr_manifest)
        self.__cr_manifest = cr_manifest
        if not isinstance(config, aconfig.Config):
            config = aconfig.Config(config, override_env_vars=False)
        self.__config = config
        self.__temporary_patches = temporary_patches or []

        # The deploy manager that will be used to manage interactions with the
        # cluster
        self.__deploy_manager = deploy_manager

        # Get the current status and version so that it can be referenced by the
        # Application and Components that use it
        self.__status = self.get_status()
        self.__current_version = get_version(self.status)

    ## Properties ##############################################################

    @property
    def id(self) -> str:  # pylint: disable=invalid-name
        """The unique reconciliation ID"""
        return self.__id

    @property
    def cr_manifest(self) -> aconfig.Config:
        """The full CR manifest that triggered this reconciliation"""
        return self.__cr_manifest

    @property
    def spec(self) -> aconfig.Config:
        """The spec section of the CR manifest"""
        return self.cr_manifest.get("spec", aconfig.Config({}))

    @property
    def version(self) -> str:
        """The spec.version for this CR"""
        return get_manifest_version(self.cr_manifest)

    @property
    def metadata(self) -> aconfig.Config:
        """The metadata for this CR"""
        return self.cr_manifest.metadata

    @property
    def kind(self) -> str:
        """The kind of the operand for this CR"""
        return self.cr_manifest.kind

    @property
    def api_version(self) -> str:
        """The api version of the operand for this CR"""
        return self.cr_manifest.apiVersion

    @property
    def name(self) -> str:
        """The metadata.name for this CR"""
        return self.metadata.name

    @property
    def namespace(self) -> str:
        """The metadata.namespace for this CR"""
        return self.metadata.namespace

    @property
    def finalizers(self) -> str:
        """The metadata.namespace for this CR"""

        # Manually create finalizer list if it doesn't exist so its
        # editable
        if "finalizers" not in self.metadata:
            self.metadata["finalizers"] = []

        return self.metadata.get("finalizers")

    @property
    def config(self) -> aconfig.Config:
        """The backend config for this reconciliation"""
        return self.__config

    @property
    def temporary_patches(self) -> List[aconfig.Config]:
        """Ordered list of temporary patches that apply to the operand being
        reconciled
        """
        return self.__temporary_patches

    @property
    def status(self) -> aconfig.Config:
        """The operand status"""
        return self.__status

    @property
    def current_version(self) -> aconfig.Config:
        """The most recently reconciled version of the operand"""
        return self.__current_version

    @property
    def deploy_manager(self) -> DeployManagerBase:
        """Allow read access to the deploy manager"""
        return self.__deploy_manager

    @property
    def graph(self) -> str:  # pylint: disable=invalid-name
        """The component graph"""
        return self.__graph

    ## State Management ########################################################
    #
    # These functions are used by derived controllers in their setup_components
    # implementations
    ##

    @alog.logged_function(log.debug2)
    def add_component(self, component: COMPONENT_INSTANCE_TYPE):
        """Add a component to this deploy associated with a specfic application

        Args:
            component:  Component
                The component to add to this deploy
            disabled:  bool
                Whether or not the component is disabled in this deploy
        """
        self.graph.add_node(component)

    def add_component_dependency(
        self,
        component: Union[str, COMPONENT_INSTANCE_TYPE],
        upstream_component: Union[str, COMPONENT_INSTANCE_TYPE],
        verify_function: Optional[COMPONENT_VERIFY_FUNCTION] = None,
    ):
        """Add a dependency indicating that one component requires an upstream
        component to be deployed before it can be deployed.

        Args:
            component:  str or Component
                The component or name of component in the deploy that must wait for the upstream
            upstream_component:  str or Component
                The upstream component or name of upstream that must be deployed before component
            verify_function:  callable
                A callable function of the form `def verify(session) -> bool:`
                to use to verify that the dependency has been satisified. This
                will be used to block deployment of the component beyond
                requiring that the upstream has been deployed successfully.
        """
        # Get component obj if name was provided
        component_node = component
        if isinstance(component, str):
            component_node = self.get_component(component)

        upstream_component_node = upstream_component
        if isinstance(upstream_component, str):
            upstream_component_node = self.get_component(upstream_component)

        if not component_node or not upstream_component_node:
            raise ValueError(
                f"Cannot add dependency [{component} -> {upstream_component}]",
                " for unknown component(s)",
            )

        if component_node.disabled or upstream_component_node.disabled:
            raise ValueError(
                f"Cannot add dependency [{component} -> {upstream_component}]",
                " for with disabled component(s)",
            )

        # Add session parameter to verify function if one was provided
        if verify_function:
            verify_function = partial(verify_function, self)
        self.graph.add_node_dependency(
            component_node, upstream_component_node, verify_function
        )

    ## Utilities ###############################################################
    #
    # These utilities may be used anywhere in client code to perform common
    # operations based on the state of the session.
    ##
    def get_component(
        self, name: str, disabled: Optional[bool] = None
    ) -> Optional[COMPONENT_INSTANCE_TYPE]:
        """Get an individual component by name

        Args:
            name: str
                Name of component to return
            disabled: Optional[bool]
                Option on wether to return disabled components. If this option is not supplied then
                the referenced component will be returned irregardless whether its disabled
                or enabled

        Returns:
            component: Optional[Component]
                The component with the given name or None if component does not exit or does
                not match disabled arg
        """
        comp = self.graph.get_node(name)

        # Only filter disabled/enabled components if the option was passed in.
        if isinstance(disabled, bool):
            if disabled:
                return comp if comp.disabled else None
            return comp if not comp.disabled else None

        return comp

    def get_components(self, disabled: bool = False) -> List[COMPONENT_INSTANCE_TYPE]:
        """Get all components associated with an application

        Args:
            disabled:  bool
                Whether to return disabled or enabled components

        Returns:
            components:  list(Component)
                The list of Component objects associated with the given
                application
        """
        assert isinstance(
            disabled, bool
        ), "Disabled flag must be a bool. You may be using the old function signature!"

        # Get list of all components.
        comp_list = self.graph.get_all_nodes()

        # Filter out disabled/enabled components using get_component
        filtered_list = [
            comp for comp in comp_list if self.get_component(comp.get_name(), disabled)
        ]

        return filtered_list

    def get_component_dependencies(
        self,
        component: Union[str, COMPONENT_INSTANCE_TYPE],
    ) -> List[Tuple[COMPONENT_INSTANCE_TYPE, Optional[COMPONENT_VERIFY_FUNCTION]]]:
        """Get the list of (upstream_name, verify_function) tuples for a given
        component.

        NOTE: This is primarily for use inside of the RolloutManager. Do not use
            this method in user code unless you know what you're doing!

        Args:
            component_name:  str
                The name of the component to lookup dependencies for

        Returns:
            upstreams:  List[Tuple[str, Optional[VERIFY_FUNCTION]]]
                The list of upstream (name, verify_fn) pairs
        """
        component_node = component
        if isinstance(component, str):
            component_node = self.get_component(component)

        return component_node.get_children()

    def get_scoped_name(self, name: str) -> str:
        """Get a name that is scoped to the application instance

        Args:
            name:  str
                The name of a resource that will be managed by this operator
                which should have instance name scoping applied

        Returns:
            scoped_name:  str
                The scoped and truncated version of the input name
        """
        scoped_name = self.get_truncated_name(f"{self.name}-{name}")
        log.debug3("Scoped name [%s] -> [%s]", name, scoped_name)
        return scoped_name

    @staticmethod
    def get_truncated_name(name: str) -> str:
        """Perform truncation on a cluster name to make it conform to kubernetes
        limits while remaining unique.

        Args:
            name:  str
                The name of the resource that should be truncated and made
                unique

        Returns:
            truncated_name:  str
                A version of name that has been truncated and made unique
        """
        if len(name) > MAX_NAME_LEN:
            sha = hashlib.sha256()
            sha.update(name.encode("utf-8"))
            trunc_name = name[: MAX_NAME_LEN - 4] + sha.hexdigest()[:4]
            log.debug2("Truncated name [%s] -> [%s]", name, trunc_name)
            name = trunc_name
        return name

    def get_object_current_state(
        self,
        kind: str,
        name: str,
        api_version: Optional[str] = None,
        namespace: Optional[str] = _SESSION_NAMESPACE,
    ) -> Tuple[bool, Optional[dict]]:
        """Get the current state of the given object in the namespace of this
        session

        Args:
            kind:  str
                The kind of the object to fetch
            name:  str
                The full name of the object to fetch
            api_version:  str
                The api_version of the resource kind to fetch

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            current_state:  dict or None
                The dict representation of the current object's configuration,
                or None if not present
        """
        namespace = namespace if namespace != _SESSION_NAMESPACE else self.namespace
        return self.deploy_manager.get_object_current_state(
            kind=kind,
            name=name,
            namespace=namespace,
            api_version=api_version,
        )

    def filter_objects_current_state(  # pylint: disable=too-many-arguments
        self,
        kind: str,
        api_version: Optional[str] = None,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
        namespace: Optional[str] = _SESSION_NAMESPACE,
    ) -> Tuple[bool, List[dict]]:
        """Get the current state of the given object in the namespace of this
        session

        Args:
            kind:  str
                The kind of the object to fetch
            label_selector:  str
                The label selector to filter the results by
            field_selector:  str
                The field selector to filter the results by
            api_version:  str
                The api_version of the resource kind to fetch

        Returns:
            success:  bool
                Whether or not the state fetch operation succeeded
            current_state:  List[Dict]
                The list of resources in dict representation,
                or [] if none match
        """
        namespace = namespace if namespace != _SESSION_NAMESPACE else self.namespace
        return self.deploy_manager.filter_objects_current_state(
            kind=kind,
            namespace=namespace,
            api_version=api_version,
            label_selector=label_selector,
            field_selector=field_selector,
        )

    @alog.logged_function(log.debug2)
    @alog.timed_function(log.debug2)
    def get_status(self) -> dict:
        """Get the status of the resource being managed by this session or an
        empty dict if not available

        Returns:
            current_status:  dict
                The dict representation of the status subresource for the CR
                being managed by this session
        """

        # Pull the kind, name, and namespace
        kind = self.cr_manifest.get("kind")
        name = self.name
        api_version = self.api_version
        log.debug3("Getting status for %s.%s/%s", api_version, kind, name)

        # Fetch the current status
        success, content = self.get_object_current_state(
            kind=kind,
            name=name,
            api_version=api_version,
        )
        assert_cluster(
            success, f"Failed to fetch status for [{api_version}/{kind}/{name}]"
        )
        if content:
            return content.get("status", {})
        return {}

    ## Implementation Details ##################################################

    @staticmethod
    def _validate_cr(cr_manifest: aconfig.Config):
        """Ensure that all expected elements of the CR are present. Expected
        elements are those that are guaranteed to be present by the kube API.
        """
        assert "kind" in cr_manifest, "CR missing required section ['kind']"
        assert "apiVersion" in cr_manifest, "CR missing required section ['apiVersion']"
        assert "metadata" in cr_manifest, "CR missing required section ['metadata']"
        assert (
            "name" in cr_manifest.metadata
        ), "CR missing required section ['metadata.name']"
        assert (
            "namespace" in cr_manifest.metadata
        ), "CR missing required section ['metadata.namespace']"
