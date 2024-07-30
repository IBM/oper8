"""
Component base class for building larger abstractions off of
"""

# Standard
from typing import Any, Callable, List, Optional, Tuple
import abc
import os

# Third Party
import yaml

# First Party
import aconfig
import alog

# Local
from . import config
from .constants import INTERNAL_NAME_ANOTATION_NAME, TEMPORARY_PATCHES_ANNOTATION_NAME
from .dag import Graph, Node, ResourceNode
from .deploy_manager import DeployMethod
from .exceptions import assert_cluster
from .managed_object import ManagedObject
from .patch import apply_patches
from .session import COMPONENT_VERIFY_FUNCTION, Session
from .utils import abstractclassproperty, sanitize_for_serialization
from .verify_resources import RESOURCE_VERIFY_FUNCTION, verify_resource

log = alog.use_channel("COMP-BASE")


class Component(Node, abc.ABC):
    """
    This file defines the top-level interface for a "Component" in the
    deployment ecosystem. Each Component will ultimately resolve to a Node in
    the deployment execution graph which can be atomically rendered, deployed,
    verified, and if needed reverted.
    """

    @abstractclassproperty
    def name(self):
        """All Components must implement a name class attribute"""

    def __init__(
        self,
        session: Session,
        disabled: bool = False,
    ):
        """Construct with the session for this deployment

        Args:
            session:  Session
                The session that this component will belong to
            disabled:  bool
                Whether or not this component is disabled
        """
        # Ensure that the name property is defined by accessing it and that
        # namespace is inherited from session.
        self.name  # noqa: B018
        self.session_namespace = session.namespace
        self.disabled = disabled

        # Initialize Node with name
        super().__init__(self.name)

        # Register with the session
        # NOTE: This is done before the parent initialization so duplicates can
        #   be caught by the session with a nice error rather than Graph
        log.debug2("[%s] Auto-registering %s", session.id, self)
        session.add_component(self)

        # Initialize the Graph that'll control component rendering
        self.graph = Graph()

        # The list of all managed objects owned by this component
        self._managed_objects = None

    def __str__(self):
        return f"Component({self.name})"

    @property
    def managed_objects(self) -> List[ManagedObject]:
        """The list of managed objects that this Component currently knows
        about. If called before rending, this will be an empty list, so it will
        always be iterable.

        Returns:
            managed_objects:  List[ManagedObject]
                The list of known managed objects
        """
        return self._managed_objects or []

    ## Base Class Interface ####################################################
    #
    # These methods MAY be implemented by children, but contain default
    # implementations that are appropriate for simple cases.
    #
    # NOTE: We liberally use pylint disables here to make the base interface
    #   clear to deriving classes.
    ##

    def build_chart(self, session: Session):  # pylint: disable=unused-argument
        """The build_chart function allows the derived class to add child Charts
        lazily so that they can take advantage of post-initialization
        information.

        Args:
            session:  Session
                The current deploy session
        """

    def verify(self, session):
        """The verify function will run any necessary testing and validation
        that the component needs to ensure that rollout was successfully
        completed.

        Args:
            session:  Session
                The current reconciliation session

        Returns:
            success:  bool
                True on successful deployment verification, False on failure
                conditions
        """
        return self._default_verify(session, is_subsystem=False)

    @alog.logged_function(log.debug2)
    @alog.timed_function(log.debug2)
    def render_chart(self, session):
        """This will be invoked by the parent Application to build and render
        the individual component's chart

        Args:
            session:  Session
                The session for this reconciliation
        """

        # Do the rendering
        self.__render(session)

        # If a working directory is configured, use it
        if config.working_dir:
            rendered_file = self.to_file(session)
            log.debug("Rendered %s to %s", self, rendered_file)

    def update_object_definition(
        self,
        session: Session,  # pylint: disable=unused-argument
        internal_name: str,  # pylint: disable=unused-argument
        resource_definition: dict,
    ):
        """Allow children to inject arbitrary object mutation logic for
        individual managed objects

        The base implementation simply returns the given definition as a
        passthrough

        Args:
            session:  Session
                The session for this reconciliation
            internal_name:  str
                The internal name of the object to update
            resource_definition:  dict
                The dict representation of the resource to modify

        Returns:
            resource_definition:  dict
                The dict representation of the resource with any modifications
                applied
        """
        return resource_definition

    @alog.logged_function(log.debug2)
    @alog.timed_function(log.debug2)
    def deploy(self, session):
        """Deploy the component

        Args:
            session:  Session
                The current reconciliation session

        Returns:
            success:  bool
                True on successful application of the kub state (not
                programmatic verification), False otherwise
        """
        assert (
            self._managed_objects is not None
        ), "Cannot call deploy() before render_chart()"

        # Deploy all managed objects
        for obj in self.managed_objects:
            success, _ = session.deploy_manager.deploy(
                resource_definitions=[obj.definition],
                method=obj.deploy_method,
            )
            if not success:
                log.warning("Failed to deploy [%s]", self)
                return False
        return True

    def disable(self, session):
        """Disable the component

        Args:
            session:  Session
                The current reconciliation session

        Returns:
            success:  bool
                True on successful application of the kub state (not
                programmatic verification), False otherwise
        """
        assert (
            self._managed_objects is not None
        ), "Cannot call disable() before render_chart()"

        # Disable all managed objects
        success, _ = session.deploy_manager.disable(
            [obj.definition for obj in self._managed_objects]
        )
        if not success:
            log.warning("Failed to disable [%s]", self)
            return False
        return True

    ## Resource Interface ####################################################
    #
    # These methods offer functionality that children can use to add resources to
    # a components graph
    ##

    def add_resource(
        self,
        name: str,  # pylint: disable=redefined-builtin
        obj: Any,
        verify_function: Optional[RESOURCE_VERIFY_FUNCTION] = None,
        deploy_method: Optional[DeployMethod] = DeployMethod.DEFAULT,
    ) -> Optional[
        ResourceNode
    ]:  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        """The add_resource function allows the derived class to add resources
        to this component to later be rendered

        Args:
            name:  str
                The name of the resource in the Graph
            obj: Any
                An object or dict which can be manipulated into a dict
                representation of the kubernetes resource
        """
        # Sanitize object to enable native support for openapi objects
        obj = sanitize_for_serialization(obj)

        # Add namespace to obj if not present
        obj.setdefault("metadata", {}).setdefault("namespace", self.session_namespace)

        node = ResourceNode(name, obj, verify_function, deploy_method)
        self.graph.add_node(node)
        return node

    def add_dependency(
        self,
        session: Session,
        *components: "Component",
        verify_function: Optional[COMPONENT_VERIFY_FUNCTION] = None,
    ):
        """This add_dependency function sets up a dependency between this component
        and a list of other components. To add a dependency between resources inside
        this component use resource.add_dependency
        Args:
            session:  Session
                The current resource session
            *components:  Components
                Any number of components to be added as a dependency
            verify_function: Optional[verify_function]
                An Optional callable function of the form `def verify(session) -> bool:`
                to use to verify that the dependency has been satisfied. This
                will be used to block deployment of the component beyond
                requiring that the upstream has been deployed successfully.
        """
        for component in components:
            session.add_component_dependency(self, component, verify_function)

    ## Base Class Utilities ####################################################
    #
    # These methods offer shared functionality that children can (and should)
    # use in their implementations
    ##

    @alog.logged_function(log.debug2)
    def to_dict(self, session):
        """
        Render the component and return it as a Dictionary, mainly useful for testing
        :return: Dictionary of the rendered component
        """
        self.__render(session)
        return [obj.definition for obj in self.managed_objects]

    def to_config(self, session):
        """
        Render the component and return it as an AttrDict, mainly useful for testing
        :return: AttrDict of the rendered component
        """

        return [
            aconfig.Config(obj, override_env_vars=False)
            for obj in self.to_dict(session)
        ]

    def to_file(self, session):
        """
        Render the component to disk and return the rendered file path
        :return: str path to rendered file
        """
        assert config.working_dir is not None, "Config must have a working_dir set"

        # If disabled and not dumping disabled components, nothing to do
        if self.disabled and not config.dump_disabled:
            log.debug("Not dumping disabled component: %s", self)
            return None

        # Get the in-memory representation
        objects = self.to_dict(session)

        # Get the output file name and make sure the directory structure exists
        path_parts = [
            config.working_dir,
            ".".join([session.api_version.replace("/", "."), session.kind]).lower(),
            session.name,
        ]
        if self.disabled:
            path_parts.append("DISABLED")
        path_parts.append(self.name)
        output_dir = os.path.join(*path_parts)
        if not os.path.exists(output_dir):
            log.debug2("Creating output dir: %s", output_dir)
            os.makedirs(output_dir)

        # Serialize to a yaml file
        instance_name = session.name
        output_file = os.path.join(output_dir, f"{instance_name}-{self.name}.k8s.yaml")
        log.debug2("Saving %s to %s", self, output_file)
        with open(output_file, "w", encoding="utf-8") as outfile:
            outfile.write("---\n" + yaml.safe_dump_all(objects))

        return output_file

    ## Base Class Implementation Details #######################################
    #
    # These methods provide shared functionality to the base class function
    # implementations and should not be used directly by children
    ##

    @classmethod
    def get_name(cls):  # pylint: disable=arguments-differ
        """Override get_name to support class attribute"""
        return cls.name

    def _default_verify(self, session, is_subsystem=False):
        """The verify function will run any necessary testing and validation
        that the component needs to ensure that rollout was successfully
        completed.

        Args:
            session:  Session
                The current reconciliation session

        Returns:
            success:  bool
                True on successful deployment verification, False on failure
                conditions
        """
        log.debug2("Using default verification for [%s]", self)

        # If this is in dry run mode, we skip verification since this relies on
        # checking for changes in the cluster which won't ever happen
        if config.dry_run:
            log.debug2("No verification to perform in dry_run")
            return True

        # Verify all managed resources
        for resource in self.managed_objects:
            log.debug2("Verifying [%s/%s]", resource.kind, resource.name)
            if not verify_resource(
                kind=resource.kind,
                name=resource.name,
                api_version=resource.api_version,
                session=session,
                is_subsystem=is_subsystem,
                namespace=resource.namespace,
                verify_function=resource.verify_function,
            ):
                log.debug("[%s/%s] not verified", resource.kind, resource.name)
                return False
        log.debug("All managed resources verified for [%s]", self)
        return True

    @staticmethod
    def _preserve_patch_annotation(session, internal_name, resource_definition):
        """This implementation helper checks the current state of the given
        resource and patches the desired state to preserve any temporary patch
        annotations found. This is done so that temporary patches can be applied
        to subsystem CRs managed by a top-level controller.
        """

        # Get the current state of the object
        kind = resource_definition.get("kind")
        api_version = resource_definition.get("apiVersion")
        metadata = resource_definition.get("metadata", {})
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        assert (
            kind is not None and api_version is not None and name is not None
        ), f"Resource {internal_name} missing critical metadata!"
        success, content = session.get_object_current_state(
            kind=kind, name=name, api_version=api_version, namespace=namespace
        )
        assert_cluster(
            success,
            f"Failed to look for current state for [{kind}/{api_version}/{namespace}/{name}]",
        )

        # Look for existing patch annotations
        if content is not None:
            content_meta = content.get("metadata", {})
            patch_anno = content_meta.get("annotations", {}).get(
                TEMPORARY_PATCHES_ANNOTATION_NAME
            )

            # If found, update the resource
            if patch_anno:
                resource_definition.setdefault("metadata", {}).setdefault(
                    "annotations", {}
                )[TEMPORARY_PATCHES_ANNOTATION_NAME] = patch_anno

            # Any time we have metadata changes, we need to include the
            # resourceVersion. It can't hurt to do so, so we will just always do
            # it here if found.
            resource_version = content_meta.get("resourceVersion")
            if resource_version is not None:
                resource_definition["metadata"]["resourceVersion"] = resource_version

            # Make sure any ownerReferences are persisted as well
            owner_refs = content_meta.get("ownerReferences")
            if owner_refs:
                resource_definition["metadata"]["ownerReferences"] = owner_refs

        return resource_definition

    def __build_lazy_charts(self, session):
        """Delegate to the child implementation of build_chart for lazy chart
        construction.
        """
        self.build_chart(session)

    @alog.logged_function(log.debug3)
    def __render(self, session):
        """This is the primary implementation for rendering objects into
        self.managed_objects
        """

        # Short-circuit if already rendered
        if self._managed_objects is not None:
            log.debug2(
                "%s returning %d pre-rendered objects", self, len(self._managed_objects)
            )
            return self.managed_objects

        # Generate name and dict representation of objects
        resource_list = self.__gather_resources(session)

        # Iterate all ApiObject children in dependency order and perform the
        # rendering, including patches and backend modifications.
        self._managed_objects = []
        for name, obj, verify_func, deploy_method in resource_list:
            # Apply any patches to this object
            log.debug2("Applying patches to managed object: %s", name)
            log.debug4("Before Patching: %s", obj)
            obj = apply_patches(name, obj, session.temporary_patches)

            # Make sure any temporary patch annotations that exist already
            # on this resource in the cluster are preserved
            log.debug2("Checking for existing subsystem patches on: %s", name)
            obj = self._preserve_patch_annotation(session, name, obj)

            # Add the internal name annotation if enabled
            if config.internal_name_annotation:
                log.debug2(
                    "Adding internal name annotation [%s: %s]",
                    INTERNAL_NAME_ANOTATION_NAME,
                    name,
                )
                obj.setdefault("metadata", {}).setdefault("annotations", {})[
                    INTERNAL_NAME_ANOTATION_NAME
                ] = name

            # Allow children to inject additional modification logic
            log.debug4("Before Object Updates: %s", obj)
            obj = self.update_object_definition(session, name, obj)

            # Add the resource to the set managed by the is component
            managed_obj = ManagedObject(obj, verify_func, deploy_method)
            log.debug2("Adding managed object: %s", managed_obj)
            log.debug4("Final Definition: %s", obj)
            self._managed_objects.append(managed_obj)

        return self.managed_objects

    def __gather_resources(
        self, session
    ) -> List[Tuple[str, dict, Callable, DeployMethod]]:
        """This is a helper for __render which handles converting resource objects
        into a list of dictionaries.
        """
        # Perform lazy chart creation before finishing rendering
        self.__build_lazy_charts(session)

        # Determine the flattened set of ApiObject children.
        log.debug2("%s populating managed_objects", self)
        topology = self.graph.topology()
        log.debug3("%s topology has %d elements", self, len(topology))
        log.debug4([type(obj) for obj in topology])
        children = [node for node in topology if isinstance(node, ResourceNode)]
        log.debug2("%s found %d ResourceNode children", self, len(children))

        resource_list = []
        for child in children:
            # Construct the managed object with its internal name
            child_name = ".".join([self.name, child.get_name()])
            resource_list.append(
                (child_name, child.manifest, child.verify_function, child.deploy_method)
            )

        return resource_list
