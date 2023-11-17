"""This class provides a base class with shared functionality that all
concrete components can use.
"""

# Standard
from typing import Union

# First Party
import alog

# Local
from .utils import common, deps_annotation
from oper8 import Component, Session

log = alog.use_channel("OPCMP")


## Oper8xComponent #############################################################

default = {}


class Oper8xComponent(Component):
    """The Oper8xComponent provides common config-based utilities on top of the
    core oper8.Component base class. It can be used as a drop-in replacement.
    """

    def __init__(self, session: Session, disabled: bool = False):
        """Construct with a member to access the session in implementations

        Args:
            session:  Session
                The session for the current deployment
            disabled:  bool
                Whether or not this component is disabled in the current
                configuration
        """
        super().__init__(session=session, disabled=disabled)
        self._session = session

    @property
    def session(self):
        return self._session

    ## Interface Overrides #####################################################

    def deploy(self, session: Session) -> bool:
        """Override the base Component's implementation of deploy to insert the
        dependency hash annotation. See NOTE in deps_annotation for explanation
        of why deploy is used instead of update_object_definition.

        Args:
            session:  Session
                The session for the current deployment

        Returns:
            success:  bool
                True on successful application of the resource to the cluster
        """
        for obj in self.managed_objects:
            obj.definition = deps_annotation.add_deps_annotation(
                self, session, obj.definition
            )
        return super().deploy(session)

    def update_object_definition(
        self,
        session: Session,
        internal_name: str,
        resource_definition: dict,
    ) -> dict:
        """For components assigned to different namespaces, ensure that the
        target namespace is set

        Args:
            session:  Session
                The session for this deploy
            internal_name:  str
                The internal name of the object to update
            resource_definition:  dict
                The dict representation of the resource to modify

        Returns:
            resource_definition:  dict
                The dict representation of the resource with any modifications
                applied
        """

        # Call the base implementation
        resource_definition = super().update_object_definition(
            session,
            internal_name,
            resource_definition,
        )

        # Inject namespace override for this component if given
        namespace_override = session.config.get(self.name, {}).get("namespace")
        if namespace_override is not None:
            log.debug2("Namespace  override for %s: %s", self, namespace_override)
            metadata = resource_definition.get("metadata")
            assert isinstance(metadata, dict), "Resource metadata is not a dict!"
            metadata["namespace"] = namespace_override

        return resource_definition

    ## Shared Utilities ########################################################

    def get_cluster_name(self, resource_name: str) -> str:
        """Get the name for a given resource with any instance scoping applied

        Args:
            resource_name:  str
                The unscoped name of a kubernetes resource

        Returns:
            resource_cluster_name:  str
                The name that the resource will use in the cluster
        """
        return common.get_resource_cluster_name(
            resource_name=resource_name,
            component=self.name,
            session=self.session,
        )

    def get_replicas(self, force: bool = False) -> Union[int, None]:
        """Get the replica count for this component based on the current
        deploy's t-shirt size and the state of the instance-size label. A
        replica count is only returned if there is not an existing replica count
        in the cluster for this deployment, the t-shirt size has changed, or
        the force flag is True.

        Args:
            force: bool
                If True, the state of the cluster will not be checked

        Returns:
            replicas:  Union[int, None]
                If replicas should be set for this deployment, the integer count
                will be returned, otherwise None is returned.
        """
        return common.get_replicas(
            session=self.session,
            component_name=self.name,
            unscoped_name=self.name,
            force=force,
        )
