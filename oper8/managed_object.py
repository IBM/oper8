"""
Helper object to represent a kubernetes object that is managed by the operator
"""
# Standard
from typing import Callable, Optional
import uuid

KUBE_LIST_IDENTIFIER = "List"


class ManagedObject:  # pylint: disable=too-many-instance-attributes
    """Basic struct to represent a managed kubernetes object"""

    def __init__(
        self,
        definition: dict,
        verify_function: Optional[Callable] = None,
        deploy_method: Optional["DeployMethod"] = None,  # noqa: F821
    ):
        self.kind = definition.get("kind")
        self.metadata = definition.get("metadata", {})
        self.name = self.metadata.get("name")
        self.namespace = self.metadata.get("namespace")
        self.uid = self.metadata.get("uid", uuid.uuid4())
        self.resource_version = self.metadata.get("resourceVersion")
        self.api_version = definition.get("apiVersion")
        self.definition = definition
        self.verify_function = verify_function
        self.deploy_method = deploy_method

        # If resource is not list then check name
        if KUBE_LIST_IDENTIFIER not in self.kind:
            assert self.name is not None, "No name found"

        assert self.kind is not None, "No kind found"
        assert self.api_version is not None, "No apiVersion found"

    def get(self, *args, **kwargs):
        """Pass get calls to the objects definition"""
        return self.definition.get(*args, **kwargs)

    def __str__(self):
        return f"{self.api_version}/{self.kind}/{self.name}"

    def __repr__(self):
        return str(self)

    def __hash__(self):
        """Hash explicitly excludes the definition so that the object's
        identifier in a map can be based only on the unique identifier of the
        resource in the cluster. If the original resource did not provide a unique
        identifier then use the apiVersion, kind, and name
        """
        return hash(self.metadata.get("uid", str(self)))

    def __eq__(self, other):
        return hash(self) == hash(other)
