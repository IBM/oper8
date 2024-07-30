"""
This module contains a collection of classes for implementing nodes of a Graph
"""
# Standard
from typing import Any, Callable, List, Optional


class Node:
    """Class for representing a node in the Graph"""

    def __init__(
        self,
        name: Optional[str] = None,
        data: Optional[Any] = None,
    ) -> None:
        """Construct a new Node

        Args:
            name:  Optional[str]
                The name of the node
            data:  Optional[Any]
                Any data that should be stored with the node
        """
        self._name = name
        self._data = data
        self.children = {}

    ## Modifiers ##############################################################
    def add_child(self, node: "Node", edge_data: Optional[Any] = None):
        """Add edge from  self to node with optional edge data"""
        if node.dfs(self):
            raise ValueError("Unable to add cyclic dependency")
        self.children[node] = edge_data

    def remove_child(self, node: "Node"):
        """Remove child node from self"""
        if node in self.children:
            self.children.pop(node)

    def set_data(self, data: Any):
        """Mutator for node data"""
        self._data = data

    ## Accessors ##############################################################

    def get_data(self):
        """Accessor for specific child"""
        return self._data

    def get_name(self):
        """Accessor for specific child"""
        return self._name

    def has_child(self, node: "Node"):
        """Accessor for specific child"""
        return node in self.children

    def get_children(self) -> set:
        """Accessor for all children"""
        return list(self.children.items())

    ## Graph Functions ##############################################################
    def topology(self) -> List["Node"]:
        """Function to get an ordered topology of a node's children"""
        found = set()
        topology = []

        def visit(node):
            for child, _ in sorted(node.get_children()):
                visit(child)

            if node not in found:
                topology.append(node)
                found.add(node)

        visit(self)

        return topology

    def dfs(self, node: "Node", visited: List["Node"] = None) -> bool:
        """Function to determine if their is a path between two nodes. Used in acyclic check"""
        if not visited:
            visited = []
        if node == self:
            return True
        visited.append(self)
        for child, _ in self.get_children():
            if child not in visited:
                if child.dfs(node, visited):
                    return True
                visited.append(child)
        return False

    ## Internal ##
    def __eq__(self, obj):
        """Compare and sort nodes by name"""
        if not isinstance(obj, Node):
            return False
        return (self.get_name()) == (obj.get_name())

    def __lt__(self, obj):
        if not isinstance(obj, Node):
            return False
        return (self.get_name()) < (obj.get_name())

    def __repr__(self) -> str:
        # __repr__ may be called before __init__ thus _name is not present
        if hasattr(self, "_name"):
            return f"{self.__class__.__name__}('{self.get_name()}', {self.get_data()})"
        return super().__repr__()

    def __hash__(self) -> str:
        return self.get_name().__hash__()


class ResourceNode(Node):
    """Class for representing a kubernetes resource in the Graph with
    a function for verifying said resource"""

    def __init__(
        self,
        name: str,
        manifest: dict,
        verify_func: Optional[Callable] = None,
        deploy_method: Optional["DeployMethod"] = None,  # noqa: F821
    ):
        # Override init to require name/manifest parameters
        super().__init__(name, manifest)
        self._verify_function = verify_func
        self._deploy_method = deploy_method
        if not deploy_method:
            # Local
            from ..deploy_manager import DeployMethod

            self._deploy_method = DeployMethod.DEFAULT

    ## ApiObject Parameters and Functions ######################################
    @property
    def api_group(self) -> str:
        """The kubernetes apiVersion group name without the schema version"""
        return self.api_version.split("/")[0]

    @property
    def api_version(self) -> str:
        """The full kubernetes apiVersion"""
        return self.manifest.get("apiVersion")

    @property
    def kind(self) -> str:
        """The resource kind"""
        return self.manifest.get("kind")

    @property
    def metadata(self) -> dict:
        """The full resource metadata dict"""
        return self.manifest.get("metadata", {})

    @property
    def name(self) -> str:
        """The resource metadata.name"""
        return self.metadata.get("name")

    @property
    def manifest(self) -> dict:
        """The resource manifest"""
        return self.get_data()

    @property
    def verify_function(self) -> Optional[Callable]:
        """The resource manifest"""
        return self._verify_function

    @property
    def deploy_method(self) -> Optional["DeployMethod"]:  # noqa: F821
        """The resource manifest"""
        return self._deploy_method

    def add_dependency(self, node: "ResourceNode"):
        """Add a child dependency to this node"""
        self.add_child(node)
