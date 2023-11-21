"""
Graph holds information about a Directed Acyclic Graph
"""


# Standard
from typing import Callable, List, Optional

# First Party
import alog

# Local
from .node import Node

log = alog.use_channel("DAG")

## Graph Class ##############################################################


class Graph:
    """Class for representing an instance of a Graph. Handles adding and removing nodes
    as well as graph functions like flattening"""

    def __init__(self) -> None:
        self.__node_dict = {}

        # Add the root node of the Graph. Every member of this graph is also a child
        # of the root node
        self.__root_node = Node()
        self.__node_dict[self.__root_node.get_name()] = self.__root_node

    ## Properties ##############################################################

    @property
    def root(self) -> Node:  # pylint: disable=invalid-name
        """The root node of the Graph"""
        return self.__root_node

    @property
    def node_dict(self) -> dict:
        """Dictionary of all node names and their nodes"""
        return self.__node_dict

    ## Modifiers ##############################################################

    def add_node(self, node: Node):
        """Add node to graph
        Args:
            node:  Node
                The node to be added to the Dag.
        """
        if not node.get_name():
            raise ValueError("None is reserved for the root node of the dag Graph")

        if node.get_name() in self.node_dict:
            raise ValueError(
                f"Only one node with id {node.get_name()} can be added to a Graph"
            )

        self.node_dict[node.get_name()] = node
        self.root.add_child(node)

    def add_node_dependency(
        self, parent_node: Node, child_node: Node, edge_fn: Optional[Callable] = None
    ):
        """Add dependency or "edge" to graph between two nodes. This is the same
        as doing parent_node.add_dependency(child_node)
        Args:
            parent_node:  Node
                The parent or dependent node aka the node that must wait
            child_node: Node
                The child or dependency node aka the node that must be deployed first
            edge_fn:
        """
        if not self.get_node(parent_node.get_name()):
            raise ValueError(f"Parent node {parent_node} is not present in Graph")

        if not self.get_node(child_node.get_name()):
            raise ValueError(f"Child node {child_node} is not present in Graph")

        # Make sure edits are applied to the nodes already present in the graph
        parent_node = self.get_node(parent_node.get_name())
        child_node = self.get_node(child_node.get_name())

        parent_node.add_child(child_node, edge_fn)

    ## Accessors ##############################################################

    def get_node(self, name: str):  # pylint: disable=invalid-name
        """Get the node with name"""
        return self.node_dict.get(name)

    def get_all_nodes(self):
        """Get list of all nodes"""
        return [node for node, _ in self.root.get_children()]

    def has_node(self, node: Node):  # pylint: disable=invalid-name
        """Check if node is in graph"""
        return self.root.has_child(node)

    def empty(self):
        """Check if a graph is empty"""
        return len(self.root.get_children()) == 0

    ## Graph Functions ##############################################################

    def topology(self) -> List["Node"]:
        """Get a list of nodes in deployment order"""
        topology = self.root.topology()
        topology.remove(self.root)
        return topology

    ## Internal Functions ##############################################################

    def __repr__(self):
        str_list = []
        for child, _ in self.root.get_children():
            child_str_list = [node.get_name() for node, _ in child.get_children()]
            str_list.append(f"{child.get_name()}:[{','.join(child_str_list)}]")

        return f"Graph({{{','.join(str_list)}}})"

    def __contains__(self, item: Node):
        return self.has_node(item)

    def __iter__(self):
        """Iterate over all child nodes"""
        return self.get_all_nodes().__iter__()
