"""
Test the DAG Graph Class functionality
"""
# Third Party
import pytest

# Local
from oper8.dag import Graph, Node

################################################################################
## Graph Tests #############################################################
################################################################################


def test_creation():
    """Test node initialization"""
    graph = Graph()
    assert graph.root


def test_add_node():
    """Test node children functions"""
    graph = Graph()
    node_a = Node("a")
    graph.add_node(node_a)
    assert graph.root.has_child(node_a)


def test_add_node_exception():
    """Test node children functions"""
    graph = Graph()
    with pytest.raises(ValueError):
        graph.add_node(Node(None))

    graph.add_node(Node("a"))
    with pytest.raises(ValueError):
        graph.add_node(Node("a"))


def test_add_node_dependency():
    """Test node children functions"""
    graph = Graph()
    node_a = Node("a")
    node_b = Node("b")
    node_c = Node("c")
    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node(node_c)
    graph.add_node_dependency(node_a, node_b)
    graph.add_node_dependency(node_a, node_c, "testdata")

    assert node_a.get_children() == [(node_b, None), (node_c, "testdata")]


def test_add_node_dependency_exception():
    """Test node children functions"""
    graph = Graph()
    node_a = Node("a")
    node_b = Node("b")
    graph.add_node(node_a)
    graph.add_node(node_b)

    with pytest.raises(ValueError):
        graph.add_node_dependency(node_a, Node("c"))
    with pytest.raises(ValueError):
        graph.add_node_dependency(Node("c"), node_a)


def test_accessors():
    """Test node children functions"""
    graph = Graph()
    node_a = Node("a")
    node_b = Node("b")
    graph.add_node(node_a)
    graph.add_node(node_b)

    assert graph.get_node("a") == node_a
    assert graph.get_all_nodes() == [node_a, node_b]
    assert graph.has_node(node_a)
    assert node_a in graph
    assert not graph.has_node(Node("c"))


def test_topology():
    graph = Graph()
    node_a = Node("a")
    node_b = Node("b")
    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node_dependency(node_b, node_a)

    assert graph.topology() == [node_a, node_b]


def test_topology_ordering():
    """Test node topology function"""
    graph = Graph()
    node_a = Node("a")
    node_b = Node("b")
    node_c = Node("c")
    graph.add_node(node_c)
    graph.add_node(node_b)
    graph.add_node(node_a)

    assert graph.topology() == [node_a, node_b, node_c]


def test_str():
    graph = Graph()
    node_a = Node("a")
    node_b = Node("b")
    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.add_node_dependency(node_a, node_b)

    assert str(graph) == "Graph({a:[b],b:[]})"
