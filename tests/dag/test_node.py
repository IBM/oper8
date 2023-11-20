"""
Test the Node and ResourceNode classes functionality
"""

# Third Party
import pytest

# Local
from oper8.dag import Node, ResourceNode

################################################################################
## Node Tests #############################################################
################################################################################


def test_node_creation():
    """Test node initialization"""
    node = Node()
    assert node.get_name() == None
    assert node.get_data() == None
    node = Node("test", "data")
    assert node.get_name() == "test"
    assert node.get_data() == "data"


def test_node_children():
    """Test node children functions"""
    node_a = Node("a")
    node_b = Node("b")
    node_c = Node("c")
    node_a.add_child(node_b)
    node_a.add_child(node_c, "testdata")

    assert node_a.has_child(node_b)
    assert node_a.get_children() == [(node_b, None), (node_c, "testdata")]

    node_a.remove_child(node_b)
    assert not node_a.has_child(node_b)
    assert node_a.get_children() == [(node_c, "testdata")]


def test_node_cyclic_dependency():
    """Test node children functions"""
    node_a = Node("a")
    node_b = Node("b")
    node_a.add_child(node_b)
    with pytest.raises(ValueError):
        node_b.add_child(node_a)


def test_node_topology():
    """Test node topology function"""

    # Graph
    #  a->b->c
    node_a = Node("a")
    node_b = Node("b")
    node_c = Node("c")
    node_a.add_child(node_b)
    node_b.add_child(node_c)

    assert node_a.topology() == [node_c, node_b, node_a]
    assert node_b.topology() == [node_c, node_b]
    assert node_c.topology() == [node_c]


def test_node_dfs():
    """Test node dfs search"""

    # Graph
    #  a
    # / \
    # b  c
    # |
    # d
    node_a = Node("a")
    node_b = Node("b")
    node_c = Node("c")
    node_d = Node("d")
    node_a.add_child(node_b)
    node_a.add_child(node_c)
    node_b.add_child(node_d)

    # node a can reach all children
    assert node_a.dfs(node_b) and node_a.dfs(node_c) and node_a.dfs(node_d)
    # node b can only reach d and not c
    assert node_b.dfs(node_d) and not node_b.dfs(node_c)
    # node_d can't reach anything except itself
    assert (
        node_d.dfs(node_d)
        and not node_d.dfs(node_b)
        and not node_d.dfs(node_c)
        and not node_d.dfs(node_a)
    )


def test_node_equality():
    # Check equality
    node_a = Node("a")
    assert node_a != Node("b")
    assert node_a == Node("a")
    assert node_a != "arandomtype"

    # Check sorting
    assert node_a < Node("b")
    assert not Node("z") < node_a
    assert not Node("z") < "arandomtype"


def test_node_descriptors():
    assert hash(Node("a")) == hash("a")
    assert str(Node("a")) == "Node('a', None)"


################################################################################
## Resource Node Tests #############################################################
################################################################################


def create_dummy_kube_resource():
    return {
        "kind": "Foo",
        "apiVersion": "foo.bar/v1",
        "metadata": {
            "name": "foo",
            "namespace": "default",
        },
    }


def test_resource_node_attributes():
    node = ResourceNode("a", create_dummy_kube_resource())
    assert node.api_group == "foo.bar"
    assert node.api_version == "foo.bar/v1"
    assert node.kind == "Foo"
    assert node.metadata == {"name": "foo", "namespace": "default"}
    assert node.name == "foo"


def test_resource_node_add_dependency():
    node_a = ResourceNode("a", create_dummy_kube_resource())
    node_b = ResourceNode("b", create_dummy_kube_resource())
    node_a.add_dependency(node_b)
    assert node_a.has_child(node_b)
    assert node_a.get_children() == [(node_b, None)]
