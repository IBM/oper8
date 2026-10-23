package dag

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Graph is a directed acyclic graph of [Node] values. All nodes share a
// synthetic root node so that the Runner can treat the graph uniformly.
//
// Graph is not safe for concurrent use during construction; after the graph
// is handed to a [Runner] it must not be mutated.
type Graph struct {
	root  *Node            // synthetic root — parent of all top-level nodes
	nodes map[string]*Node // name → node (excludes root)
}

// NewGraph returns an empty Graph.
func NewGraph() *Graph {
	root := NewNode("") // empty name is reserved for the root
	return &Graph{
		root:  root,
		nodes: make(map[string]*Node),
	}
}

// AddNode inserts n into the graph. Returns an error if a node with the
// same name already exists or if the name is empty (reserved for root).
func (g *Graph) AddNode(n *Node) error {
	if n.name == "" {
		return fmt.Errorf("dag: empty name is reserved for the graph root")
	}
	if _, exists := g.nodes[n.name]; exists {
		return fmt.Errorf("dag: node %q already exists in graph", n.name)
	}
	g.nodes[n.name] = n
	// All top-level nodes are children of root (no EdgeFunc needed).
	_ = g.root.AddChild(n, nil) // cannot cycle since root has no parents
	return nil
}

// AddDependency declares that parent depends on child (child deploys first).
// Both nodes must already be in the graph. verify may be nil.
func (g *Graph) AddDependency(parent, child *Node, verify EdgeFunc) error {
	p, ok := g.nodes[parent.name]
	if !ok {
		return fmt.Errorf("dag: parent node %q not in graph", parent.name)
	}
	c, ok := g.nodes[child.name]
	if !ok {
		return fmt.Errorf("dag: child node %q not in graph", child.name)
	}
	return p.AddChild(c, verify)
}

// GetNode returns the node with the given name, or (nil, false) if absent.
func (g *Graph) GetNode(name string) (*Node, bool) {
	n, ok := g.nodes[name]
	return n, ok
}

// Nodes returns all non-root nodes in deterministic (sorted-by-name) order.
func (g *Graph) Nodes() []*Node {
	names := slices.Sorted(maps.Keys(g.nodes))
	out := make([]*Node, len(names))
	for i, name := range names {
		out[i] = g.nodes[name]
	}
	return out
}

// Empty reports whether the graph contains no nodes.
func (g *Graph) Empty() bool { return len(g.nodes) == 0 }

// Topology returns all nodes in dependency-first order (safe deploy order).
// Nodes with no dependencies come first; nodes that depend on others follow.
func (g *Graph) Topology() []*Node {
	all := g.root.Topology()
	// Remove the synthetic root from the result.
	out := make([]*Node, 0, len(all))
	for _, n := range all {
		if n != g.root {
			out = append(out, n)
		}
	}
	return out
}

// String returns a compact representation: Graph({a:[b,c],b:[],c:[]}).
func (g *Graph) String() string {
	names := slices.Sorted(maps.Keys(g.nodes))
	parts := make([]string, 0, len(names))
	for _, name := range names {
		n := g.nodes[name]
		deps := make([]string, 0, len(n.children))
		for _, e := range n.Children() {
			deps = append(deps, e.node.name)
		}
		parts = append(parts, fmt.Sprintf("%s:[%s]", name, strings.Join(deps, ",")))
	}
	return fmt.Sprintf("Graph({%s})", strings.Join(parts, ","))
}
