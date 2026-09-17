// Package dag provides a directed-acyclic-graph execution engine for
// ordering and concurrently running operator reconciliation work.
//
// Ported from oper8 Python (oper8/dag/node.py, graph.py).
// Design decision: Node is a plain struct; no constructor side-effects.
// Dependencies are declared explicitly by the caller (idiomatic Go).
package dag

import (
	"fmt"
	"maps"
	"slices"
)

// NodeFunc is the unit of work a Node carries. Returning a [HaltError]
// signals the Runner to stop the graph; any other non-nil error marks the
// node as failed.
type NodeFunc func() error

// EdgeFunc gates a dependency edge. Before a dependent node starts, the
// Runner calls EdgeFunc (if non-nil) on the edge connecting it to each
// upstream. If EdgeFunc returns false the dependent waits.
// A nil EdgeFunc means "upstream completed is sufficient".
type EdgeFunc func() bool

// edge is the internal representation of a directed edge to a child node.
type edge struct {
	node   *Node
	verify EdgeFunc // may be nil
}

// Node is a named vertex in a [Graph]. It stores an optional [NodeFunc]
// and a set of directed edges to upstream dependency nodes.
type Node struct {
	name     string
	fn       NodeFunc
	children map[string]edge // key = upstream node name
}

// NewNode creates a Node with no work function. Useful as a structural
// placeholder in the graph.
func NewNode(name string) *Node {
	return &Node{name: name, children: make(map[string]edge)}
}

// NewFuncNode creates a Node that executes fn when the Runner processes it.
func NewFuncNode(name string, fn NodeFunc) *Node {
	return &Node{name: name, fn: fn, children: make(map[string]edge)}
}

// Name returns the node's unique name within a Graph.
func (n *Node) Name() string { return n.name }

// Func returns the NodeFunc, which may be nil.
func (n *Node) Func() NodeFunc { return n.fn }

// AddChild adds a directed edge from n to upstream child, optionally gated
// by verify. Returns an error if adding the edge would create a cycle.
func (n *Node) AddChild(child *Node, verify EdgeFunc) error {
	if child == n {
		return fmt.Errorf("dag: self-loop on node %q", n.name)
	}
	// Cycle check: does a path already exist from child back to n?
	if child.hasPath(n, nil) {
		return fmt.Errorf("dag: adding edge %q → %q would create a cycle", n.name, child.name)
	}
	n.children[child.name] = edge{node: child, verify: verify}
	return nil
}

// Children returns all upstream dependency edges in deterministic (sorted) order.
func (n *Node) Children() []edge {
	keys := slices.Sorted(maps.Keys(n.children))
	out := make([]edge, len(keys))
	for i, k := range keys {
		out[i] = n.children[k]
	}
	return out
}

// hasPath reports whether there exists a directed path from n to target.
// visited prevents infinite loops during traversal.
func (n *Node) hasPath(target *Node, visited map[string]bool) bool {
	if visited == nil {
		visited = make(map[string]bool)
	}
	if n == target {
		return true
	}
	visited[n.name] = true
	for _, e := range n.children {
		if !visited[e.node.name] && e.node.hasPath(target, visited) {
			return true
		}
	}
	return false
}

// Topology returns this node and all reachable descendants in
// dependency-first (DFS post-order) order — the order in which they
// should be deployed.
func (n *Node) Topology() []*Node {
	seen := make(map[string]bool)
	var result []*Node
	var visit func(v *Node)
	visit = func(v *Node) {
		for _, e := range v.Children() {
			visit(e.node)
		}
		if !seen[v.name] {
			result = append(result, v)
			seen[v.name] = true
		}
	}
	visit(n)
	return result
}

// String returns a human-readable representation.
func (n *Node) String() string { return fmt.Sprintf("Node(%q)", n.name) }

// ── ResourceNode ─────────────────────────────────────────────────────────────

// DeployMethod controls how a resource is applied to the cluster.
type DeployMethod string

const (
	// DeployMethodDefault uses server-side apply (recommended).
	DeployMethodDefault DeployMethod = "default"
	// DeployMethodUpdate uses a PUT with the current resourceVersion.
	DeployMethodUpdate DeployMethod = "update"
	// DeployMethodReplace DELETEs then re-CREATEs the resource.
	DeployMethodReplace DeployMethod = "replace"
)

// ResourceNode is a [Node] that also carries a Kubernetes resource manifest
// and optional verification logic.
type ResourceNode struct {
	*Node

	// Manifest is the full unstructured Kubernetes object definition.
	Manifest map[string]any

	// VerifyFunc is called after deploy to confirm readiness.
	// If nil the built-in verifier registry is consulted by the Runner.
	VerifyFunc func(obj map[string]any) bool

	// DeployMethod controls the apply strategy.
	DeployMethod DeployMethod
}

// NewResourceNode creates a ResourceNode.
func NewResourceNode(name string, manifest map[string]any, verify func(map[string]any) bool, method DeployMethod) *ResourceNode {
	if method == "" {
		method = DeployMethodDefault
	}
	return &ResourceNode{
		Node:         NewNode(name),
		Manifest:     manifest,
		VerifyFunc:   verify,
		DeployMethod: method,
	}
}
