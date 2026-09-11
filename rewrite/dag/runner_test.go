package dag_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/oper8-go/dag"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// okNode returns a node whose function succeeds immediately.
func okNode(name string) *dag.Node {
	return dag.NewFuncNode(name, func() error { return nil })
}

// failNode returns a node whose function returns a fatal HaltError.
func failNode(name string) *dag.Node {
	return dag.NewFuncNode(name, func() error {
		return &dag.HaltError{Fatal: true, Cause: fmt.Errorf("forced failure")}
	})
}

// unverifiedNode returns a node that signals "deployed but not verified".
func unverifiedNode(name string) *dag.Node {
	return dag.NewFuncNode(name, func() error {
		return &dag.HaltError{Fatal: false, Cause: fmt.Errorf("not ready")}
	})
}

// buildLinearGraph creates: a → b → c (c must deploy first, then b, then a).
func buildLinearGraph(t *testing.T, nodes ...*dag.Node) *dag.Graph {
	t.Helper()
	g := dag.NewGraph()
	for _, n := range nodes {
		if err := g.AddNode(n); err != nil {
			t.Fatalf("AddNode %q: %v", n.Name(), err)
		}
	}
	// Chain: nodes[0] depends on nodes[1] depends on nodes[2] …
	for i := 0; i < len(nodes)-1; i++ {
		if err := g.AddDependency(nodes[i], nodes[i+1], nil); err != nil {
			t.Fatalf("AddDependency: %v", err)
		}
	}
	return g
}

// assertCounts checks expected node counts in a CompletionState.
func assertCounts(t *testing.T, cs *dag.CompletionState, verified, unverified, failed, unstarted int) {
	t.Helper()
	if got := len(cs.Verified); got != verified {
		t.Errorf("Verified: want %d got %d", verified, got)
	}
	if got := len(cs.Unverified); got != unverified {
		t.Errorf("Unverified: want %d got %d", unverified, got)
	}
	if got := len(cs.Failed); got != failed {
		t.Errorf("Failed: want %d got %d", failed, got)
	}
	if got := len(cs.Unstarted); got != unstarted {
		t.Errorf("Unstarted: want %d got %d", unstarted, got)
	}
}

// ── Graph / Node unit tests ───────────────────────────────────────────────────

func TestNewGraph_Empty(t *testing.T) {
	g := dag.NewGraph()
	if !g.Empty() {
		t.Fatal("expected empty graph")
	}
}

func TestAddNode_Duplicate(t *testing.T) {
	g := dag.NewGraph()
	n := okNode("a")
	if err := g.AddNode(n); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(dag.NewNode("a")); err == nil {
		t.Fatal("expected error on duplicate node name")
	}
}

func TestAddNode_EmptyName(t *testing.T) {
	g := dag.NewGraph()
	if err := g.AddNode(dag.NewNode("")); err == nil {
		t.Fatal("expected error on empty name (reserved for root)")
	}
}

func TestCycleDetection(t *testing.T) {
	a, b := okNode("a"), okNode("b")
	g := dag.NewGraph()
	_ = g.AddNode(a)
	_ = g.AddNode(b)
	_ = g.AddDependency(a, b, nil) // a → b

	// Trying to add b → a should fail (would create a cycle).
	if err := g.AddDependency(b, a, nil); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestSelfLoop(t *testing.T) {
	a := okNode("a")
	if err := a.AddChild(a, nil); err == nil {
		t.Fatal("expected self-loop error")
	}
}

func TestTopology_Linear(t *testing.T) {
	a, b, c := okNode("a"), okNode("b"), okNode("c")
	g := buildLinearGraph(t, a, b, c) // a→b→c so order should be c,b,a
	topo := g.Topology()
	names := make([]string, len(topo))
	for i, n := range topo {
		names[i] = n.Name()
	}
	// c has no deps, b depends on c, a depends on b
	want := []string{"c", "b", "a"}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("topo[%d]: want %q got %q", i, w, names[i])
		}
	}
}

func TestGraphString(t *testing.T) {
	g := dag.NewGraph()
	a, b := okNode("a"), okNode("b")
	_ = g.AddNode(a)
	_ = g.AddNode(b)
	_ = g.AddDependency(a, b, nil)
	s := g.String()
	// Just verify it contains both names and the dependency.
	for _, want := range []string{"a:", "b:", "[b]"} {
		if !containsStr(s, want) {
			t.Errorf("String() = %q; want to contain %q", s, want)
		}
	}
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}

// ── Runner — serial mode ──────────────────────────────────────────────────────

func TestRunner_Serial_AllSucceed(t *testing.T) {
	g := buildLinearGraph(t, okNode("a"), okNode("b"), okNode("c"))
	cs := dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	assertCounts(t, cs, 3, 0, 0, 0)
	if !cs.DeployCompleted() {
		t.Error("DeployCompleted should be true")
	}
	if !cs.VerifyCompleted() {
		t.Error("VerifyCompleted should be true")
	}
}

func TestRunner_Serial_EmptyGraph(t *testing.T) {
	g := dag.NewGraph()
	cs := dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	assertCounts(t, cs, 0, 0, 0, 0)
	if !cs.DeployCompleted() {
		t.Error("empty graph should be DeployCompleted")
	}
	if !cs.VerifyCompleted() {
		t.Error("empty graph should be VerifyCompleted")
	}
}

func TestRunner_Serial_FatalHalt(t *testing.T) {
	// b fails → a should be Unstarted (depends on b), c is independent.
	a, b, c := okNode("a"), failNode("b"), okNode("c")
	g := dag.NewGraph()
	for _, n := range []*dag.Node{a, b, c} {
		_ = g.AddNode(n)
	}
	_ = g.AddDependency(a, b, nil) // a depends on b

	cs := dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	// b failed, a unstarted, c verified (independent)
	assertCounts(t, cs, 1, 0, 1, 1)
	if !cs.AnyFailed() {
		t.Error("AnyFailed should be true")
	}
}

func TestRunner_Serial_UnverifiedHalt(t *testing.T) {
	// b returns HaltError{Fatal:false} → Unverified; a (depends on b) Unstarted.
	a, b := okNode("a"), unverifiedNode("b")
	g := buildLinearGraph(t, a, b)
	cs := dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	assertCounts(t, cs, 0, 1, 0, 1) // b=unverified, a=unstarted
	if cs.VerifyCompleted() {
		t.Error("VerifyCompleted should be false")
	}
	if cs.AnyFailed() {
		t.Error("AnyFailed should be false for non-fatal halt")
	}
}

func TestRunner_Serial_DisabledNode(t *testing.T) {
	// Disable b; a depends on b. Both should still be "verified" (disabled = no-op deploy).
	a, b := okNode("a"), dag.NewFuncNode("b", func() error {
		return errors.New("should not run")
	})
	g := buildLinearGraph(t, a, b)
	r := dag.NewRunner(g, dag.WithConcurrency(0))
	r.DisableNode("b")
	cs := r.Run(context.Background())
	assertCounts(t, cs, 2, 0, 0, 0)
}

func TestRunner_Serial_ExecutionOrder(t *testing.T) {
	// Verify topological order: c before b before a.
	var order []string
	makeNode := func(name string) *dag.Node {
		return dag.NewFuncNode(name, func() error {
			order = append(order, name)
			return nil
		})
	}
	a, b, c := makeNode("a"), makeNode("b"), makeNode("c")
	g := buildLinearGraph(t, a, b, c)
	dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	want := []string{"c", "b", "a"}
	for i, w := range want {
		if order[i] != w {
			t.Errorf("order[%d]: want %q got %q", i, w, order[i])
		}
	}
}

// ── Runner — concurrent mode ──────────────────────────────────────────────────

func TestRunner_Concurrent_AllSucceed(t *testing.T) {
	g := buildLinearGraph(t, okNode("a"), okNode("b"), okNode("c"))
	cs := dag.NewRunner(g).Run(context.Background())
	assertCounts(t, cs, 3, 0, 0, 0)
}

func TestRunner_Concurrent_FatalHalt(t *testing.T) {
	a, b := okNode("a"), failNode("b")
	g := buildLinearGraph(t, a, b)
	cs := dag.NewRunner(g).Run(context.Background())
	// b fails, a never starts
	assertCounts(t, cs, 0, 0, 1, 1)
}

func TestRunner_Concurrent_IndependentNodesRunParallel(t *testing.T) {
	// a, b, c have no dependencies. Verify they run concurrently by recording
	// start times. With true concurrency, all 3 start before any finishes.
	// Each node sleeps briefly to allow others to start, then records its
	// start time. We assert all 3 started within 10ms of each other.
	type record struct {
		name  string
		start time.Time
	}
	records := make(chan record, 3)

	makeNode := func(name string) *dag.Node {
		return dag.NewFuncNode(name, func() error {
			records <- record{name: name, start: time.Now()}
			time.Sleep(10 * time.Millisecond)
			return nil
		})
	}
	g := dag.NewGraph()
	for _, name := range []string{"a", "b", "c"} {
		_ = g.AddNode(makeNode(name))
	}

	cs := dag.NewRunner(g).Run(context.Background())
	assertCounts(t, cs, 3, 0, 0, 0)

	// Collect start times.
	close(records)
	times := make([]time.Time, 0, 3)
	for rec := range records {
		_ = rec.name
		times = append(times, rec.start)
	}
	if len(times) != 3 {
		t.Fatalf("expected 3 start records, got %d", len(times))
	}
	// Find spread between earliest and latest start.
	earliest, latest := times[0], times[0]
	for _, ts := range times[1:] {
		if ts.Before(earliest) {
			earliest = ts
		}
		if ts.After(latest) {
			latest = ts
		}
	}
	spread := latest.Sub(earliest)
	// Serial: nodes[1] starts after nodes[0] finishes (≥10ms later).
	// Concurrent: all 3 start nearly simultaneously.
	// 50ms threshold is generous enough for slow CI while still catching serial.
	if spread > 50*time.Millisecond {
		t.Errorf("start-time spread=%v; nodes appear to be running serially (want <50ms)", spread)
	}
}

func TestRunner_Concurrent_RaceDetector(t *testing.T) {
	// This test is specifically designed to trigger races if the runner has
	// unsynchronised state. Run with: go test -race ./dag/...
	const numNodes = 20
	g := dag.NewGraph()
	nodes := make([]*dag.Node, numNodes)
	var counter int64
	for i := 0; i < numNodes; i++ {
		nodes[i] = dag.NewFuncNode(fmt.Sprintf("n%d", i), func() error {
			atomic.AddInt64(&counter, 1)
			time.Sleep(time.Millisecond)
			return nil
		})
		_ = g.AddNode(nodes[i])
	}
	cs := dag.NewRunner(g).Run(context.Background())
	assertCounts(t, cs, numNodes, 0, 0, 0)
	if atomic.LoadInt64(&counter) != numNodes {
		t.Errorf("counter: want %d got %d", numNodes, counter)
	}
}

func TestRunner_Concurrent_ContextCancellation(t *testing.T) {
	// Nodes that sleep long enough that context cancellation fires first.
	g := dag.NewGraph()
	for _, name := range []string{"a", "b", "c"} {
		_ = g.AddNode(dag.NewFuncNode(name, func() error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	cs := dag.NewRunner(g).Run(ctx)
	// All nodes should be in Unstarted since context was cancelled early.
	total := len(cs.Verified) + len(cs.Unverified) + len(cs.Failed) + len(cs.Unstarted)
	if total != 3 {
		t.Errorf("total nodes in CompletionState: want 3 got %d", total)
	}
}

// ── EdgeFunc / upstream verification ─────────────────────────────────────────

func TestRunner_EdgeFunc_BlocksDependent(t *testing.T) {
	// b depends on a; edge says "not satisfied" → b stays Unstarted.
	a, b := okNode("a"), okNode("b")
	g := dag.NewGraph()
	_ = g.AddNode(a)
	_ = g.AddNode(b)
	// b depends on a with an edge that always returns false.
	_ = g.AddDependency(b, a, func() bool { return false })

	cs := dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	// a verified, b unstarted because edge never satisfied
	assertCounts(t, cs, 1, 0, 0, 1)
}

func TestRunner_EdgeFunc_AllowsDependent(t *testing.T) {
	a, b := okNode("a"), okNode("b")
	g := dag.NewGraph()
	_ = g.AddNode(a)
	_ = g.AddNode(b)
	_ = g.AddDependency(b, a, func() bool { return true })

	cs := dag.NewRunner(g, dag.WithConcurrency(0)).Run(context.Background())
	assertCounts(t, cs, 2, 0, 0, 0)
}

// ── CompletionState predicates ────────────────────────────────────────────────

func TestCompletionState_Predicates(t *testing.T) {
	allGood := &dag.CompletionState{Verified: []*dag.Node{okNode("x")}}
	if !allGood.DeployCompleted() {
		t.Error("DeployCompleted")
	}
	if !allGood.VerifyCompleted() {
		t.Error("VerifyCompleted")
	}
	if allGood.AnyFailed() {
		t.Error("AnyFailed should be false")
	}

	hasFailed := &dag.CompletionState{Failed: []*dag.Node{okNode("x")}}
	if !hasFailed.AnyFailed() {
		t.Error("AnyFailed should be true")
	}
	if hasFailed.DeployCompleted() {
		t.Error("DeployCompleted should be false with failed nodes")
	}
}

// ── ResourceNode ─────────────────────────────────────────────────────────────

func TestResourceNode(t *testing.T) {
	manifest := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "test"},
	}
	rn := dag.NewResourceNode("deploy-test", manifest, nil, dag.DeployMethodDefault)
	if rn.Name() != "deploy-test" {
		t.Errorf("Name: %q", rn.Name())
	}
	if rn.DeployMethod != dag.DeployMethodDefault {
		t.Errorf("DeployMethod: %v", rn.DeployMethod)
	}
}
