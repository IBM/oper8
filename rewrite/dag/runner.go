package dag

import (
	"context"
	"fmt"
	"sync"
)

// nodeResult is sent by a node goroutine back to the scheduler.
type nodeResult struct {
	node  *Node
	state NodeState
	err   error // non-nil only for HaltError propagation
}

// RunnerOption configures a [Runner].
type RunnerOption func(*runnerCfg)

type runnerCfg struct {
	concurrency    int  // 0 = serial (no goroutines)
	verifyUpstream bool // call EdgeFunc before starting dependent
}

// WithConcurrency sets the maximum number of nodes that run concurrently.
// 0 means serial execution (topology order, no goroutines). Default: unbounded.
func WithConcurrency(n int) RunnerOption {
	return func(c *runnerCfg) { c.concurrency = n }
}

// WithVerifyUpstream controls whether EdgeFuncs are called to gate
// downstream start. Default: true (mirrors Python oper8 behavior).
func WithVerifyUpstream(v bool) RunnerOption {
	return func(c *runnerCfg) { c.verifyUpstream = v }
}

// Runner executes a [Graph] concurrently, scheduling each node as soon as
// all its upstream dependencies are satisfied.
//
// Goroutine model (replaces Python's ThreadPoolExecutor + sleep-poll loop):
//   - One goroutine per node is spawned when the node becomes ready.
//   - Each goroutine sends a [nodeResult] to a buffered results channel.
//   - The scheduler goroutine reads from the channel and decides what to
//     start next. Zero busy-polling.
//
// When concurrency=0 the graph runs in topology order in the calling
// goroutine — useful for tests and deterministic dry-runs.
type Runner struct {
	graph   *Graph
	cfg     runnerCfg
	disabled map[string]bool

	// state — written only by the scheduler (serial or channel-driven)
	mu         sync.Mutex
	stateMap   map[string]NodeState // name → terminal state
	inFlight   int64                // atomic counter of running goroutines
	fatalErr   error
}

// NewRunner creates a Runner for the given graph.
func NewRunner(graph *Graph, opts ...RunnerOption) *Runner {
	cfg := runnerCfg{verifyUpstream: true}
	for _, o := range opts {
		o(&cfg)
	}
	return &Runner{
		graph:    graph,
		cfg:      cfg,
		disabled: make(map[string]bool),
		stateMap: make(map[string]NodeState),
	}
}

// DisableNode excludes a node from execution without removing it from the
// graph. Its dependents behave as if it completed successfully.
func (r *Runner) DisableNode(name string) { r.disabled[name] = true }

// EnableNode re-enables a previously disabled node.
func (r *Runner) EnableNode(name string) { delete(r.disabled, name) }

// Run executes the graph and returns the [CompletionState]. It blocks until
// all runnable nodes have finished or ctx is cancelled.
func (r *Runner) Run(ctx context.Context) *CompletionState {
	if r.cfg.concurrency == 0 {
		return r.runSerial(ctx)
	}
	return r.runConcurrent(ctx)
}

// ── Serial execution (concurrency=0) ─────────────────────────────────────────

func (r *Runner) runSerial(_ context.Context) *CompletionState {
	for _, node := range r.graph.Topology() {
		if r.disabled[node.name] {
			r.setState(node, NodeStateVerified, nil)
			continue
		}
		// Skip nodes whose upstreams haven't completed successfully.
		// NOTE: we do NOT short-circuit the entire loop on fatalErr so that
		// independent nodes (no path to the failed node) still run — matching
		// Python oper8 behaviour where independent branches always execute.
		if !r.upstreamsOK(node) {
			r.setState(node, NodeStateUnstarted, nil)
			continue
		}
		r.execNode(node)
	}
	// Mark anything that never ran as Unstarted.
	for _, node := range r.graph.Nodes() {
		r.mu.Lock()
		_, started := r.stateMap[node.name]
		r.mu.Unlock()
		if !started {
			r.setState(node, NodeStateUnstarted, nil)
		}
	}
	return r.buildCompletionState()
}

// ── Concurrent execution ─────────────────────────────────────────────────────

func (r *Runner) runConcurrent(ctx context.Context) *CompletionState {
	all := r.graph.Nodes()
	if len(all) == 0 {
		return r.buildCompletionState()
	}

	// Buffered to len(all): goroutines never block writing results.
	results := make(chan nodeResult, len(all))

	var sem chan struct{} // nil = unbounded concurrency
	if r.cfg.concurrency > 0 {
		sem = make(chan struct{}, r.cfg.concurrency)
	}

	// dispatched tracks nodes that have been sent to a goroutine.
	// Only the scheduler (this goroutine) reads/writes dispatched.
	dispatched := make(map[string]bool)
	// inFlight counts goroutines currently running.
	var inFlight int

	dispatch := func(node *Node) {
		dispatched[node.name] = true
		inFlight++
		go func(n *Node) {
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			var st NodeState
			var herr error
			if r.disabled[n.name] {
				st = NodeStateVerified
			} else {
				st, herr = r.runNodeFunc(n)
			}
			results <- nodeResult{node: n, state: st, err: herr}
		}(node)
	}

	markUnstarted := func() {
		for _, n := range all {
			r.mu.Lock()
			_, done := r.stateMap[n.name]
			r.mu.Unlock()
			if !done {
				r.setState(n, NodeStateUnstarted, nil)
			}
		}
	}

	// Initial dispatch: all nodes whose upstreams are already satisfied
	// (i.e. nodes with no dependencies at all).
	for _, n := range all {
		if r.upstreamsOK(n) {
			dispatch(n)
		}
	}

	// Scheduler loop: runs until every node has a terminal state.
	for {
		// If nothing is in-flight and we can't dispatch anything new,
		// the remaining undispatched nodes are unreachable (their upstreams
		// failed). Mark them Unstarted and exit.
		if inFlight == 0 {
			allDone := true
			for _, n := range all {
				if !dispatched[n.name] {
					allDone = false
					break
				}
			}
			if allDone {
				break
			}
			// Nodes left undispatched with nothing in-flight → upstreams failed.
			markUnstarted()
			break
		}

		select {
		case <-ctx.Done():
			// Drain in-flight results so goroutines can exit.
			for inFlight > 0 {
				<-results
				inFlight--
			}
			markUnstarted()
			return r.buildCompletionState()

		case res := <-results:
			inFlight--
			r.setState(res.node, res.state, res.err)

			if r.fatalErr != nil {
				// Fatal halt: drain in-flight, mark rest unstarted.
				for inFlight > 0 {
					<-results
					inFlight--
				}
				markUnstarted()
				return r.buildCompletionState()
			}

			// After each result, check if previously-blocked nodes are now ready.
			for _, n := range all {
				if !dispatched[n.name] && r.upstreamsOK(n) {
					dispatch(n)
				}
			}
		}
	}
	return r.buildCompletionState()
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// runNodeFunc calls the node's function and translates the error into a
// terminal NodeState.
func (r *Runner) runNodeFunc(node *Node) (NodeState, error) {
	fn := node.Func()
	if fn == nil {
		return NodeStateVerified, nil
	}
	err := fn()
	if err == nil {
		return NodeStateVerified, nil
	}
	var h *HaltError
	if asHaltError(err, &h) {
		if h.Fatal {
			return NodeStateFailed, h
		}
		return NodeStateUnverified, h
	}
	// Any other error is fatal.
	return NodeStateFailed, &HaltError{Fatal: true, Cause: fmt.Errorf("node %q: %w", node.name, err)}
}

// execNode runs a single node synchronously (serial mode).
func (r *Runner) execNode(node *Node) {
	st, herr := r.runNodeFunc(node)
	r.setState(node, st, herr)
}

// setState records the terminal state for a node. Must only be called
// from the scheduler (serial loop or concurrent results consumer) to
// keep stateMap writes single-threaded.
func (r *Runner) setState(node *Node, state NodeState, herr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stateMap[node.name] = state
	if herr != nil && r.fatalErr == nil {
		var h *HaltError
		if asHaltError(herr, &h) && h.Fatal {
			r.fatalErr = h.Cause
		}
	}
}

// upstreamsOK reports whether all upstream dependencies of node have
// completed (and, if cfg.verifyUpstream, their EdgeFuncs pass).
func (r *Runner) upstreamsOK(node *Node) bool {
	for _, e := range node.Children() {
		r.mu.Lock()
		st, done := r.stateMap[e.node.name]
		r.mu.Unlock()

		// Disabled nodes are treated as verified.
		if r.disabled[e.node.name] {
			continue
		}
		if !done || st != NodeStateVerified {
			return false
		}
		if r.cfg.verifyUpstream && e.verify != nil && !e.verify() {
			return false
		}
	}
	return true
}

// buildCompletionState assembles the final [CompletionState] from stateMap.
func (r *Runner) buildCompletionState() *CompletionState {
	cs := &CompletionState{Err: r.fatalErr}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, node := range r.graph.Nodes() {
		switch r.stateMap[node.name] {
		case NodeStateVerified:
			cs.Verified = append(cs.Verified, node)
		case NodeStateUnverified:
			cs.Unverified = append(cs.Unverified, node)
		case NodeStateFailed:
			cs.Failed = append(cs.Failed, node)
		default: // NodeStateUnstarted or missing
			cs.Unstarted = append(cs.Unstarted, node)
		}
	}
	return cs
}
