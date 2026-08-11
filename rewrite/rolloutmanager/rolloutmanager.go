// Package rolloutmanager executes the four-phase oper8 rollout loop for
// a fully-populated session.
//
// Ported from oper8 Python (rollout_manager.py).
//
// Four phases:
//  1. Deploy Graph  — render + apply each component in DAG order
//  2. After Deploy  — optional controller hook
//  3. Verify Graph  — verify each deployed component in DAG order
//  4. After Verify  — optional controller hook
//
// Design decisions vs Python:
//   - Python used a ThreadPoolExecutor + sleep-poll loop. Go uses Runner
//     goroutines with context.Context cancellation (already in dag.Runner).
//   - Python wrapped component functions in run_node which raised DagHaltError.
//     Go's NodeFunc returns (*dag.HaltError) directly — no wrapping needed.
//   - The Python RolloutManager stored session + 4 callback funcs.
//     Go's RolloutManager receives the Controller (which carries the hooks)
//     so the caller does not need to extract callbacks manually.
//   - Disabled components are not a first-class feature in this port;
//     operators control which components are registered via SetupComponents.
package rolloutmanager

import (
	"context"
	"fmt"

	"github.com/example/oper8-go/component"
	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/session"
)

// RolloutManager executes the 4-phase rollout for a session.
type RolloutManager struct {
	sess *session.Session
	ctrl controller.Controller
	conc int // Runner concurrency; 0 = serial
}

// New creates a RolloutManager.
// concurrency=0 runs the DAG serially (useful for tests and dry-runs).
func New(sess *session.Session, ctrl controller.Controller, concurrency int) *RolloutManager {
	return &RolloutManager{sess: sess, ctrl: ctrl, conc: concurrency}
}

// Rollout executes all four phases and returns the final CompletionState.
// ctx cancellation propagates into both Runner executions.
func (rm *RolloutManager) Rollout(ctx context.Context) *dag.CompletionState {
	components := rm.componentsByName()

	///////////////////////////////////////////////////////////////////////////
	// Phase 1: Deploy graph
	///////////////////////////////////////////////////////////////////////////
	deployRunner := rm.buildDeployRunner(ctx, components)
	deployState := deployRunner.Run(ctx)

	phase1Complete := deployState.VerifyCompleted()
	phase1Failed := deployState.AnyFailed()

	///////////////////////////////////////////////////////////////////////////
	// Phase 2: After deploy hooks
	///////////////////////////////////////////////////////////////////////////
	var phase2Err error

	if !phase1Complete || phase1Failed {
		if res := rm.ctrl.AfterDeployUnsuccessful(ctx, rm.sess, phase1Failed, deployState); !res.OK || res.Err != nil {
			phase2Err = firstErr(res.Err, fmt.Errorf("rollout: after-deploy-unsuccessful hook returned not-OK"))
		}
	} else {
		if res := rm.ctrl.AfterDeploy(ctx, rm.sess, deployState); !res.OK || res.Err != nil {
			phase2Err = firstErr(res.Err, fmt.Errorf("rollout: after-deploy hook returned not-OK"))
		}
	}

	phase2Complete := phase1Complete && phase2Err == nil

	///////////////////////////////////////////////////////////////////////////
	// Phase 3: Verify graph
	///////////////////////////////////////////////////////////////////////////
	var verifyState *dag.CompletionState

	if !phase1Failed {
		verifyRunner := rm.buildVerifyRunner(ctx, components, deployState)
		verifyState = verifyRunner.Run(ctx)
	} else {
		verifyState = &dag.CompletionState{}
	}

	phase3Complete := verifyState.VerifyCompleted() && phase1Complete && phase2Complete
	phase3Failed := verifyState.AnyFailed()

	///////////////////////////////////////////////////////////////////////////
	// Phase 4: After verify hooks
	///////////////////////////////////////////////////////////////////////////
	var phase4Err error

	if phase1Complete && phase2Complete && !phase3Complete {
		if res := rm.ctrl.AfterVerifyUnsuccessful(ctx, rm.sess, phase3Failed, verifyState, deployState); !res.OK || res.Err != nil {
			phase4Err = firstErr(res.Err, fmt.Errorf("rollout: after-verify-unsuccessful hook returned not-OK"))
		}
	} else if phase3Complete {
		if res := rm.ctrl.AfterVerify(ctx, rm.sess, verifyState, deployState); !res.OK || res.Err != nil {
			phase4Err = firstErr(res.Err, fmt.Errorf("rollout: after-verify hook returned not-OK"))
		}
	}

	///////////////////////////////////////////////////////////////////////////
	// Assemble final CompletionState
	///////////////////////////////////////////////////////////////////////////
	// Verified = nodes that made it all the way through verify.
	// Unverified = deployed successfully but not verified (or not run in verify).
	// Failed = failed in either graph.
	// Unstarted = never attempted in deploy graph.
	verifiedSet := nodeSet(verifyState.Verified)
	failedSet := mergeNodeSets(nodeSet(verifyState.Failed), nodeSet(deployState.Failed))

	deployedAll := append(deployState.Verified, deployState.Unverified...)
	unverifiedNodes := []*dag.Node{}
	for _, n := range deployedAll {
		if !verifiedSet[n.Name()] && !failedSet[n.Name()] {
			unverifiedNodes = append(unverifiedNodes, n)
		}
	}
	for _, n := range verifyState.Unverified {
		if !verifiedSet[n.Name()] && !failedSet[n.Name()] {
			unverifiedNodes = append(unverifiedNodes, n)
		}
	}

	finalErr := firstErr(
		deployState.Err,
		phase2Err,
		verifyState.Err,
		phase4Err,
	)

	return &dag.CompletionState{
		Verified:   verifyState.Verified,
		Unverified: unverifiedNodes,
		Failed:     failedNodes(failedSet, rm.sess),
		Unstarted:  deployState.Unstarted,
		Err:        finalErr,
	}
}

// ── Runner builders ───────────────────────────────────────────────────────────

func (rm *RolloutManager) buildDeployRunner(ctx context.Context, comps map[string]component.Component) *dag.Runner {
	opts := []dag.RunnerOption{dag.WithVerifyUpstream(true)}
	if rm.conc > 0 {
		opts = append(opts, dag.WithConcurrency(rm.conc))
	}

	runner := dag.NewRunner(rm.sess.Graph, opts...)

	// Wire each DAG node to run its component's Setup+Deploy.
	for _, n := range rm.sess.Graph.Nodes() {
		comp, ok := comps[n.Name()]
		if !ok {
			continue
		}
		// Capture loop variables.
		capturedComp := comp
		capturedCtx := ctx
		n.SetFunc(func() error {
			if err := capturedComp.Setup(capturedCtx, rm.sess); err != nil {
				return &dag.HaltError{Fatal: true, Cause: err}
			}
			if err := capturedComp.Deploy(capturedCtx, rm.sess); err != nil {
				return &dag.HaltError{Fatal: true, Cause: err}
			}
			return nil
		})
	}
	return runner
}

func (rm *RolloutManager) buildVerifyRunner(ctx context.Context, comps map[string]component.Component, deployState *dag.CompletionState) *dag.Runner {
	opts := []dag.RunnerOption{dag.WithVerifyUpstream(true)}
	if rm.conc > 0 {
		opts = append(opts, dag.WithConcurrency(rm.conc))
	}

	runner := dag.NewRunner(rm.sess.Graph, opts...)

	// Only run verify for nodes that were actually deployed.
	deployed := nodeSet(append(deployState.Verified, deployState.Unverified...))
	for _, n := range rm.sess.Graph.Nodes() {
		if !deployed[n.Name()] {
			runner.DisableNode(n.Name())
			continue
		}
		comp, ok := comps[n.Name()]
		if !ok {
			runner.DisableNode(n.Name())
			continue
		}
		capturedComp := comp
		capturedCtx := ctx
		n.SetFunc(func() error {
			if ready := capturedComp.Verify(capturedCtx, rm.sess); !ready {
				// Non-fatal: node goes into Unverified, not Failed.
				return &dag.HaltError{Fatal: false, Cause: fmt.Errorf("component %q not yet verified", capturedComp.Name())}
			}
			return nil
		})
	}
	return runner
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// componentsByName builds a map from DAG node name → Component using the
// components registered on the session graph.
// Components must be stored on nodes via node.SetData (see reconcilemanager).
func (rm *RolloutManager) componentsByName() map[string]component.Component {
	out := make(map[string]component.Component)
	for _, n := range rm.sess.Graph.Nodes() {
		if comp, ok := n.Data().(component.Component); ok {
			out[n.Name()] = comp
		}
	}
	return out
}

func nodeSet(nodes []*dag.Node) map[string]bool {
	m := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		m[n.Name()] = true
	}
	return m
}

func mergeNodeSets(a, b map[string]bool) map[string]bool {
	out := make(map[string]bool, len(a)+len(b))
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}

// failedNodes reconstructs the []*dag.Node slice from the failed set using
// the session graph so the final CompletionState has real node pointers.
func failedNodes(failedSet map[string]bool, sess *session.Session) []*dag.Node {
	var out []*dag.Node
	for _, n := range sess.Graph.Nodes() {
		if failedSet[n.Name()] {
			out = append(out, n)
		}
	}
	return out
}

// firstErr returns the first non-nil error from the provided list.
func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
