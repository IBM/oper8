// Package controller defines the Controller interface that operator authors
// implement to wire their Components into the reconcile loop.
//
// Ported from oper8 Python (controller.py).
//
// Design decisions vs Python:
//   - Python Controller was tightly coupled to the reconcile manager and
//     called RolloutManager internally. The Go Controller is a pure
//     interface: it only declares the component graph. The ReconcileManager
//     owns session construction and rollout orchestration.
//   - Python used abstract class properties for group/version/kind. Go uses
//     a separate GVK struct returned by a method — no reflection tricks.
//   - Hook methods (AfterDeploy, AfterVerify, etc.) default to no-ops via
//     the BaseController embed. Authors override only what they need.
//   - controller-runtime coupling lives exclusively in the reconciler adapter
//     (see rewrite/reconciler/), not here. This package has zero imports
//     outside the standard library and oper8-go packages.
package controller

import (
	"context"

	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/session"
)

// GVK identifies the CustomResource kind this controller manages.
type GVK struct {
	Group   string
	Version string
	Kind    string
}

// HookResult carries the outcome of an optional lifecycle hook.
type HookResult struct {
	// OK signals whether the hook completed successfully.
	// False causes the rollout to be treated as not-fully-verified.
	OK bool
	// Err is any error the hook encountered. A non-nil error causes a fatal
	// rollout failure regardless of OK.
	Err error
}

// ok is the zero-cost success sentinel.
var ok = HookResult{OK: true}

// OK returns a successful HookResult. Use this as a no-op hook return value.
func OK() HookResult { return ok }

// Controller is the interface every operator controller must implement.
type Controller interface {
	// GVK returns the group/version/kind of the CR this controller manages.
	GVK() GVK

	// SetupComponents builds the component DAG for this reconcile.
	// Implementations should:
	//   1. Construct their Component values.
	//   2. Call sess.AddComponent for each one.
	//   3. Call sess.AddDependency to declare ordering.
	// Any error aborts the reconcile with a config error.
	SetupComponents(ctx context.Context, sess *session.Session) error

	// FinalizeComponents is called instead of SetupComponents when the CR
	// is being deleted. The default implementation (BaseController) is a no-op.
	FinalizeComponents(ctx context.Context, sess *session.Session) error

	// AfterDeploy is called when the deploy phase completes all nodes
	// successfully. Return OK() to proceed to verify. A non-OK result
	// prevents verification.
	AfterDeploy(ctx context.Context, sess *session.Session, state *dag.CompletionState) HookResult

	// AfterDeployUnsuccessful is called when the deploy phase ends with
	// incomplete or failed nodes.
	AfterDeployUnsuccessful(ctx context.Context, sess *session.Session, failed bool, state *dag.CompletionState) HookResult

	// AfterVerify is called when both deploy and verify phases complete
	// all nodes successfully.
	AfterVerify(ctx context.Context, sess *session.Session, verifyState, deployState *dag.CompletionState) HookResult

	// AfterVerifyUnsuccessful is called when deploy succeeded but verify
	// did not complete.
	AfterVerifyUnsuccessful(ctx context.Context, sess *session.Session, failed bool, verifyState, deployState *dag.CompletionState) HookResult

	// ShouldRequeue decides whether to requeue after a reconcile.
	// The default implementation (BaseController) returns (true, 0) when
	// the CR has not reached a stable verified state.
	ShouldRequeue(ctx context.Context, sess *session.Session) bool

	// HasFinalizer reports whether this controller registers a Kubernetes
	// finalizer. When true, ReconcileManager adds the finalizer on every
	// reconcile and removes it only after FinalizeComponents succeeds.
	HasFinalizer() bool

	// Finalizer returns the finalizer string to register on the CR.
	// Only called when HasFinalizer() returns true.
	Finalizer() string
}

// ── BaseController ────────────────────────────────────────────────────────────

// BaseController provides no-op implementations of every optional Controller
// method. Embed it in your concrete controller to avoid boilerplate.
//
// Usage:
//
//	type MyController struct {
//	    controller.BaseController
//	}
//
//	func (c *MyController) GVK() controller.GVK { ... }
//	func (c *MyController) SetupComponents(...) error { ... }
type BaseController struct{}

// FinalizeComponents is a no-op by default.
func (b *BaseController) FinalizeComponents(_ context.Context, _ *session.Session) error {
	return nil
}

// AfterDeploy is a no-op that returns OK.
func (b *BaseController) AfterDeploy(_ context.Context, _ *session.Session, _ *dag.CompletionState) HookResult {
	return OK()
}

// AfterDeployUnsuccessful is a no-op that returns OK.
func (b *BaseController) AfterDeployUnsuccessful(_ context.Context, _ *session.Session, _ bool, _ *dag.CompletionState) HookResult {
	return OK()
}

// AfterVerify is a no-op that returns OK.
func (b *BaseController) AfterVerify(_ context.Context, _ *session.Session, _, _ *dag.CompletionState) HookResult {
	return OK()
}

// AfterVerifyUnsuccessful is a no-op that returns OK.
func (b *BaseController) AfterVerifyUnsuccessful(_ context.Context, _ *session.Session, _ bool, _, _ *dag.CompletionState) HookResult {
	return OK()
}

// ShouldRequeue defaults to false.
// ReconcileManager already requeues when VerifyCompleted() is false;
// override this method only when additional requeue logic is needed
// (e.g. a periodic health-check interval).
func (b *BaseController) ShouldRequeue(_ context.Context, _ *session.Session) bool {
	return false
}

// HasFinalizer returns false — no finalizer by default.
func (b *BaseController) HasFinalizer() bool { return false }

// Finalizer returns an empty string when HasFinalizer is false.
func (b *BaseController) Finalizer() string { return "" }
