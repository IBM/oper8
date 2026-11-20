// Package controller defines the Controller interface that operator authors
// implement to wire their Components into the reconcile loop.
//
// Ported from oper8 Python (controller.py).
//
// Design decisions vs Python:
//   - Python Controller drove reconciliation directly. The Go Controller is a
//     pure interface: it only declares the component graph and optional hooks.
//     ReconcileManager owns session construction and rollout orchestration.
//   - Hook methods default to no-ops via the BaseController embed.
//   - controller-runtime coupling lives exclusively in a future reconciler
//     adapter package — zero controller-runtime imports here.
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

// HookResult is returned by optional lifecycle hooks.
type HookResult struct {
	// OK signals whether the hook completed successfully.
	OK bool
	// Err is any error the hook encountered (causes fatal rollout failure).
	Err error
}

// OK returns a successful HookResult.
func OK() HookResult { return HookResult{OK: true} }

// Controller is the interface every operator controller must implement.
type Controller interface {
	// GVK returns the group/version/kind of the CR this controller manages.
	GVK() GVK

	// SetupComponents builds the component DAG for this reconcile.
	SetupComponents(ctx context.Context, sess *session.Session) error

	// FinalizeComponents is called instead of SetupComponents when the CR is
	// being deleted. Default (BaseController) is a no-op.
	FinalizeComponents(ctx context.Context, sess *session.Session) error

	// AfterDeploy is called when all nodes deploy successfully.
	AfterDeploy(ctx context.Context, sess *session.Session, state *dag.CompletionState) HookResult

	// AfterDeployUnsuccessful is called when deploy ends with failures.
	AfterDeployUnsuccessful(ctx context.Context, sess *session.Session, failed bool, state *dag.CompletionState) HookResult

	// AfterVerify is called when deploy and verify both complete fully.
	AfterVerify(ctx context.Context, sess *session.Session, verifyState, deployState *dag.CompletionState) HookResult

	// AfterVerifyUnsuccessful is called when deploy succeeded but verify did not.
	AfterVerifyUnsuccessful(ctx context.Context, sess *session.Session, failed bool, verifyState, deployState *dag.CompletionState) HookResult

	// ShouldRequeue decides whether to re-enqueue after a reconcile.
	ShouldRequeue(ctx context.Context, sess *session.Session) bool

	// HasFinalizer reports whether this controller registers a finalizer.
	HasFinalizer() bool

	// Finalizer returns the finalizer string. Only called when HasFinalizer is true.
	Finalizer() string
}

// ── BaseController ────────────────────────────────────────────────────────────

// BaseController provides no-op implementations of every optional Controller
// method. Embed it in your concrete controller to avoid boilerplate.
//
//	type MyController struct {
//	    controller.BaseController
//	}
//
//	func (c *MyController) GVK() controller.GVK { ... }
//	func (c *MyController) SetupComponents(...) error { ... }
type BaseController struct{}

// FinalizeComponents is a no-op.
func (b *BaseController) FinalizeComponents(_ context.Context, _ *session.Session) error {
	return nil
}

// AfterDeploy returns OK.
func (b *BaseController) AfterDeploy(_ context.Context, _ *session.Session, _ *dag.CompletionState) HookResult {
	return OK()
}

// AfterDeployUnsuccessful returns OK.
func (b *BaseController) AfterDeployUnsuccessful(_ context.Context, _ *session.Session, _ bool, _ *dag.CompletionState) HookResult {
	return OK()
}

// AfterVerify returns OK.
func (b *BaseController) AfterVerify(_ context.Context, _ *session.Session, _, _ *dag.CompletionState) HookResult {
	return OK()
}

// AfterVerifyUnsuccessful returns OK.
func (b *BaseController) AfterVerifyUnsuccessful(_ context.Context, _ *session.Session, _ bool, _, _ *dag.CompletionState) HookResult {
	return OK()
}

// ShouldRequeue returns true by default (requeue until explicitly stable).
func (b *BaseController) ShouldRequeue(_ context.Context, _ *session.Session) bool { return true }

// HasFinalizer returns false.
func (b *BaseController) HasFinalizer() bool { return false }

// Finalizer returns an empty string.
func (b *BaseController) Finalizer() string { return "" }
