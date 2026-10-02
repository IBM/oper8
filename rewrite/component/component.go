// Package component defines the Component interface that operator authors
// implement to describe the Kubernetes resources managed by their operator.
//
// Ported from oper8 Python (component.py).
//
// Design decisions vs Python:
//   - Python Component was an ABC with a required class-level `name` property
//     and a large base class that handled rendering, patching, and deploys.
//     Go uses a minimal interface: authors implement only what they need.
//   - Rendering (build_chart / render_chart) and patching logic from the
//     Python base class are intentionally omitted — they live in the operator's
//     own Setup implementation, keeping the base interface slim.
//   - The Python constructor auto-registered the component into the session.
//     In Go, registration is explicit: operators call session.AddComponent
//     inside SetupComponents, not inside the component itself.
package component

import (
	"context"

	"github.com/example/oper8-go/session"
)

// Component is the interface every operator component must implement.
//
// Lifecycle called by RolloutManager (in order):
//
//  1. Setup   — build resource manifests and store them on the component.
//  2. Deploy  — apply the manifests to the cluster via session.DeployManager.
//  3. Verify  — check whether the deployed resources are ready.
type Component interface {
	// Name returns the unique name of this component within a reconcile.
	// It is used as the DAG node name.
	Name() string

	// Disabled reports whether this component should be skipped entirely.
	// When true, Setup and Deploy are not called; the component is treated
	// as immediately verified (counts as a no-op success in the DAG).
	// Mirrors Python oper8's disable_component behaviour.
	Disabled() bool

	// Setup is called once before the deploy phase. Implementations should
	// build their Kubernetes resource manifests here. Any error stops the
	// rollout for this component (treated as a fatal HaltError).
	Setup(ctx context.Context, sess *session.Session) error

	// Deploy applies the resources built in Setup to the cluster.
	// Returns an error if any apply operation fails.
	Deploy(ctx context.Context, sess *session.Session) error

	// Verify checks whether the deployed resources are in their desired state.
	// Returns true when fully ready, false when still waiting.
	// Errors during verification are treated as not-ready (false).
	Verify(ctx context.Context, sess *session.Session) bool
}
