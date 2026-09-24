// Package component defines the Component interface that operator authors
// implement to describe the Kubernetes resources managed by their operator.
//
// Ported from oper8 Python (component.py).
//
// Design decisions vs Python:
//   - Python Component was an ABC with a large base class handling rendering,
//     patching, and deploy. Go uses a minimal three-method interface only.
//   - The Python constructor auto-registered the component into the session.
//     In Go, registration is explicit: operators call sess.AddComponent inside
//     SetupComponents, not inside the component itself.
package component

import (
	"context"

	"github.com/example/oper8-go/session"
)

// Component is the interface every operator component must implement.
//
// Lifecycle (called by RolloutManager in order):
//  1. Setup   — build resource manifests; called once before deploy phase.
//  2. Deploy  — apply the manifests to the cluster.
//  3. Verify  — check whether deployed resources are ready.
type Component interface {
	// Name returns the unique name of this component within a reconcile.
	// Used as the DAG node name.
	Name() string

	// Setup builds Kubernetes resource manifests for this component.
	// Any non-nil error stops the rollout for this component (fatal HaltError).
	Setup(ctx context.Context, sess *session.Session) error

	// Deploy applies the resources built in Setup to the cluster.
	// Returns a non-nil error if any apply operation fails.
	Deploy(ctx context.Context, sess *session.Session) error

	// Verify checks whether the deployed resources are in their desired state.
	// Returns true when fully ready, false when still waiting.
	Verify(ctx context.Context, sess *session.Session) bool
}
