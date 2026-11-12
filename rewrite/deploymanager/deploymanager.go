// Package deploymanager defines the interface for all cluster interactions
// performed by oper8-go components.
//
// Ported from oper8 Python (deploy_manager/base.py, kube_event.py).
//
// Design decisions vs Python:
//   - Python returned (success bool, changed bool) tuples. Go uses (changed bool, error)
//     so callers use the idiomatic if err != nil pattern rather than checking two booleans.
//   - watch_objects was a generator (Iterator); in Go it returns a channel so the
//     caller can range over events and cancel via context.
//   - Abstract base class → Go interface. No base struct; every method is required.
package deploymanager

import (
	"context"
	"time"
)

// DeployMethod controls how a resource manifest is applied to the cluster.
type DeployMethod string

const (
	// DeployMethodDefault uses server-side apply (recommended; idempotent).
	DeployMethodDefault DeployMethod = "default"

	// DeployMethodUpdate does a full PUT using the current resourceVersion.
	DeployMethodUpdate DeployMethod = "update"

	// DeployMethodReplace deletes the existing resource then re-creates it.
	DeployMethodReplace DeployMethod = "replace"
)

// EventType mirrors the Kubernetes watch event types.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

// WatchEvent is a single event emitted by the cluster watch stream.
type WatchEvent struct {
	Type      EventType
	Object    map[string]any
	Timestamp time.Time
}

// ListOptions filters a List or Watch call.
type ListOptions struct {
	LabelSelector string
	FieldSelector string
	// ResourceVersion restricts results to objects newer than this version.
	ResourceVersion string
}

// DeployManager is the single interface through which oper8-go components
// interact with a Kubernetes cluster.
//
// All methods accept a context.Context so callers can propagate cancellation
// and deadlines. Implementations must honour context cancellation.
type DeployManager interface {
	// Deploy ensures the given resource manifests exist in the cluster in the
	// desired state. It returns whether any resource was created or updated,
	// and any error encountered.
	//
	// manageOwnerRefs: when true the implementation should stamp the owning
	// CR's ownerReference onto each resource before applying.
	Deploy(ctx context.Context, resources []map[string]any, method DeployMethod, manageOwnerRefs bool) (changed bool, err error)

	// Delete removes the given resources from the cluster. Returns whether
	// any resource was actually deleted.
	Delete(ctx context.Context, resources []map[string]any) (changed bool, err error)

	// Get fetches a single resource by group/version/kind, name, and namespace.
	// Returns (nil, nil) when the resource does not exist.
	Get(ctx context.Context, apiVersion, kind, name, namespace string) (map[string]any, error)

	// List returns all resources matching the given group/version/kind and
	// optional ListOptions filters.
	List(ctx context.Context, apiVersion, kind, namespace string, opts ListOptions) ([]map[string]any, error)

	// Watch opens a streaming watch on resources matching the given
	// group/version/kind. Events are sent to the returned channel until ctx
	// is cancelled, at which point the channel is closed.
	Watch(ctx context.Context, apiVersion, kind, namespace string, opts ListOptions) (<-chan WatchEvent, error)

	// SetStatus patches only the status sub-resource of a named object.
	// Returns whether the status actually changed.
	SetStatus(ctx context.Context, apiVersion, kind, name, namespace string, status map[string]any) (changed bool, err error)
}
