// Package session holds the per-reconciliation context passed through every
// phase of an oper8-go rollout.
//
// Ported from oper8 Python (session.py).
//
// Design decisions vs Python:
//   - Session is a plain struct; no constructor side-effects.
//   - Python aconfig.Config dot-access is replaced by map[string]any.
//   - Session is created once per Reconcile call and discarded afterwards.
//   - Components self-register via session.AddComponent; they do NOT
//     auto-register in their own constructor (avoids Python's __init__
//     side-effect pattern).
package session

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/status"
)

const maxNameLen = 63

// Session is the core context object for a single reconciliation pass.
// It is created by ReconcileManager and handed to the Controller's
// SetupComponents, then to every phase of the RolloutManager.
type Session struct {
	// ID is a unique identifier for this reconciliation.
	ID string

	// CRManifest is the full, unmodified CR that triggered this reconcile.
	CRManifest map[string]any

	// DeployManager is the cluster interaction layer for this reconcile.
	DeployManager deploymanager.DeployManager

	// Graph holds the component DAG built during SetupComponents.
	// Starts empty; populated by AddComponent / AddDependency.
	Graph *dag.Graph

	// Status is the CR's .status block fetched at Session construction.
	// May be empty when the CR has never been reconciled.
	Status map[string]any

	// CurrentVersion is versions.reconciled from Status at construction time.
	CurrentVersion string
}

// New creates a Session for the given CR manifest.
// It fetches the current .status from the cluster so components can read it.
func New(ctx context.Context, id string, crManifest map[string]any, dm deploymanager.DeployManager) (*Session, error) {
	if err := validateCR(crManifest); err != nil {
		return nil, err
	}

	apiVersion, _ := crManifest["apiVersion"].(string)
	kind, _ := crManifest["kind"].(string)
	meta, _ := crManifest["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	namespace, _ := meta["namespace"].(string)

	obj, err := dm.Get(ctx, apiVersion, kind, name, namespace)
	if err != nil {
		return nil, fmt.Errorf("session: failed to fetch current status: %w", err)
	}
	var currentStatus map[string]any
	if obj != nil {
		if s, ok := obj["status"].(map[string]any); ok {
			currentStatus = s
		}
	}
	if currentStatus == nil {
		currentStatus = make(map[string]any)
	}

	return &Session{
		ID:             id,
		CRManifest:     crManifest,
		DeployManager:  dm,
		Graph:          dag.NewGraph(),
		Status:         currentStatus,
		CurrentVersion: status.GetVersion(currentStatus),
	}, nil
}

// ── CR field accessors ────────────────────────────────────────────────────────

// Namespace returns metadata.namespace from the CR.
func (s *Session) Namespace() string {
	meta, _ := s.CRManifest["metadata"].(map[string]any)
	ns, _ := meta["namespace"].(string)
	return ns
}

// Name returns metadata.name from the CR.
func (s *Session) Name() string {
	meta, _ := s.CRManifest["metadata"].(map[string]any)
	n, _ := meta["name"].(string)
	return n
}

// Kind returns the CR kind.
func (s *Session) Kind() string {
	k, _ := s.CRManifest["kind"].(string)
	return k
}

// APIVersion returns the CR apiVersion.
func (s *Session) APIVersion() string {
	av, _ := s.CRManifest["apiVersion"].(string)
	return av
}

// Version returns spec.version from the CR, empty string if absent.
func (s *Session) Version() string {
	spec, _ := s.CRManifest["spec"].(map[string]any)
	v, _ := spec["version"].(string)
	return v
}

// ── Component DAG helpers ─────────────────────────────────────────────────────

// AddComponent registers a node in the session's component DAG.
// Returns an error if a node with that name already exists.
func (s *Session) AddComponent(n *dag.Node) error {
	return s.Graph.AddNode(n)
}

// AddDependency declares that parent must wait for child (child deploys first).
// Both nodes must already be in the graph. verify may be nil.
func (s *Session) AddDependency(parent, child *dag.Node, verify dag.EdgeFunc) error {
	return s.Graph.AddDependency(parent, child, verify)
}

// ── Name utilities ────────────────────────────────────────────────────────────

// ScopedName returns "<cr-name>-<name>", truncated to 63 chars.
func (s *Session) ScopedName(name string) string {
	return TruncateName(fmt.Sprintf("%s-%s", s.Name(), name))
}

// TruncateName truncates name to 63 chars, appending a 4-char SHA256 suffix
// when truncation occurs so the result remains unique.
func TruncateName(name string) string {
	if len(name) <= maxNameLen {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return name[:maxNameLen-4] + fmt.Sprintf("%x", sum[:2])
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func validateCR(cr map[string]any) error {
	for _, key := range []string{"kind", "apiVersion"} {
		if v, _ := cr[key].(string); v == "" {
			return fmt.Errorf("session: CR missing required field %q", key)
		}
	}
	meta, ok := cr["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("session: CR missing 'metadata'")
	}
	for _, key := range []string{"name", "namespace"} {
		if v, _ := meta[key].(string); v == "" {
			return fmt.Errorf("session: CR metadata missing required field %q", key)
		}
	}
	return nil
}
