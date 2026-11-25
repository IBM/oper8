// Package patch implements resource patching for the TemporaryPatch system.
//
// Ported from oper8 Python (patch.py, patch_strategic_merge.py).
//
// Two patch types are supported:
//
//   - StrategicMergePatch — Kubernetes-style merge that understands list merge
//     keys and $patch directives. Uses k8s.io/apimachinery's built-in SMP.
//
//   - JSONPatch6902 — RFC 6902 JSON Patch. Uses github.com/evanphx/json-patch.
//
// Usage:
//
//	patched, err := patch.Apply("my-component.container", manifest, patches)
package patch

import (
	"encoding/json"
	"fmt"
	"strings"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// PatchType identifies the kind of patch to apply.
type PatchType string

const (
	// StrategicMergePatch uses Kubernetes Strategic Merge Patch semantics.
	StrategicMergePatch PatchType = "patchStrategicMerge"

	// JSONPatch6902 uses RFC 6902 JSON Patch semantics.
	JSONPatch6902 PatchType = "patchJson6902"
)

// PatchEntry is a single patch to be applied. It mirrors the spec structure
// of a TemporaryPatch CR.
type PatchEntry struct {
	// Name is the unique name of this patch (metadata.name of the patch CR).
	Name string

	// Type is the patch format.
	Type PatchType

	// Patch is the patch payload.
	//   - For StrategicMergePatch: a map[string]any representing the merge document.
	//   - For JSONPatch6902: a []any representing the RFC 6902 operation list.
	Patch any
}

// Apply applies all patches in entries to resourceDef whose operator-internal
// name is internalName. Patches are applied in-order; only entries whose
// nested key path matches internalName are applied.
//
// internalName uses dot-separated notation (e.g. "deployment.container").
// The patch entry's Patch field may be a nested map keyed by the components
// of internalName — the function walks into it to find the matching patch.
//
// Returns the (potentially modified) resource definition.
// Mirrors Python's apply_patches.
func Apply(internalName string, resourceDef map[string]any, entries []PatchEntry) (map[string]any, error) {
	result := deepCopyMap(resourceDef)

	for _, entry := range entries {
		patchPayload := resolvePatchPayload(internalName, entry.Patch)
		if patchPayload == nil {
			continue
		}

		var err error
		switch entry.Type {
		case StrategicMergePatch:
			patchMap, ok := patchPayload.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("patch %q: StrategicMergePatch payload must be a map", entry.Name)
			}
			result, err = applyStrategicMerge(result, patchMap)
		case JSONPatch6902:
			result, err = applyJSON6902(result, patchPayload)
		default:
			return nil, fmt.Errorf("patch %q: unsupported patch type %q", entry.Name, entry.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("patch %q: %w", entry.Name, err)
		}
	}
	return result, nil
}

// ── Strategic Merge Patch ─────────────────────────────────────────────────────

// knownTypes are registered so that SMP can resolve merge keys for their
// fields (e.g. containers keyed by name). Add more types here as needed.
var knownTypes = map[string]any{
	"Deployment":  &appsv1.Deployment{},
	"StatefulSet": &appsv1.StatefulSet{},
	"DaemonSet":   &appsv1.DaemonSet{},
	"Pod":         &corev1.Pod{},
	"ReplicaSet":  &appsv1.ReplicaSet{},
}

func applyStrategicMerge(base, patch map[string]any) (map[string]any, error) {
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("marshal base: %w", err)
	}
	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("marshal patch: %w", err)
	}

	// Look up a typed schema object so SMP can resolve list merge keys.
	kind, _ := base["kind"].(string)
	schema := knownTypes[kind] // nil is fine; SMP falls back to JSON merge

	var merged []byte
	if schema != nil {
		merged, err = strategicpatch.StrategicMergePatch(baseJSON, patchJSON, schema)
	} else {
		// Fallback: JSON merge patch (RFC 7396) for unknown kinds.
		merged, err = jsonpatch.MergePatch(baseJSON, patchJSON)
	}
	if err != nil {
		return nil, fmt.Errorf("strategic merge: %w", err)
	}

	result := make(map[string]any)
	if err := json.Unmarshal(merged, &result); err != nil {
		return nil, fmt.Errorf("unmarshal merged: %w", err)
	}
	return result, nil
}

// ── JSON Patch 6902 ───────────────────────────────────────────────────────────

func applyJSON6902(base map[string]any, rawPatch any) (map[string]any, error) {
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("marshal base: %w", err)
	}
	patchJSON, err := json.Marshal(rawPatch)
	if err != nil {
		return nil, fmt.Errorf("marshal patch: %w", err)
	}

	p, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		return nil, fmt.Errorf("decode JSON patch: %w", err)
	}
	merged, err := p.Apply(baseJSON)
	if err != nil {
		return nil, fmt.Errorf("apply JSON patch: %w", err)
	}

	result := make(map[string]any)
	if err := json.Unmarshal(merged, &result); err != nil {
		return nil, fmt.Errorf("unmarshal patched: %w", err)
	}
	return result, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

// resolvePatchPayload walks into a nested patch map following the dot-separated
// parts of internalName, returning the leaf patch value (or nil if no match).
// Mirrors the name_parts traversal in Python's apply_patches.
//
// Routing rules (match Python apply_patches):
//   - If rawPatch is a map keyed by the first segment of internalName, descend.
//   - If rawPatch is a non-map (e.g. []any for JSON-6902 ops), return it directly.
//   - If rawPatch is a map but does NOT contain the first segment of internalName
//     as a key, return nil — this patch does not apply to this component.
func resolvePatchPayload(internalName string, rawPatch any) any {
	if rawPatch == nil {
		return nil
	}
	parts := strings.Split(internalName, ".")
	current := rawPatch
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			// Non-map value (e.g. RFC-6902 []any ops) — this is the leaf payload.
			return current
		}
		val, exists := m[part]
		if !exists {
			// Routing key absent: this patch entry does not target this component.
			return nil
		}
		current = val
	}
	return current
}

func deepCopyMap(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	out := make(map[string]any)
	_ = json.Unmarshal(b, &out)
	return out
}
