package patch_test

import (
	"testing"

	"github.com/example/oper8-go/patch"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func deployment(name string) map[string]any {
	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"replicas": float64(1),
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "app", "image": "old:1.0"},
					},
				},
			},
		},
	}
}

func configmap(name string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name},
		"data":       map[string]any{"key": "original"},
	}
}

// ── StrategicMergePatch ───────────────────────────────────────────────────────

func TestApply_StrategicMerge_ScalarOverride(t *testing.T) {
	t.Parallel()
	// Patch is routed under "mycomp" — resolvePatchPayload descends into it.
	entries := []patch.PatchEntry{{
		Name: "scale-patch",
		Type: patch.StrategicMergePatch,
		Patch: map[string]any{
			"mycomp": map[string]any{"spec": map[string]any{"replicas": float64(3)}},
		},
	}}
	got, err := patch.Apply("mycomp", deployment("d1"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	spec := got["spec"].(map[string]any)
	if spec["replicas"] != float64(3) {
		t.Errorf("expected replicas=3, got %v", spec["replicas"])
	}
}

func TestApply_StrategicMerge_FallbackForUnknownKind(t *testing.T) {
	t.Parallel()
	base := configmap("cm1")
	// Patch routed under "cm-component".
	entries := []patch.PatchEntry{{
		Name: "data-patch",
		Type: patch.StrategicMergePatch,
		Patch: map[string]any{
			"cm-component": map[string]any{"data": map[string]any{"key": "patched", "new": "val"}},
		},
	}}
	got, err := patch.Apply("cm-component", base, entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data := got["data"].(map[string]any)
	if data["key"] != "patched" || data["new"] != "val" {
		t.Errorf("unexpected data: %v", data)
	}
}

// ── JSON Patch 6902 ───────────────────────────────────────────────────────────

func TestApply_JSON6902_ReplaceOp(t *testing.T) {
	t.Parallel()
	// JSON-6902 patch payload is a []any, not a map, so resolvePatchPayload
	// returns it directly regardless of internalName.
	entries := []patch.PatchEntry{{
		Name: "json-patch",
		Type: patch.JSONPatch6902,
		Patch: []any{
			map[string]any{"op": "replace", "path": "/data/key", "value": "replaced"},
		},
	}}
	got, err := patch.Apply("cm-component", configmap("cm2"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data := got["data"].(map[string]any)
	if data["key"] != "replaced" {
		t.Errorf("expected replaced, got %v", data["key"])
	}
}

func TestApply_JSON6902_AddOp(t *testing.T) {
	t.Parallel()
	entries := []patch.PatchEntry{{
		Name: "add-patch",
		Type: patch.JSONPatch6902,
		Patch: []any{
			map[string]any{"op": "add", "path": "/data/newkey", "value": "newval"},
		},
	}}
	got, err := patch.Apply("comp", configmap("cm3"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got["data"].(map[string]any)["newkey"] != "newval" {
		t.Errorf("add op failed: %v", got["data"])
	}
}

// ── Routing by internalName ───────────────────────────────────────────────────

func TestApply_RoutedByInternalName_Match(t *testing.T) {
	t.Parallel()
	// Patch nested under "comp" key — applied only when internalName == "comp".
	entries := []patch.PatchEntry{{
		Name: "routed",
		Type: patch.StrategicMergePatch,
		Patch: map[string]any{
			"comp": map[string]any{"data": map[string]any{"key": "routed"}},
		},
	}}
	got, err := patch.Apply("comp", configmap("cm4"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got["data"].(map[string]any)["key"] != "routed" {
		t.Errorf("routed patch not applied: %v", got["data"])
	}
}

func TestApply_RoutedByInternalName_NoMatch(t *testing.T) {
	t.Parallel()
	// Patch is nested under "other" — should NOT be applied when internalName == "comp"
	entries := []patch.PatchEntry{{
		Name: "no-match",
		Type: patch.StrategicMergePatch,
		Patch: map[string]any{
			"other": map[string]any{"data": map[string]any{"key": "should-not-apply"}},
		},
	}}
	got, err := patch.Apply("comp", configmap("cm5"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got["data"].(map[string]any)["key"] != "original" {
		t.Errorf("patch should not have been applied: %v", got["data"])
	}
}

func TestApply_NoEntries(t *testing.T) {
	t.Parallel()
	base := configmap("cm6")
	got, err := patch.Apply("comp", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got["data"].(map[string]any)["key"] != "original" {
		t.Errorf("empty entries changed the map: %v", got)
	}
}

func TestApply_UnsupportedPatchType(t *testing.T) {
	t.Parallel()
	// Patch must be non-nil and resolve to a non-nil payload so that the
	// type-switch is actually reached. A direct (non-nested) payload works.
	entries := []patch.PatchEntry{{
		Name:  "bad",
		Type:  patch.PatchType("unknownType"),
		Patch: map[string]any{"data": map[string]any{"key": "x"}}, // direct payload — not nested under "comp"
	}}
	// The patch payload resolves to the whole map (since "comp" key doesn't
	// exist, resolvePatchPayload returns the map itself at the top level).
	// Use a single-part name so the resolver returns the raw payload.
	_, err := patch.Apply("data", configmap("cm7"), entries)
	if err == nil {
		t.Fatal("expected error for unsupported patch type")
	}
}

// ── Additional routing and edge-case tests ────────────────────────────────────

func TestApply_MultipleEntries_AllApplied(t *testing.T) {
	t.Parallel()
	// Two sequential patches to the same component — both must apply.
	entries := []patch.PatchEntry{
		{
			Name: "add-first",
			Type: patch.JSONPatch6902,
			Patch: []any{
				map[string]any{"op": "add", "path": "/data/k1", "value": "v1"},
			},
		},
		{
			Name: "add-second",
			Type: patch.JSONPatch6902,
			Patch: []any{
				map[string]any{"op": "add", "path": "/data/k2", "value": "v2"},
			},
		},
	}
	got, err := patch.Apply("comp", configmap("cm-multi"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data := got["data"].(map[string]any)
	if data["k1"] != "v1" || data["k2"] != "v2" {
		t.Errorf("not all patches applied: %v", data)
	}
}

func TestApply_RoutedDotted_DeeplyNested(t *testing.T) {
	t.Parallel()
	// internalName "a.b" — patch must be nested under ["a"]["b"].
	entries := []patch.PatchEntry{{
		Name: "deep",
		Type: patch.JSONPatch6902,
		Patch: map[string]any{
			"a": map[string]any{
				"b": []any{
					map[string]any{"op": "replace", "path": "/data/key", "value": "deep"},
				},
			},
		},
	}}
	got, err := patch.Apply("a.b", configmap("cm-deep"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got["data"].(map[string]any)["key"] != "deep" {
		t.Errorf("deeply-routed patch not applied: %v", got["data"])
	}
}

func TestApply_RoutedDotted_PartialMatchNoApply(t *testing.T) {
	t.Parallel()
	// Patch is under "a.b" but internalName is "a.c" — must NOT apply.
	entries := []patch.PatchEntry{{
		Name: "wrong-branch",
		Type: patch.JSONPatch6902,
		Patch: map[string]any{
			"a": map[string]any{
				"b": []any{
					map[string]any{"op": "replace", "path": "/data/key", "value": "should-not-apply"},
				},
			},
		},
	}}
	got, err := patch.Apply("a.c", configmap("cm-nomatch"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got["data"].(map[string]any)["key"] != "original" {
		t.Errorf("patch should not have applied to wrong branch: %v", got["data"])
	}
}

func TestApply_JSON6902_RemoveOp(t *testing.T) {
	t.Parallel()
	entries := []patch.PatchEntry{{
		Name: "remove-patch",
		Type: patch.JSONPatch6902,
		Patch: []any{
			map[string]any{"op": "remove", "path": "/data/key"},
		},
	}}
	got, err := patch.Apply("comp", configmap("cm-remove"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data := got["data"].(map[string]any)
	if _, exists := data["key"]; exists {
		t.Error("key should have been removed by JSON-6902 remove op")
	}
}

func TestApply_StrategicMerge_AddsNewTopLevelKey(t *testing.T) {
	t.Parallel()
	entries := []patch.PatchEntry{{
		Name: "new-label",
		Type: patch.StrategicMergePatch,
		Patch: map[string]any{
			"comp": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"env": "prod"},
				},
			},
		},
	}}
	got, err := patch.Apply("comp", configmap("cm-label"), entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	meta := got["metadata"].(map[string]any)
	labels, _ := meta["labels"].(map[string]any)
	if labels["env"] != "prod" {
		t.Errorf("label not added by SMP: %v", labels)
	}
}

func TestApply_PreservesOriginalMap(t *testing.T) {
	t.Parallel()
	// Apply must not mutate the original resourceDef.
	original := configmap("cm-immutable")
	entries := []patch.PatchEntry{{
		Name: "mutate",
		Type: patch.JSONPatch6902,
		Patch: []any{
			map[string]any{"op": "replace", "path": "/data/key", "value": "changed"},
		},
	}}
	_, err := patch.Apply("comp", original, entries)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// original must be unchanged
	if original["data"].(map[string]any)["key"] != "original" {
		t.Error("Apply mutated the input resourceDef (should deep-copy first)")
	}
}
