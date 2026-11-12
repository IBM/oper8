package utils_test

import (
	"testing"

	"github.com/example/oper8-go/utils"
)

// ── MergeConfigs ─────────────────────────────────────────────────────────────

func TestMergeConfigs_OverrideWins(t *testing.T) {
	t.Parallel()
	base := map[string]any{"a": 1, "b": 2}
	result := utils.MergeConfigs(base, map[string]any{"b": 99, "c": 3})
	if result["a"] != 1 || result["b"] != 99 || result["c"] != 3 {
		t.Errorf("unexpected merge result: %v", result)
	}
}

func TestMergeConfigs_DeepMerge(t *testing.T) {
	t.Parallel()
	base := map[string]any{"outer": map[string]any{"x": 1, "y": 2}}
	over := map[string]any{"outer": map[string]any{"y": 99, "z": 3}}
	result := utils.MergeConfigs(base, over)
	inner := result["outer"].(map[string]any)
	if inner["x"] != 1 || inner["y"] != 99 || inner["z"] != 3 {
		t.Errorf("deep merge failed: %v", inner)
	}
}

func TestMergeConfigs_EmptyOverride(t *testing.T) {
	t.Parallel()
	base := map[string]any{"a": 1}
	result := utils.MergeConfigs(base, map[string]any{})
	if result["a"] != 1 {
		t.Errorf("empty override should not change base: %v", result)
	}
}

func TestMergeConfigs_OverrideScalarWithMap(t *testing.T) {
	// When base has a scalar and override has a map, override wins (no panic).
	t.Parallel()
	base := map[string]any{"a": "scalar"}
	over := map[string]any{"a": map[string]any{"nested": true}}
	result := utils.MergeConfigs(base, over)
	m, ok := result["a"].(map[string]any)
	if !ok || m["nested"] != true {
		t.Errorf("scalar→map override failed: %v", result)
	}
}

// ── GetNested ─────────────────────────────────────────────────────────────────

func TestGetNested_TopLevel(t *testing.T) {
	t.Parallel()
	m := map[string]any{"key": "val"}
	v, err := utils.GetNested(m, "key", nil)
	if err != nil || v != "val" {
		t.Errorf("got (%v, %v)", v, err)
	}
}

func TestGetNested_Dotted(t *testing.T) {
	t.Parallel()
	m := map[string]any{"a": map[string]any{"b": map[string]any{"c": 42}}}
	v, err := utils.GetNested(m, "a.b.c", nil)
	if err != nil || v != 42 {
		t.Errorf("got (%v, %v)", v, err)
	}
}

func TestGetNested_Missing_ReturnsDefault(t *testing.T) {
	t.Parallel()
	m := map[string]any{}
	v, err := utils.GetNested(m, "missing", "dflt")
	if err != nil || v != "dflt" {
		t.Errorf("got (%v, %v)", v, err)
	}
}

func TestGetNested_IntermediateNotMap_ReturnsError(t *testing.T) {
	t.Parallel()
	m := map[string]any{"a": "not-a-map"}
	_, err := utils.GetNested(m, "a.b", nil)
	if err == nil {
		t.Error("expected error for non-map intermediate")
	}
}

// ── GetNestedString ────────────────────────────────────────────────────────────

func TestGetNestedString_Found(t *testing.T) {
	t.Parallel()
	m := map[string]any{"meta": map[string]any{"name": "foo"}}
	if got := utils.GetNestedString(m, "meta.name", ""); got != "foo" {
		t.Errorf("got %q", got)
	}
}

func TestGetNestedString_Missing_ReturnsDefault(t *testing.T) {
	t.Parallel()
	m := map[string]any{}
	if got := utils.GetNestedString(m, "missing", "dflt"); got != "dflt" {
		t.Errorf("got %q", got)
	}
}

func TestGetNestedString_WrongType_ReturnsDefault(t *testing.T) {
	t.Parallel()
	m := map[string]any{"n": 42}
	if got := utils.GetNestedString(m, "n", "dflt"); got != "dflt" {
		t.Errorf("got %q", got)
	}
}

// ── SetNested ─────────────────────────────────────────────────────────────────

func TestSetNested_TopLevel(t *testing.T) {
	t.Parallel()
	m := map[string]any{}
	if err := utils.SetNested(m, "key", "val"); err != nil {
		t.Fatal(err)
	}
	if m["key"] != "val" {
		t.Errorf("got %v", m)
	}
}

func TestSetNested_DottedCreatesIntermediates(t *testing.T) {
	t.Parallel()
	m := map[string]any{}
	if err := utils.SetNested(m, "a.b.c", 99); err != nil {
		t.Fatal(err)
	}
	a := m["a"].(map[string]any)
	b := a["b"].(map[string]any)
	if b["c"] != 99 {
		t.Errorf("got %v", m)
	}
}

func TestSetNested_IntermediateNotMap_ReturnsError(t *testing.T) {
	t.Parallel()
	m := map[string]any{"a": "scalar"}
	if err := utils.SetNested(m, "a.b", 1); err == nil {
		t.Error("expected error for non-map intermediate")
	}
}

// ── GetPassthroughAnnotations ─────────────────────────────────────────────────

func TestGetPassthroughAnnotations(t *testing.T) {
	t.Parallel()
	src := map[string]string{
		"oper8.org/pause-execution": "true",
		"unrelated.io/key":          "ignored",
	}
	allowed := []string{"oper8.org/pause-execution", "oper8.org/config-defaults"}
	result := utils.GetPassthroughAnnotations(src, allowed)
	if result["oper8.org/pause-execution"] != "true" {
		t.Errorf("passthrough annotation missing: %v", result)
	}
	if _, ok := result["unrelated.io/key"]; ok {
		t.Error("non-passthrough annotation should not be included")
	}
	if _, ok := result["oper8.org/config-defaults"]; ok {
		t.Error("absent allowed key should not be in result")
	}
}
