package constants_test

import (
	"strings"
	"testing"

	"github.com/example/oper8-go/constants"
)

// ── Annotation key format ─────────────────────────────────────────────────────

func TestAnnotationKeys_HaveOper8OrgPrefix(t *testing.T) {
	t.Parallel()
	keys := []string{
		constants.PauseAnnotation,
		constants.TemporaryPatchesAnnotation,
		constants.ConfigDefaultsAnnotation,
		constants.InternalNameAnnotation,
		constants.LeaseNameAnnotation,
		constants.LeaseTimeAnnotation,
		constants.LogDefaultLevelAnnotation,
		constants.LogFiltersAnnotation,
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, "oper8.org/") {
			t.Errorf("annotation key %q does not have oper8.org/ prefix", k)
		}
	}
}

func TestAnnotationKeys_AreNonEmpty(t *testing.T) {
	t.Parallel()
	keys := []string{
		constants.PauseAnnotation,
		constants.TemporaryPatchesAnnotation,
		constants.ConfigDefaultsAnnotation,
		constants.InternalNameAnnotation,
	}
	for _, k := range keys {
		if k == "" {
			t.Errorf("annotation key must not be empty (got empty for one of the keys)")
		}
	}
}

func TestAnnotationKeys_AreDistinct(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	keys := []string{
		constants.PauseAnnotation,
		constants.TemporaryPatchesAnnotation,
		constants.ConfigDefaultsAnnotation,
		constants.InternalNameAnnotation,
		constants.LeaseNameAnnotation,
		constants.LeaseTimeAnnotation,
		constants.LogDefaultLevelAnnotation,
		constants.LogFiltersAnnotation,
	}
	for _, k := range keys {
		if seen[k] {
			t.Errorf("duplicate annotation key: %q", k)
		}
		seen[k] = true
	}
}

func TestPauseAnnotation_Value(t *testing.T) {
	t.Parallel()
	// The watchmanager adapter hard-codes this value in its predicate.
	// Verifying equality keeps both in sync.
	if constants.PauseAnnotation != "oper8.org/pause-execution" {
		t.Errorf("PauseAnnotation = %q, want oper8.org/pause-execution", constants.PauseAnnotation)
	}
}

func TestTemporaryPatchesAnnotation_Value(t *testing.T) {
	t.Parallel()
	if constants.TemporaryPatchesAnnotation != "oper8.org/temporary-patches" {
		t.Errorf("TemporaryPatchesAnnotation = %q", constants.TemporaryPatchesAnnotation)
	}
}

// ── PassthroughAnnotations ────────────────────────────────────────────────────

func TestPassthroughAnnotations_NonEmpty(t *testing.T) {
	t.Parallel()
	if len(constants.PassthroughAnnotations) == 0 {
		t.Error("PassthroughAnnotations must not be empty")
	}
}

func TestPassthroughAnnotations_ContainsPauseAnnotation(t *testing.T) {
	t.Parallel()
	for _, a := range constants.PassthroughAnnotations {
		if a == constants.PauseAnnotation {
			return
		}
	}
	t.Errorf("PassthroughAnnotations must contain PauseAnnotation (%s)", constants.PauseAnnotation)
}

func TestPassthroughAnnotations_ContainsConfigDefaults(t *testing.T) {
	t.Parallel()
	for _, a := range constants.PassthroughAnnotations {
		if a == constants.ConfigDefaultsAnnotation {
			return
		}
	}
	t.Errorf("PassthroughAnnotations must contain ConfigDefaultsAnnotation (%s)", constants.ConfigDefaultsAnnotation)
}

func TestPassthroughAnnotations_NoDuplicates(t *testing.T) {
	t.Parallel()
	seen := make(map[string]int)
	for _, a := range constants.PassthroughAnnotations {
		seen[a]++
		if seen[a] > 1 {
			t.Errorf("duplicate in PassthroughAnnotations: %q", a)
		}
	}
}

func TestPassthroughAnnotations_AllHaveOper8Prefix(t *testing.T) {
	t.Parallel()
	for _, a := range constants.PassthroughAnnotations {
		if !strings.HasPrefix(a, "oper8.org/") {
			t.Errorf("passthrough annotation %q does not have oper8.org/ prefix", a)
		}
	}
}

// ── Other constants ───────────────────────────────────────────────────────────

func TestNestedDictDelim_IsDot(t *testing.T) {
	t.Parallel()
	if constants.NestedDictDelim != "." {
		t.Errorf("NestedDictDelim: want '.' got %q", constants.NestedDictDelim)
	}
}

func TestDefaultNamespace_IsDefault(t *testing.T) {
	t.Parallel()
	if constants.DefaultNamespace != "default" {
		t.Errorf("DefaultNamespace: want 'default' got %q", constants.DefaultNamespace)
	}
}

func TestConfigOverridesKey_NonEmpty(t *testing.T) {
	t.Parallel()
	if constants.ConfigOverridesKey == "" {
		t.Error("ConfigOverridesKey must not be empty")
	}
}

func TestConfigOverridesKey_Value(t *testing.T) {
	t.Parallel()
	if constants.ConfigOverridesKey != "configOverrides" {
		t.Errorf("ConfigOverridesKey: want 'configOverrides' got %q", constants.ConfigOverridesKey)
	}
}
