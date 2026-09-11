// Package verify provides helpers for checking whether a deployed Kubernetes
// resource is in its desired state.
//
// Ported from oper8 Python (verify_resources.py).
//
// Design decisions vs Python:
//   - Python used a module-level _resource_verifiers dict populated at import
//     time.  Go uses a package-level map with explicit Register() so callers
//     can add custom verifiers without mutation race conditions in tests.
//   - Python's session.get_object_current_state() call is replaced by a
//     context-aware DeployManager.Get() call – the session abstraction does
//     not exist yet in the Go port.
//   - Python's _SESSION_NAMESPACE sentinel is replaced by an explicit empty
//     string (callers that want the operator namespace pass it in directly).
//   - Python's dateutil.parser timestamp sorting is replaced by a
//     time.Parse(RFC3339) sort – no external dependency.
package verify

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/example/oper8-go/deploymanager"
)

// VerifyFunc is the signature for a kind-specific resource verifier.
// It receives the full current object state as returned by the cluster
// and returns true when the resource is considered ready / verified.
type VerifyFunc func(objectState map[string]any) bool

// DefaultTimestampKey is the condition field used for timestamp-based sorting.
const DefaultTimestampKey = "lastTransitionTime"

// package-level registry populated by Register and the init() in builtin.go.
var kindVerifiers = map[string]VerifyFunc{}

// Register adds or replaces the VerifyFunc for a given kind name.
// It is safe to call at init time from multiple files.
func Register(kind string, fn VerifyFunc) { kindVerifiers[kind] = fn }

// VerifyOptions configures an individual VerifyResource call.
type VerifyOptions struct {
	// Namespace of the resource. Empty string → non-namespaced resource.
	Namespace string

	// VerifyFunc overrides the kind-level registry for this call only.
	VerifyFunc VerifyFunc

	// IsSubsystem marks the resource as an oper8-managed subsystem, enabling
	// the subsystem verifier when no other verifier is found.
	IsSubsystem bool

	// DesiredVersion is passed to the subsystem verifier to confirm rollout
	// completion. Ignored when IsSubsystem is false.
	DesiredVersion string

	// ConditionType triggers the generic condition verifier with the given
	// condition type. When set, VerifyFunc, IsSubsystem, and kind registry
	// are all bypassed.
	ConditionType string

	// TimestampKey overrides DefaultTimestampKey when ConditionType is used.
	TimestampKey string
}

// VerifyResource fetches the current cluster state for the named resource and
// decides whether it is verified / ready.
//
// The lookup uses dm.Get; if the object is not found, false is returned.
// Verification logic priority (matches Python):
//  1. opts.ConditionType set → generic condition check
//  2. opts.VerifyFunc set → use it
//  3. kind registered in registry → use that
//  4. opts.IsSubsystem → subsystem verifier
//  5. object exists → considered verified (present = ready)
func VerifyResource(
	ctx context.Context,
	dm deploymanager.DeployManager,
	apiVersion, kind, name string,
	opts VerifyOptions,
) (bool, error) {
	obj, err := dm.Get(ctx, apiVersion, kind, name, opts.Namespace)
	if err != nil {
		return false, err
	}
	if obj == nil {
		return false, nil
	}

	// 1. Custom condition type.
	if opts.ConditionType != "" {
		tsKey := opts.TimestampKey
		if tsKey == "" {
			tsKey = DefaultTimestampKey
		}
		return verifyCondition(obj, opts.ConditionType, true, tsKey, ""), nil
	}

	// 2. Per-call override.
	if opts.VerifyFunc != nil {
		return opts.VerifyFunc(obj), nil
	}

	// 3. Kind registry.
	if fn, ok := kindVerifiers[kind]; ok {
		return fn(obj), nil
	}

	// 4. Subsystem verifier.
	if opts.IsSubsystem {
		return VerifySubsystem(obj, opts.DesiredVersion), nil
	}

	// 5. Present → verified.
	return true, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// verifyCondition is the Go port of Python's _verify_condition.
// It finds all conditions of condType, picks the most recent, and checks
// whether its status matches expectedStatus (and, optionally, expectedReason).
func verifyCondition(
	objectState map[string]any,
	condType string,
	expectedStatus bool,
	timestampKey string,
	expectedReason string,
) bool {
	conditions := getConditions(objectState, condType)
	if len(conditions) == 0 {
		return false
	}
	latest := sortConditionsByDate(conditions, timestampKey)[0]
	return checkCondition(latest, expectedStatus, expectedReason)
}

func getConditions(objectState map[string]any, condType string) []map[string]any {
	statusBlock, _ := objectState["status"].(map[string]any)
	all, _ := statusBlock["conditions"].([]any)
	var out []map[string]any
	for _, c := range all {
		cm, _ := c.(map[string]any)
		if cm["type"] == condType {
			out = append(out, cm)
		}
	}
	return out
}

func parseConditionTimestamp(condition map[string]any, timestampKey string) time.Time {
	ts, _ := condition[timestampKey].(string)
	if ts == "" {
		return time.Unix(0, 0)
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Unix(0, 0)
	}
	return t
}

func sortConditionsByDate(conditions []map[string]any, timestampKey string) []map[string]any {
	sorted := make([]map[string]any, len(conditions))
	copy(sorted, conditions)
	sort.Slice(sorted, func(i, j int) bool {
		ti := parseConditionTimestamp(sorted[i], timestampKey)
		tj := parseConditionTimestamp(sorted[j], timestampKey)
		return ti.After(tj) // newest first
	})
	return sorted
}

// checkCondition checks whether a single condition dict matches expectedStatus
// and (optionally) expectedReason.
//
// Python's _check_condition handles three representations of the status field:
//   - string "True"/"False" (standard Kubernetes)
//   - any other string — case-insensitive compare against "true"/"false"
//   - raw bool (e.g. from YAML parsed without string coercion)
//
// Go must handle all three because map[string]any can hold a bool value.
func checkCondition(condition map[string]any, expectedStatus bool, expectedReason string) bool {
	raw := condition["status"]
	if raw == nil {
		return false
	}

	var statusOK bool
	switch v := raw.(type) {
	case bool:
		// Raw bool — direct compare (covers test_condition_non_str_status).
		statusOK = v == expectedStatus
	case string:
		if v == "" {
			return false
		}
		// Case-insensitive string compare: "True"/"true"/"False"/"false"/etc.
		statusOK = strings.EqualFold(v, "true") == expectedStatus
	default:
		return false
	}

	if !statusOK {
		return false
	}
	if expectedReason == "" {
		return true
	}
	reason, _ := condition["reason"].(string)
	return reason == expectedReason
}
