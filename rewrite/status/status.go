// Package status provides helpers for building and updating the status
// conditions of oper8-managed Kubernetes custom resources.
//
// Ported from oper8 Python (status.py).
//
// Two orthogonal conditions are managed:
//
//   - Ready:    True when the application can serve traffic (reason=Stable)
//   - Updating: True while an active rollout is in progress
//
// A ComponentStatus block records per-component deploy/verify counts from the
// DAG runner's CompletionState.
//
// Python translation notes:
//   - deepdiff library → pure-Go recursive comparison ignoring timestamp keys
//   - aconfig nested_set/nested_get → explicit map traversal helpers
//   - Python **kwargs merged into make_application_status → Go Options struct
package status

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/example/oper8-go/dag"
)

// ── Condition type constants ──────────────────────────────────────────────────

const (
	ConditionReady    = "Ready"
	ConditionUpdating = "Updating"

	// TimestampKey is the field inside a condition that holds the RFC3339 timestamp.
	// Python used "lastTransactionTime"; this matches that exactly.
	TimestampKey = "lastTransactionTime"
)

// ── ReadyReason ───────────────────────────────────────────────────────────────

// ReadyReason is the reason string placed in the Ready condition.
type ReadyReason string

const (
	ReadyStable       ReadyReason = "Stable"
	ReadyInitializing ReadyReason = "Initializing"
	ReadyInProgress   ReadyReason = "InProgress"
	ReadyConfigError  ReadyReason = "ConfigError"
	ReadyErrored      ReadyReason = "Errored"
)

// readyActive returns true when the Ready condition status should be "True"
// (i.e. the application is stable and serving traffic).
func readyActive(r ReadyReason) bool { return r == ReadyStable }

// ── UpdatingReason ────────────────────────────────────────────────────────────

// UpdatingReason is the reason string placed in the Updating condition.
type UpdatingReason string

const (
	UpdatingStable           UpdatingReason = "Stable"
	UpdatingPreconditionWait UpdatingReason = "PreconditionWait"
	UpdatingVerifyWait       UpdatingReason = "VerifyWait"
	UpdatingClusterError     UpdatingReason = "ClusterError"
	UpdatingErrored          UpdatingReason = "Errored"
	UpdatingVersionChange    UpdatingReason = "VersionChange"
)

// updatingActive returns true when the Updating condition status should be "True"
// (i.e. a rollout is actively in progress).
func updatingActive(r UpdatingReason) bool {
	switch r {
	case UpdatingStable, UpdatingClusterError, UpdatingErrored:
		return false
	default:
		return true
	}
}

// ── ServiceStatus ─────────────────────────────────────────────────────────────

// ServiceStatus is the IBM CloudPak-compatible top-level service status value.
type ServiceStatus string

const (
	ServiceInProgress ServiceStatus = "InProgress"
	ServiceFailed     ServiceStatus = "Failed"
	ServiceCompleted  ServiceStatus = "Completed"
)

func deriveServiceStatus(ready ReadyReason, updating UpdatingReason) ServiceStatus {
	if ready == ReadyStable && updating == UpdatingStable {
		return ServiceCompleted
	}
	if ready == ReadyErrored || ready == ReadyConfigError {
		return ServiceFailed
	}
	if updating == UpdatingErrored || updating == UpdatingClusterError {
		return ServiceFailed
	}
	return ServiceInProgress
}

// ── Options ───────────────────────────────────────────────────────────────────

// Options holds all inputs to [MakeApplicationStatus] and
// [UpdateApplicationStatus].
// Only set the fields you need; zero values are treated as "not provided".
type Options struct {
	ReadyReason        ReadyReason
	ReadyMessage       string
	UpdatingReason     UpdatingReason
	UpdatingMessage    string
	ComponentState     *dag.CompletionState // nil = omit componentStatus block
	ExternalConditions []map[string]any     // non-Ready/Updating conditions to preserve
	ExternalStatus     map[string]any       // non-conditions top-level status fields
	Version            string               // versions.reconciled
	SupportedVersions  []string             // versions.available.versions
	OperatorVersion    string               // operatorVersion
	Kind               string               // enables IBM CloudPak <kind>Status field
	DependencyGraph    string               // optional graph string in componentStatus
}

// ── MakeApplicationStatus ────────────────────────────────────────────────────

// MakeApplicationStatus builds a complete status map from scratch using opts.
// It is a direct port of Python's make_application_status().
func MakeApplicationStatus(opts Options) map[string]any {
	now := time.Now()
	status := shallowCopyMap(opts.ExternalStatus)

	// Build conditions slice.
	conditions := make([]any, 0)
	if opts.ReadyReason != "" {
		conditions = append(conditions, makeCondition(ConditionReady,
			readyActive(opts.ReadyReason), string(opts.ReadyReason), opts.ReadyMessage, now))
	}
	if opts.UpdatingReason != "" {
		conditions = append(conditions, makeCondition(ConditionUpdating,
			updatingActive(opts.UpdatingReason), string(opts.UpdatingReason), opts.UpdatingMessage, now))
	}
	for _, ec := range opts.ExternalConditions {
		conditions = append(conditions, ec)
	}
	status["conditions"] = conditions

	// ComponentStatus block.
	if opts.ComponentState != nil {
		status["componentStatus"] = makeComponentStatus(opts.ComponentState, opts.DependencyGraph)
	}

	// Version fields (IBM CloudPak nested structure).
	if opts.Version != "" {
		nestedSet(status, "versions.reconciled", opts.Version)
	}
	if len(opts.SupportedVersions) > 0 {
		versions := make([]any, len(opts.SupportedVersions))
		for i, v := range opts.SupportedVersions {
			versions[i] = map[string]any{"name": v}
		}
		nestedSet(status, "versions.available.versions", versions)
	}
	if opts.OperatorVersion != "" {
		status["operatorVersion"] = opts.OperatorVersion
	}

	// IBM CloudPak <kind>Status field.
	if opts.Kind != "" {
		field := kindStatusField(opts.Kind)
		current, _ := status[field].(string)
		managed := []string{
			string(ServiceInProgress),
			string(ServiceFailed),
			string(ServiceCompleted),
		}
		if current == "" || contains(managed, current) {
			status[field] = string(deriveServiceStatus(opts.ReadyReason, opts.UpdatingReason))
		}
	}

	return status
}

// ── UpdateApplicationStatus ──────────────────────────────────────────────────

// UpdateApplicationStatus merges opts onto currentStatus, preserving any
// existing Ready/Updating reasons and external conditions not overridden by
// opts.
//
// Ports Python's update_application_status().
func UpdateApplicationStatus(currentStatus map[string]any, opts Options) map[string]any {
	// Extract existing conditions into a type→condition map.
	existing := conditionMap(currentStatus)

	readyCond := existing[ConditionReady]
	updCond := existing[ConditionUpdating]

	// Carry forward current reasons when caller didn't provide new ones.
	if opts.ReadyReason == "" {
		opts.ReadyReason = ReadyReason(strField(readyCond, "reason"))
	}
	if opts.UpdatingReason == "" {
		opts.UpdatingReason = UpdatingReason(strField(updCond, "reason"))
	}
	if opts.ReadyMessage == "" {
		opts.ReadyMessage = strField(readyCond, "message")
	}
	if opts.UpdatingMessage == "" {
		opts.UpdatingMessage = strField(updCond, "message")
	}

	// Preserve external conditions (those not managed by oper8).
	if len(opts.ExternalConditions) == 0 {
		all, _ := currentStatus["conditions"].([]any)
		for _, c := range all {
			cm, _ := c.(map[string]any)
			t, _ := cm["type"].(string)
			if t != ConditionReady && t != ConditionUpdating {
				opts.ExternalConditions = append(opts.ExternalConditions, cm)
			}
		}
	}

	// Preserve non-conditions top-level fields.
	if opts.ExternalStatus == nil {
		opts.ExternalStatus = make(map[string]any)
		for k, v := range currentStatus {
			if k != "conditions" {
				opts.ExternalStatus[k] = v
			}
		}
	}

	return MakeApplicationStatus(opts)
}

// ── GetCondition ─────────────────────────────────────────────────────────────

// GetCondition extracts the first condition of the given type from a status
// map. Returns nil when not found.
func GetCondition(condType string, status map[string]any) map[string]any {
	conditions, _ := status["conditions"].([]any)
	for _, c := range conditions {
		cm, _ := c.(map[string]any)
		if cm["type"] == condType {
			return cm
		}
	}
	return nil
}

// ── StatusChanged ─────────────────────────────────────────────────────────────

// StatusChanged reports whether current and proposed differ in any field
// other than TimestampKey.
//
// Python used the deepdiff library; Go uses recursive JSON comparison after
// stripping timestamp keys — no external dependency.
func StatusChanged(current, proposed map[string]any) bool {
	if current == nil || proposed == nil {
		return current != nil || proposed != nil
	}
	a := stripTimestamps(current)
	b := stripTimestamps(proposed)
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) != string(bj)
}

// GetVersion extracts versions.reconciled from a status map.
func GetVersion(status map[string]any) string {
	v, _ := nestedGet(status, "versions.reconciled").(string)
	return v
}

// ── Internal helpers ─────────────────────────────────────────────────────────

func makeCondition(condType string, active bool, reason, message string, ts time.Time) map[string]any {
	statusStr := "False"
	if active {
		statusStr = "True"
	}
	return map[string]any{
		"type":       condType,
		"status":     statusStr,
		"reason":     reason,
		"message":    message,
		TimestampKey: ts.UTC().Format(time.RFC3339),
	}
}

func makeComponentStatus(cs *dag.CompletionState, depGraph string) map[string]any {
	allNames := nodeNames(append(append(append(cs.Verified, cs.Unverified...), cs.Failed...), cs.Unstarted...))
	deployedNames := nodeNames(append(cs.Verified, cs.Unverified...))
	verifiedNames := nodeNames(cs.Verified)
	unverifiedNames := nodeNames(cs.Unverified)
	failedNames := nodeNames(cs.Failed)

	sort.Strings(allNames)
	sort.Strings(deployedNames)
	sort.Strings(verifiedNames)
	sort.Strings(unverifiedNames)
	sort.Strings(failedNames)

	result := map[string]any{
		"allComponents":        toAnySlice(allNames),
		"deployedComponents":   toAnySlice(deployedNames),
		"verifiedComponents":   toAnySlice(verifiedNames),
		"unverifiedComponents": toAnySlice(unverifiedNames),
		"failedComponents":     toAnySlice(failedNames),
		"deployed":             fmt.Sprintf("%d/%d", len(deployedNames), len(allNames)),
		"verified":             fmt.Sprintf("%d/%d", len(verifiedNames), len(allNames)),
	}
	if depGraph != "" {
		result["dependencyGraph"] = depGraph
	}
	return result
}

func nodeNames(nodes []*dag.Node) []string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name()
	}
	return names
}

func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// kindStatusField converts "Customer" → "customerStatus" (IBM CloudPak convention).
func kindStatusField(kind string) string {
	if kind == "" {
		return ""
	}
	return strings.ToLower(kind[:1]) + kind[1:] + "Status"
}

// conditionMap returns a map of condition type → condition dict from a status.
func conditionMap(status map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any)
	conditions, _ := status["conditions"].([]any)
	for _, c := range conditions {
		cm, _ := c.(map[string]any)
		if t, ok := cm["type"].(string); ok {
			out[t] = cm
		}
	}
	return out
}

// stripTimestamps returns a deep copy of v with all TimestampKey fields removed.
func stripTimestamps(v any) any {
	switch val := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, vv := range val {
			if k == TimestampKey {
				continue
			}
			out[k] = stripTimestamps(vv)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = stripTimestamps(item)
		}
		return out
	default:
		return v
	}
}

// nestedSet writes value into m at the dot-separated path (e.g. "versions.reconciled").
func nestedSet(m map[string]any, path string, value any) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		m[path] = value
		return
	}
	child, ok := m[parts[0]].(map[string]any)
	if !ok {
		child = make(map[string]any)
		m[parts[0]] = child
	}
	nestedSet(child, parts[1], value)
}

// nestedGet reads from m at the dot-separated path.
func nestedGet(m map[string]any, path string) any {
	parts := strings.SplitN(path, ".", 2)
	val, ok := m[parts[0]]
	if !ok {
		return nil
	}
	if len(parts) == 1 {
		return val
	}
	child, ok := val.(map[string]any)
	if !ok {
		return nil
	}
	return nestedGet(child, parts[1])
}

func shallowCopyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func strField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
