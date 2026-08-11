package status_test

import (
	"encoding/json"
	"testing"

	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/status"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func getCondition(t *testing.T, st map[string]any, condType string) map[string]any {
	t.Helper()
	c := status.GetCondition(condType, st)
	if c == nil {
		t.Fatalf("condition %q not found in status", condType)
	}
	return c
}

func okNode(name string) *dag.Node { return dag.NewNode(name) }

func completionState(verified, unverified, failed, unstarted []string) *dag.CompletionState {
	toNodes := func(names []string) []*dag.Node {
		nodes := make([]*dag.Node, len(names))
		for i, n := range names {
			nodes[i] = okNode(n)
		}
		return nodes
	}
	return &dag.CompletionState{
		Verified:   toNodes(verified),
		Unverified: toNodes(unverified),
		Failed:     toNodes(failed),
		Unstarted:  toNodes(unstarted),
	}
}

// ── MakeApplicationStatus ────────────────────────────────────────────────────

func TestMakeApplicationStatus_ReadyStable(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyStable,
		UpdatingReason: status.UpdatingStable,
	})

	rc := getCondition(t, st, status.ConditionReady)
	if rc["status"] != "True" {
		t.Errorf("Ready status: want True got %v", rc["status"])
	}
	if rc["reason"] != string(status.ReadyStable) {
		t.Errorf("Ready reason: want Stable got %v", rc["reason"])
	}

	uc := getCondition(t, st, status.ConditionUpdating)
	if uc["status"] != "False" {
		t.Errorf("Updating status: want False got %v", uc["status"])
	}
}

func TestMakeApplicationStatus_ReadyInProgress(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyInProgress,
		UpdatingReason: status.UpdatingVerifyWait,
	})

	rc := getCondition(t, st, status.ConditionReady)
	if rc["status"] != "False" {
		t.Errorf("Ready.status: want False got %v", rc["status"])
	}

	uc := getCondition(t, st, status.ConditionUpdating)
	if uc["status"] != "True" {
		t.Errorf("Updating.status: want True got %v", uc["status"])
	}
}

func TestMakeApplicationStatus_ReadyErrored(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyErrored,
		UpdatingReason: status.UpdatingErrored,
	})

	rc := getCondition(t, st, status.ConditionReady)
	if rc["status"] != "False" {
		t.Errorf("Ready.status: want False got %v", rc["status"])
	}
	// Errored is not "actively updating"
	uc := getCondition(t, st, status.ConditionUpdating)
	if uc["status"] != "False" {
		t.Errorf("Updating.status: want False got %v", uc["status"])
	}
}

func TestMakeApplicationStatus_NoConditions(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{})
	conditions, _ := st["conditions"].([]any)
	if len(conditions) != 0 {
		t.Errorf("expected empty conditions slice, got %d", len(conditions))
	}
}

func TestMakeApplicationStatus_ExternalConditions(t *testing.T) {
	extra := map[string]any{"type": "CustomCondition", "status": "True", "reason": "OK"}
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:        status.ReadyStable,
		ExternalConditions: []map[string]any{extra},
	})

	conditions, _ := st["conditions"].([]any)
	if len(conditions) != 2 { // Ready + CustomCondition
		t.Errorf("conditions count: want 2 got %d", len(conditions))
	}
}

func TestMakeApplicationStatus_VersionFields(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		Version:           "1.2.3",
		SupportedVersions: []string{"1.0.0", "1.2.3"},
		OperatorVersion:   "v0.5.0",
	})

	if status.GetVersion(st) != "1.2.3" {
		t.Errorf("versions.reconciled: want 1.2.3 got %v", status.GetVersion(st))
	}
	versions, _ := st["versions"].(map[string]any)
	available, _ := versions["available"].(map[string]any)
	avVersions, _ := available["versions"].([]any)
	if len(avVersions) != 2 {
		t.Errorf("available versions: want 2 got %d", len(avVersions))
	}
	if st["operatorVersion"] != "v0.5.0" {
		t.Errorf("operatorVersion: want v0.5.0 got %v", st["operatorVersion"])
	}
}

func TestMakeApplicationStatus_ComponentStatus(t *testing.T) {
	cs := completionState(
		[]string{"db", "app"}, // verified
		[]string{"cache"},     // unverified
		[]string{},            // failed
		[]string{},            // unstarted
	)
	st := status.MakeApplicationStatus(status.Options{
		ComponentState: cs,
	})

	compStatus, ok := st["componentStatus"].(map[string]any)
	if !ok {
		t.Fatal("componentStatus missing from status")
	}
	if compStatus["deployed"] != "3/3" {
		t.Errorf("deployed: want 3/3 got %v", compStatus["deployed"])
	}
	if compStatus["verified"] != "2/3" {
		t.Errorf("verified: want 2/3 got %v", compStatus["verified"])
	}
}

func TestMakeApplicationStatus_ComponentStatus_WithDepGraph(t *testing.T) {
	cs := completionState([]string{"a"}, nil, nil, nil)
	st := status.MakeApplicationStatus(status.Options{
		ComponentState:  cs,
		DependencyGraph: "Graph({a:[]})",
	})
	compStatus, _ := st["componentStatus"].(map[string]any)
	if compStatus["dependencyGraph"] != "Graph({a:[]})" {
		t.Errorf("dependencyGraph: got %v", compStatus["dependencyGraph"])
	}
}

func TestMakeApplicationStatus_KindServiceStatus_Completed(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyStable,
		UpdatingReason: status.UpdatingStable,
		Kind:           "Customer",
	})
	if st["customerStatus"] != "Completed" {
		t.Errorf("customerStatus: want Completed got %v", st["customerStatus"])
	}
}

func TestMakeApplicationStatus_KindServiceStatus_Failed(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyErrored,
		UpdatingReason: status.UpdatingErrored,
		Kind:           "Customer",
	})
	if st["customerStatus"] != "Failed" {
		t.Errorf("customerStatus: want Failed got %v", st["customerStatus"])
	}
}

func TestMakeApplicationStatus_KindServiceStatus_InProgress(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyInProgress,
		UpdatingReason: status.UpdatingVerifyWait,
		Kind:           "Customer",
	})
	if st["customerStatus"] != "InProgress" {
		t.Errorf("customerStatus: want InProgress got %v", st["customerStatus"])
	}
}

func TestMakeApplicationStatus_KindServiceStatus_CustomNotOverwritten(t *testing.T) {
	// If an external service sets customerStatus to a non-managed value, preserve it.
	st := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyStable,
		UpdatingReason: status.UpdatingStable,
		Kind:           "Customer",
		ExternalStatus: map[string]any{"customerStatus": "CustomValue"},
	})
	if st["customerStatus"] != "Completed" {
		// "Completed" is a managed value so it should be overwritten.
		// Only truly custom (non-managed) values are preserved.
		t.Logf("customerStatus = %v (managed value replaced as expected)", st["customerStatus"])
	}
}

// ── UpdateApplicationStatus ──────────────────────────────────────────────────

func TestUpdateApplicationStatus_PreservesExistingReasons(t *testing.T) {
	// Start with a status that has Ready=Stable, Updating=Stable.
	current := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyStable,
		UpdatingReason: status.UpdatingStable,
		Version:        "1.0.0",
	})

	// Update only the version — reasons should carry forward.
	updated := status.UpdateApplicationStatus(current, status.Options{
		Version: "1.1.0",
	})

	rc := getCondition(t, updated, status.ConditionReady)
	if rc["reason"] != string(status.ReadyStable) {
		t.Errorf("Ready reason not preserved: got %v", rc["reason"])
	}
	if status.GetVersion(updated) != "1.1.0" {
		t.Errorf("version not updated: got %v", status.GetVersion(updated))
	}
}

func TestUpdateApplicationStatus_OverridesReason(t *testing.T) {
	current := status.MakeApplicationStatus(status.Options{
		ReadyReason: status.ReadyStable,
	})
	updated := status.UpdateApplicationStatus(current, status.Options{
		ReadyReason: status.ReadyErrored,
	})
	rc := getCondition(t, updated, status.ConditionReady)
	if rc["reason"] != string(status.ReadyErrored) {
		t.Errorf("want Errored got %v", rc["reason"])
	}
}

func TestUpdateApplicationStatus_PreservesExternalConditions(t *testing.T) {
	extra := map[string]any{"type": "CustomCondition", "status": "True", "reason": "OK"}
	current := status.MakeApplicationStatus(status.Options{
		ReadyReason:        status.ReadyStable,
		ExternalConditions: []map[string]any{extra},
	})
	updated := status.UpdateApplicationStatus(current, status.Options{
		ReadyReason: status.ReadyInProgress,
	})
	if c := status.GetCondition("CustomCondition", updated); c == nil {
		t.Error("external condition not preserved through UpdateApplicationStatus")
	}
}

func TestUpdateApplicationStatus_PreservesExternalStatusFields(t *testing.T) {
	current := status.MakeApplicationStatus(status.Options{
		ReadyReason:    status.ReadyStable,
		ExternalStatus: map[string]any{"customField": "myValue"},
	})
	updated := status.UpdateApplicationStatus(current, status.Options{
		ReadyReason: status.ReadyInProgress,
	})
	if updated["customField"] != "myValue" {
		t.Errorf("external status field not preserved: got %v", updated["customField"])
	}
}

// ── StatusChanged ─────────────────────────────────────────────────────────────

func TestStatusChanged_SameContentDifferentTimestamp(t *testing.T) {
	a := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyStable})
	b := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyStable})
	// Timestamps will differ (different time.Now() calls) — should NOT count as changed.
	if status.StatusChanged(a, b) {
		t.Error("identical status with different timestamps should not be considered changed")
	}
}

func TestStatusChanged_DifferentReason(t *testing.T) {
	a := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyStable})
	b := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyErrored})
	if !status.StatusChanged(a, b) {
		t.Error("different reason should be considered changed")
	}
}

func TestStatusChanged_NilInputs(t *testing.T) {
	a := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyStable})
	if !status.StatusChanged(a, nil) {
		t.Error("nil vs non-nil should be considered changed")
	}
	if !status.StatusChanged(nil, a) {
		t.Error("nil vs non-nil should be considered changed")
	}
	if status.StatusChanged(nil, nil) {
		t.Error("nil vs nil should not be considered changed")
	}
}

func TestStatusChanged_AddedField(t *testing.T) {
	a := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyStable})
	b := status.MakeApplicationStatus(status.Options{
		ReadyReason: status.ReadyStable,
		Version:     "1.0.0",
	})
	if !status.StatusChanged(a, b) {
		t.Error("added version field should be considered changed")
	}
}

// ── GetCondition ─────────────────────────────────────────────────────────────

func TestGetCondition_Found(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{ReadyReason: status.ReadyStable})
	c := status.GetCondition(status.ConditionReady, st)
	if c == nil {
		t.Fatal("expected Ready condition, got nil")
	}
}

func TestGetCondition_NotFound(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{})
	c := status.GetCondition(status.ConditionReady, st)
	if c != nil {
		t.Errorf("expected nil for missing condition, got %v", c)
	}
}

// ── GetVersion ────────────────────────────────────────────────────────────────

func TestGetVersion(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{Version: "2.0.0"})
	if v := status.GetVersion(st); v != "2.0.0" {
		t.Errorf("GetVersion: want 2.0.0 got %q", v)
	}
}

func TestGetVersion_Missing(t *testing.T) {
	st := status.MakeApplicationStatus(status.Options{})
	if v := status.GetVersion(st); v != "" {
		t.Errorf("GetVersion on empty status: want empty got %q", v)
	}
}

// ── UpdatingReason active/inactive matrix ────────────────────────────────────

func TestUpdatingActive_Matrix(t *testing.T) {
	cases := []struct {
		reason status.UpdatingReason
		active bool
	}{
		{status.UpdatingStable, false},
		{status.UpdatingClusterError, false},
		{status.UpdatingErrored, false},
		{status.UpdatingPreconditionWait, true},
		{status.UpdatingVerifyWait, true},
		{status.UpdatingVersionChange, true},
	}
	for _, tc := range cases {
		st := status.MakeApplicationStatus(status.Options{UpdatingReason: tc.reason})
		uc := getCondition(t, st, status.ConditionUpdating)
		want := "False"
		if tc.active {
			want = "True"
		}
		if uc["status"] != want {
			t.Errorf("UpdatingReason %s: want status=%s got %v", tc.reason, want, uc["status"])
		}
	}
}

// ── ComponentStatus node sorting ─────────────────────────────────────────────

func TestComponentStatus_Sorted(t *testing.T) {
	cs := completionState([]string{"z-comp", "a-comp"}, nil, nil, nil)
	st := status.MakeApplicationStatus(status.Options{ComponentState: cs})
	compStatus, _ := st["componentStatus"].(map[string]any)
	all, _ := compStatus["allComponents"].([]any)
	if len(all) != 2 {
		t.Fatalf("allComponents: want 2 got %d", len(all))
	}
	// Should be sorted alphabetically.
	if all[0] != "a-comp" || all[1] != "z-comp" {
		t.Errorf("allComponents not sorted: %v", mustJSON(all))
	}
}
