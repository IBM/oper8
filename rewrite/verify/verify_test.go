package verify_test

import (
	"context"
	"testing"
	"time"

	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/status"
	"github.com/example/oper8-go/verify"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func makeObj(apiVersion, kind, name, ns string, statusBlock map[string]any) map[string]any {
	obj := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
		},
	}
	if statusBlock != nil {
		obj["status"] = statusBlock
	}
	return obj
}

func cond(condType, condStatus, reason string) map[string]any {
	return map[string]any{
		"type":               condType,
		"status":             condStatus,
		"reason":             reason,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}
}

// dmWith pre-populates a DryRunDeployManager with the given objects.
func dmWith(objs ...map[string]any) *deploymanager.DryRunDeployManager {
	return deploymanager.NewDryRunDeployManager(nil, objs...)
}

// ── VerifyResource – integration through DryRunDeployManager ─────────────────

func TestVerifyResource_NotFound(t *testing.T) {
	dm := dmWith()
	ok, err := verify.VerifyResource(context.Background(), dm, "v1", "Pod", "missing",
		verify.VerifyOptions{Namespace: "default"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false for missing object, got true")
	}
}

func TestVerifyResource_NoVerifier_PresentMeansVerified(t *testing.T) {
	obj := makeObj("v1", "ConfigMap", "cm", "default", nil)
	dm := dmWith(obj)
	ok, err := verify.VerifyResource(context.Background(), dm, "v1", "ConfigMap", "cm",
		verify.VerifyOptions{Namespace: "default"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true for present ConfigMap with no verifier")
	}
}

func TestVerifyResource_CustomConditionType(t *testing.T) {
	obj := makeObj("v1", "Foo", "foo", "ns",
		map[string]any{
			"conditions": []any{
				cond("Synced", "True", ""),
			},
		})
	dm := dmWith(obj)

	ok, err := verify.VerifyResource(context.Background(), dm, "v1", "Foo", "foo",
		verify.VerifyOptions{
			Namespace:     "ns",
			ConditionType: "Synced",
		})
	if err != nil || !ok {
		t.Fatalf("expected true for Synced=True custom condition, got ok=%v err=%v", ok, err)
	}
}

func TestVerifyResource_CustomConditionType_False(t *testing.T) {
	obj := makeObj("v1", "Foo", "foo", "ns",
		map[string]any{
			"conditions": []any{
				cond("Synced", "False", ""),
			},
		})
	dm := dmWith(obj)
	ok, _ := verify.VerifyResource(context.Background(), dm, "v1", "Foo", "foo",
		verify.VerifyOptions{Namespace: "ns", ConditionType: "Synced"})
	if ok {
		t.Fatal("expected false when Synced=False")
	}
}

func TestVerifyResource_PerCallVerifyFunc(t *testing.T) {
	obj := makeObj("v1", "Widget", "w", "ns", nil)
	dm := dmWith(obj)

	called := false
	ok, err := verify.VerifyResource(context.Background(), dm, "v1", "Widget", "w",
		verify.VerifyOptions{
			Namespace:  "ns",
			VerifyFunc: func(_ map[string]any) bool { called = true; return true },
		})
	if err != nil || !ok || !called {
		t.Fatalf("per-call VerifyFunc not invoked or returned wrong value")
	}
}

// ── VerifyPod ─────────────────────────────────────────────────────────────────

func TestVerifyPod(t *testing.T) {
	tests := []struct {
		name   string
		obj    map[string]any
		expect bool
	}{
		{
			"Ready=True → true",
			makeObj("v1", "Pod", "p", "ns", map[string]any{
				"conditions": []any{cond("Ready", "True", "")},
			}),
			true,
		},
		{
			"Ready=False → false",
			makeObj("v1", "Pod", "p", "ns", map[string]any{
				"conditions": []any{cond("Ready", "False", "")},
			}),
			false,
		},
		{
			"no conditions → false",
			makeObj("v1", "Pod", "p", "ns", map[string]any{}),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verify.VerifyPod(tt.obj); got != tt.expect {
				t.Errorf("VerifyPod = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ── VerifyJob ─────────────────────────────────────────────────────────────────

func TestVerifyJob(t *testing.T) {
	tests := []struct {
		name   string
		obj    map[string]any
		expect bool
	}{
		{
			"Complete=True → true",
			makeObj("batch/v1", "Job", "j", "ns", map[string]any{
				"conditions": []any{cond("Complete", "True", "")},
			}),
			true,
		},
		{
			"Complete=False → false",
			makeObj("batch/v1", "Job", "j", "ns", map[string]any{
				"conditions": []any{cond("Complete", "False", "")},
			}),
			false,
		},
		{
			"no status → false",
			makeObj("batch/v1", "Job", "j", "ns", nil),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verify.VerifyJob(tt.obj); got != tt.expect {
				t.Errorf("VerifyJob = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ── VerifyDeployment ──────────────────────────────────────────────────────────

func TestVerifyDeployment(t *testing.T) {
	available := cond("Available", "True", "")
	progressingOK := cond("Progressing", "True", "NewReplicaSetAvailable")
	progressingWait := cond("Progressing", "True", "ReplicaSetUpdated")

	tests := []struct {
		name   string
		obj    map[string]any
		expect bool
	}{
		{
			"Available+Progressing(NewRSAvailable) → true",
			makeObj("apps/v1", "Deployment", "d", "ns", map[string]any{
				"conditions": []any{available, progressingOK},
			}),
			true,
		},
		{
			"Available+Progressing(wrong reason) → false",
			makeObj("apps/v1", "Deployment", "d", "ns", map[string]any{
				"conditions": []any{available, progressingWait},
			}),
			false,
		},
		{
			"only Available → false",
			makeObj("apps/v1", "Deployment", "d", "ns", map[string]any{
				"conditions": []any{available},
			}),
			false,
		},
		{
			"no conditions → false",
			makeObj("apps/v1", "Deployment", "d", "ns", map[string]any{}),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verify.VerifyDeployment(tt.obj); got != tt.expect {
				t.Errorf("VerifyDeployment = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ── VerifyStatefulSet ─────────────────────────────────────────────────────────

func TestVerifyStatefulSet(t *testing.T) {
	tests := []struct {
		name   string
		obj    map[string]any
		expect bool
	}{
		{
			"replicas==readyReplicas → true",
			makeObj("apps/v1", "StatefulSet", "ss", "ns", map[string]any{
				"replicas":      float64(3),
				"readyReplicas": float64(3),
			}),
			true,
		},
		{
			"partial ready → false",
			makeObj("apps/v1", "StatefulSet", "ss", "ns", map[string]any{
				"replicas":      float64(3),
				"readyReplicas": float64(2),
			}),
			false,
		},
		{
			"no replicas → false",
			makeObj("apps/v1", "StatefulSet", "ss", "ns", map[string]any{}),
			false,
		},
		{
			"nil status → false",
			makeObj("apps/v1", "StatefulSet", "ss", "ns", nil),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verify.VerifyStatefulSet(tt.obj); got != tt.expect {
				t.Errorf("VerifyStatefulSet = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ── VerifySubsystem ───────────────────────────────────────────────────────────

func subsystemStatus(readyVal, updatingVal string, version string) map[string]any {
	conds := []any{
		map[string]any{
			"type":              status.ConditionReady,
			"status":            readyVal,
			"reason":            "Stable",
			status.TimestampKey: time.Now().UTC().Format(time.RFC3339),
		},
		map[string]any{
			"type":              status.ConditionUpdating,
			"status":            updatingVal,
			"reason":            "Stable",
			status.TimestampKey: time.Now().UTC().Format(time.RFC3339),
		},
	}
	st := map[string]any{"conditions": conds}
	if version != "" {
		st["versions"] = map[string]any{
			"reconciled": version,
		}
	}
	return st
}

func TestVerifySubsystem(t *testing.T) {
	tests := []struct {
		name    string
		obj     map[string]any
		desired string
		expect  bool
	}{
		{
			"Ready=True,Updating=False,version matches → true",
			makeObj("example.com/v1", "Foo", "f", "ns", subsystemStatus("True", "False", "1.0.0")),
			"1.0.0",
			true,
		},
		{
			"Ready=True,Updating=False,version mismatch → false",
			makeObj("example.com/v1", "Foo", "f", "ns", subsystemStatus("True", "False", "0.9.0")),
			"1.0.0",
			false,
		},
		{
			"version not yet set → false",
			makeObj("example.com/v1", "Foo", "f", "ns", subsystemStatus("True", "False", "")),
			"1.0.0",
			false,
		},
		{
			"Updating=True → false",
			makeObj("example.com/v1", "Foo", "f", "ns", subsystemStatus("True", "True", "1.0.0")),
			"1.0.0",
			false,
		},
		{
			"Ready=False → false",
			makeObj("example.com/v1", "Foo", "f", "ns", subsystemStatus("False", "False", "1.0.0")),
			"1.0.0",
			false,
		},
		{
			"no desiredVersion → passes version check",
			makeObj("example.com/v1", "Foo", "f", "ns", subsystemStatus("True", "False", "")),
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verify.VerifySubsystem(tt.obj, tt.desired); got != tt.expect {
				t.Errorf("VerifySubsystem = %v, want %v", got, tt.expect)
			}
		})
	}
}

// ── VerifyResource via kind registry (Pod, Job, Deployment, StatefulSet) ──────

func TestVerifyResource_KindRegistry(t *testing.T) {
	readyPod := makeObj("v1", "Pod", "pod", "ns", map[string]any{
		"conditions": []any{cond("Ready", "True", "")},
	})
	completedJob := makeObj("batch/v1", "Job", "job", "ns", map[string]any{
		"conditions": []any{cond("Complete", "True", "")},
	})
	readyDeploy := makeObj("apps/v1", "Deployment", "dep", "ns", map[string]any{
		"conditions": []any{
			cond("Available", "True", ""),
			cond("Progressing", "True", "NewReplicaSetAvailable"),
		},
	})
	readySS := makeObj("apps/v1", "StatefulSet", "ss", "ns", map[string]any{
		"replicas":      float64(2),
		"readyReplicas": float64(2),
	})

	tests := []struct {
		apiVersion string
		kind       string
		name       string
		obj        map[string]any
		expect     bool
	}{
		{"v1", "Pod", "pod", readyPod, true},
		{"batch/v1", "Job", "job", completedJob, true},
		{"apps/v1", "Deployment", "dep", readyDeploy, true},
		{"apps/v1", "StatefulSet", "ss", readySS, true},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			dm := dmWith(tt.obj)
			ok, err := verify.VerifyResource(context.Background(), dm,
				tt.apiVersion, tt.kind, tt.name,
				verify.VerifyOptions{Namespace: "ns"})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.expect {
				t.Errorf("VerifyResource(%s) = %v, want %v", tt.kind, ok, tt.expect)
			}
		})
	}
}

// ── Timestamp sort: latest condition wins ────────────────────────────────────

func TestVerifyResource_PicksLatestCondition(t *testing.T) {
	old := map[string]any{
		"type":               "Ready",
		"status":             "False",
		"reason":             "old",
		"lastTransitionTime": "2024-01-01T00:00:00Z",
	}
	recent := map[string]any{
		"type":               "Ready",
		"status":             "True",
		"reason":             "new",
		"lastTransitionTime": "2024-06-01T00:00:00Z",
	}
	obj := makeObj("v1", "Pod", "p", "ns", map[string]any{
		"conditions": []any{old, recent}, // old first — verifier must pick recent
	})
	dm := dmWith(obj)
	ok, err := verify.VerifyResource(context.Background(), dm, "v1", "Pod", "p",
		verify.VerifyOptions{Namespace: "ns"})
	if err != nil || !ok {
		t.Fatalf("expected true (latest condition wins), got ok=%v err=%v", ok, err)
	}
}

// ── Python-parity: bool status value and non-bool string ─────────────────────

func TestCheckCondition_BoolStatusTrue(t *testing.T) {
	// Python test_condition_non_str_status: {"type": "Ready", "status": True}
	// A raw bool true in a condition must be treated as Ready=True.
	obj := makeObj("v1", "Pod", "p", "ns", map[string]any{
		"conditions": []any{
			map[string]any{"type": "Ready", "status": true},
		},
	})
	if got := verify.VerifyPod(obj); !got {
		t.Error("bool status=true should be treated as Ready=True")
	}
}

func TestCheckCondition_BoolStatusFalse(t *testing.T) {
	obj := makeObj("v1", "Pod", "p", "ns", map[string]any{
		"conditions": []any{
			map[string]any{"type": "Ready", "status": false},
		},
	})
	if got := verify.VerifyPod(obj); got {
		t.Error("bool status=false should be treated as Ready=False")
	}
}

func TestCheckCondition_NonBoolString(t *testing.T) {
	// Python test_non_bool_str_value: "NotABool" → false (not a valid bool string)
	obj := makeObj("v1", "Foo", "foo", "ns", map[string]any{
		"conditions": []any{
			cond("Custom", "NotABool", ""),
		},
	})
	dm := dmWith(obj)
	ok, _ := verify.VerifyResource(context.Background(), dm, "v1", "Foo", "foo",
		verify.VerifyOptions{Namespace: "ns", ConditionType: "Custom"})
	if ok {
		t.Error("non-bool string status should not be considered true")
	}
}

func TestCheckCondition_MissingStatus(t *testing.T) {
	// Python test_condition_missing_status: condition with no status key → false
	obj := makeObj("v1", "Pod", "p", "ns", map[string]any{
		"conditions": []any{
			map[string]any{"type": "Ready"},
		},
	})
	if got := verify.VerifyPod(obj); got {
		t.Error("condition with missing status should return false")
	}
}

// ── IsSubsystem flag ──────────────────────────────────────────────────────────

func TestVerifyResource_IsSubsystem(t *testing.T) {
	obj := makeObj("example.com/v1", "Widget", "w", "ns",
		subsystemStatus("True", "False", "2.0.0"))

	dm := dmWith(obj)
	ok, err := verify.VerifyResource(context.Background(), dm,
		"example.com/v1", "Widget", "w",
		verify.VerifyOptions{
			Namespace:      "ns",
			IsSubsystem:    true,
			DesiredVersion: "2.0.0",
		})
	if err != nil || !ok {
		t.Fatalf("expected true for ready subsystem, got ok=%v err=%v", ok, err)
	}
}
