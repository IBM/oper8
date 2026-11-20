package deploymanager_test

import (
	"context"
	"testing"
	"time"

	"github.com/example/oper8-go/deploymanager"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func deployment(ns, name string) map[string]any {
	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
		},
		"spec": map[string]any{"replicas": float64(1)},
	}
}

func ownerCR(ns, name, uid string) map[string]any {
	return map[string]any{
		"apiVersion": "example.com/v1",
		"kind":       "Customer",
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
			"uid":       uid,
		},
	}
}

func bg() context.Context { return context.Background() }

// ── DeployManager interface via DryRun ────────────────────────────────────────

func TestDryRun_Deploy_Create(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")

	changed, err := dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !changed {
		t.Error("expected changed=true on first deploy")
	}
	if dm.ObjectCount() != 1 {
		t.Errorf("ObjectCount: want 1 got %d", dm.ObjectCount())
	}
}

func TestDryRun_Deploy_Idempotent(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	// Same object again — should report no change.
	changed, err := dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)
	if err != nil {
		t.Fatalf("second Deploy: %v", err)
	}
	if changed {
		t.Error("expected changed=false on identical re-deploy")
	}
}

func TestDryRun_Deploy_Update(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	// Modify replicas.
	updated := deployment("default", "myapp")
	updated["spec"] = map[string]any{"replicas": float64(3)}
	changed, err := dm.Deploy(bg(), []map[string]any{updated}, deploymanager.DeployMethodDefault, false)
	if err != nil {
		t.Fatalf("Deploy update: %v", err)
	}
	if !changed {
		t.Error("expected changed=true when spec changes")
	}
	stored := dm.GetStored("default", "Deployment", "apps/v1", "myapp")
	spec, _ := stored["spec"].(map[string]any)
	if spec["replicas"] != float64(3) {
		t.Errorf("replicas: want 3 got %v", spec["replicas"])
	}
}

func TestDryRun_Get_NotFound(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj, err := dm.Get(bg(), "apps/v1", "Deployment", "missing", "default")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if obj != nil {
		t.Errorf("expected nil for missing object, got %v", obj)
	}
}

func TestDryRun_Get_Found(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	got, err := dm.Get(bg(), "apps/v1", "Deployment", "myapp", "default")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected object, got nil")
	}
	meta, _ := got["metadata"].(map[string]any)
	if meta["name"] != "myapp" {
		t.Errorf("name: want myapp got %v", meta["name"])
	}
}

func TestDryRun_Get_ReturnsDeepCopy(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	got, _ := dm.Get(bg(), "apps/v1", "Deployment", "myapp", "default")
	// Mutating the returned copy must not affect the store.
	got["spec"] = map[string]any{"replicas": float64(99)}

	stored := dm.GetStored("default", "Deployment", "apps/v1", "myapp")
	spec, _ := stored["spec"].(map[string]any)
	if spec["replicas"] == float64(99) {
		t.Error("Get should return a deep copy; store was mutated")
	}
}

func TestDryRun_Delete(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	changed, err := dm.Delete(bg(), []map[string]any{obj})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !changed {
		t.Error("expected changed=true when object existed")
	}
	if dm.ObjectCount() != 0 {
		t.Errorf("expected 0 objects after delete, got %d", dm.ObjectCount())
	}
}

func TestDryRun_Delete_NonExistent(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "ghost")

	changed, err := dm.Delete(bg(), []map[string]any{obj})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if changed {
		t.Error("expected changed=false when object does not exist")
	}
}

func TestDryRun_List(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	for _, name := range []string{"a", "b", "c"} {
		_, _ = dm.Deploy(bg(), []map[string]any{deployment("default", name)}, deploymanager.DeployMethodDefault, false)
	}

	results, err := dm.List(bg(), "apps/v1", "Deployment", "default", deploymanager.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("List: want 3 got %d", len(results))
	}
}

func TestDryRun_List_LabelSelector(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)

	withLabel := deployment("default", "tagged")
	withLabel["metadata"].(map[string]any)["labels"] = map[string]any{"env": "prod"}
	withoutLabel := deployment("default", "plain")

	_, _ = dm.Deploy(bg(), []map[string]any{withLabel, withoutLabel}, deploymanager.DeployMethodDefault, false)

	results, _ := dm.List(bg(), "apps/v1", "Deployment", "default", deploymanager.ListOptions{
		LabelSelector: "env=prod",
	})
	if len(results) != 1 {
		t.Errorf("label selector: want 1 got %d", len(results))
	}
}

func TestDryRun_SetStatus(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	status := map[string]any{"phase": "Active", "readyReplicas": float64(1)}
	changed, err := dm.SetStatus(bg(), "apps/v1", "Deployment", "myapp", "default", status)
	if err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	if !changed {
		t.Error("expected changed=true on first status set")
	}

	stored := dm.GetStored("default", "Deployment", "apps/v1", "myapp")
	st, _ := stored["status"].(map[string]any)
	if st["phase"] != "Active" {
		t.Errorf("status.phase: want Active got %v", st["phase"])
	}
}

func TestDryRun_SetStatus_NotFound(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	_, err := dm.SetStatus(bg(), "apps/v1", "Deployment", "ghost", "default", map[string]any{})
	if err == nil {
		t.Error("expected error when object does not exist")
	}
}

func TestDryRun_Watch_ReceivesAddedEvent(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	ctx, cancel := context.WithTimeout(bg(), 3*time.Second)
	defer cancel()

	ch, err := dm.Watch(ctx, "apps/v1", "Deployment", "default", deploymanager.ListOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	select {
	case evt := <-ch:
		if evt.Type != deploymanager.EventAdded {
			t.Errorf("event type: want ADDED got %s", evt.Type)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for watch event")
	}
}

func TestDryRun_Watch_ReceivesDeletedEvent(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	obj := deployment("default", "myapp")
	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, false)

	ctx, cancel := context.WithTimeout(bg(), 3*time.Second)
	defer cancel()

	ch, _ := dm.Watch(ctx, "apps/v1", "Deployment", "default", deploymanager.ListOptions{})
	// Drain the initial ADDED seed event.
	<-ch

	_, _ = dm.Delete(bg(), []map[string]any{obj})
	select {
	case evt := <-ch:
		if evt.Type != deploymanager.EventDeleted {
			t.Errorf("event type: want DELETED got %s", evt.Type)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delete event")
	}
}

func TestDryRun_Watch_ClosedOnContextCancel(t *testing.T) {
	dm := deploymanager.NewDryRunDeployManager(nil)
	ctx, cancel := context.WithCancel(bg())

	ch, _ := dm.Watch(ctx, "apps/v1", "Deployment", "default", deploymanager.ListOptions{})
	cancel()

	// Channel must close promptly after cancel.
	select {
	case _, open := <-ch:
		if open {
			t.Error("channel should be closed after context cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close within 1s of context cancel")
	}
}

// ── OwnerRef tests ────────────────────────────────────────────────────────────

func TestApplyOwnerRef_StampsReference(t *testing.T) {
	owner := ownerCR("default", "my-cr", "uid-abc")
	child := deployment("default", "myapp")

	if err := deploymanager.ApplyOwnerRef(owner, child); err != nil {
		t.Fatalf("ApplyOwnerRef: %v", err)
	}

	meta, _ := child["metadata"].(map[string]any)
	refs, _ := meta["ownerReferences"].([]any)
	if len(refs) != 1 {
		t.Fatalf("ownerReferences: want 1 got %d", len(refs))
	}
	ref, _ := refs[0].(map[string]any)
	if ref["uid"] != "uid-abc" {
		t.Errorf("uid: want uid-abc got %v", ref["uid"])
	}
	if ref["blockOwnerDeletion"] != true {
		t.Error("blockOwnerDeletion should be true")
	}
}

func TestApplyOwnerRef_Idempotent(t *testing.T) {
	owner := ownerCR("default", "my-cr", "uid-abc")
	child := deployment("default", "myapp")

	_ = deploymanager.ApplyOwnerRef(owner, child)
	_ = deploymanager.ApplyOwnerRef(owner, child) // second call

	meta, _ := child["metadata"].(map[string]any)
	refs, _ := meta["ownerReferences"].([]any)
	if len(refs) != 1 {
		t.Errorf("ownerReferences should not duplicate: got %d", len(refs))
	}
}

func TestApplyOwnerRef_CrossNamespace_Skipped(t *testing.T) {
	owner := ownerCR("ns-a", "my-cr", "uid-abc")
	child := deployment("ns-b", "myapp")

	_ = deploymanager.ApplyOwnerRef(owner, child)

	meta, _ := child["metadata"].(map[string]any)
	refs, _ := meta["ownerReferences"].([]any)
	if len(refs) != 0 {
		t.Error("cross-namespace owner ref should be skipped")
	}
}

func TestDryRun_Deploy_ManagesOwnerRefs(t *testing.T) {
	owner := ownerCR("default", "my-cr", "uid-abc")
	dm := deploymanager.NewDryRunDeployManager(owner)
	obj := deployment("default", "myapp")

	_, _ = dm.Deploy(bg(), []map[string]any{obj}, deploymanager.DeployMethodDefault, true)

	stored := dm.GetStored("default", "Deployment", "apps/v1", "myapp")
	meta, _ := stored["metadata"].(map[string]any)
	refs, _ := meta["ownerReferences"].([]any)
	if len(refs) != 1 {
		t.Errorf("expected 1 ownerReference, got %d", len(refs))
	}
}
