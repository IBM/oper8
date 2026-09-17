package temporarypatch_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/example/oper8-go/constants"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/session"
	"github.com/example/oper8-go/temporarypatch"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func targetCR(name, namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "example.com/v1alpha1",
		"kind":       "FooCR",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"uid":       "test-uid",
		},
	}
}

func patchCRManifest(patchName, targetName, targetNS string) map[string]any {
	return map[string]any{
		"apiVersion": "oper8.org/v1",
		"kind":       "TemporaryPatch",
		"metadata": map[string]any{
			"name":      patchName,
			"namespace": targetNS,
			"uid":       "patch-uid",
		},
		"spec": map[string]any{
			"apiVersion": "example.com/v1alpha1",
			"kind":       "FooCR",
			"name":       targetName,
		},
	}
}

func newSession(dm deploymanager.DeployManager, cr map[string]any) (*session.Session, error) {
	return session.New(context.Background(), "test-id", cr, dm)
}

// ── Component tests ───────────────────────────────────────────────────────────

func TestNewComponent_TargetNotFound_Returns_PreconditionError(t *testing.T) {
	t.Parallel()
	dm := deploymanager.NewDryRunDeployManager(nil)
	_, err := temporarypatch.NewComponent(
		context.Background(), dm,
		"my-patch", "example.com/v1alpha1", "FooCR", "missing", "default", false,
	)
	if err == nil {
		t.Fatal("expected precondition error for missing target")
	}
}

func TestNewComponent_DisabledWithMissingTarget_OK(t *testing.T) {
	t.Parallel()
	dm := deploymanager.NewDryRunDeployManager(nil)
	comp, err := temporarypatch.NewComponent(
		context.Background(), dm,
		"my-patch", "example.com/v1alpha1", "FooCR", "missing", "default", true,
	)
	if err != nil {
		t.Fatalf("disabled with missing target should succeed: %v", err)
	}
	if comp == nil {
		t.Fatal("expected non-nil component")
	}
}

func TestComponent_Deploy_AddsPatchAnnotation(t *testing.T) {
	t.Parallel()
	target := targetCR("foo", "default")
	dm := deploymanager.NewDryRunDeployManager(nil, target)

	comp, err := temporarypatch.NewComponent(
		context.Background(), dm,
		"p1", "example.com/v1alpha1", "FooCR", "foo", "default", false,
	)
	if err != nil {
		t.Fatalf("NewComponent: %v", err)
	}

	patchCR := patchCRManifest("p1", "foo", "default")
	sess, err := newSession(dm, patchCR)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}

	if err := comp.Deploy(context.Background(), sess); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	stored, _ := dm.Get(context.Background(), "example.com/v1alpha1", "FooCR", "foo", "default")
	if stored == nil {
		t.Fatal("target not in store after deploy")
	}
	meta := stored["metadata"].(map[string]any)
	annos, _ := meta["annotations"].(map[string]any)
	annoStr, _ := annos[constants.TemporaryPatchesAnnotation].(string)
	patches := map[string]any{}
	_ = json.Unmarshal([]byte(annoStr), &patches)
	if _, ok := patches["p1"]; !ok {
		t.Errorf("patch p1 not in annotation: %s", annoStr)
	}
}

func TestComponent_Deploy_RemovesPatchAnnotation(t *testing.T) {
	t.Parallel()
	existing := map[string]any{"p1": map[string]any{"timestamp": "2024-01-01T00:00:00Z"}}
	annoJSON, _ := json.Marshal(existing)
	target := targetCR("foo", "default")
	target["metadata"].(map[string]any)["annotations"] = map[string]any{
		constants.TemporaryPatchesAnnotation: string(annoJSON),
	}
	dm := deploymanager.NewDryRunDeployManager(nil, target)

	comp, err := temporarypatch.NewComponent(
		context.Background(), dm,
		"p1", "example.com/v1alpha1", "FooCR", "foo", "default", true,
	)
	if err != nil {
		t.Fatalf("NewComponent: %v", err)
	}
	patchCR := patchCRManifest("p1", "foo", "default")
	sess, err := newSession(dm, patchCR)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	if err := comp.Deploy(context.Background(), sess); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	stored, _ := dm.Get(context.Background(), "example.com/v1alpha1", "FooCR", "foo", "default")
	meta := stored["metadata"].(map[string]any)
	annos, _ := meta["annotations"].(map[string]any)
	annoStr, _ := annos[constants.TemporaryPatchesAnnotation].(string)
	patches := map[string]any{}
	_ = json.Unmarshal([]byte(annoStr), &patches)
	if _, ok := patches["p1"]; ok {
		t.Error("patch p1 should have been removed")
	}
}

// ── Controller tests ──────────────────────────────────────────────────────────

func TestController_GVK(t *testing.T) {
	t.Parallel()
	c := temporarypatch.NewController(nil)
	gvk := c.GVK()
	if gvk.Group != "oper8.org" || gvk.Kind != "TemporaryPatch" {
		t.Errorf("unexpected GVK: %+v", gvk)
	}
}

func TestController_HasFinalizer(t *testing.T) {
	t.Parallel()
	c := temporarypatch.NewController(nil)
	if !c.HasFinalizer() {
		t.Error("TemporaryPatchController must have a finalizer")
	}
}

func TestController_SetupComponents_MissingSpec_Errors(t *testing.T) {
	t.Parallel()
	c := temporarypatch.NewController(nil)
	cr := map[string]any{
		"apiVersion": "oper8.org/v1",
		"kind":       "TemporaryPatch",
		"metadata":   map[string]any{"name": "p1", "namespace": "default", "uid": "uid"},
	}
	dm := deploymanager.NewDryRunDeployManager(nil)
	sess, err := newSession(dm, cr)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	if err := c.SetupComponents(context.Background(), sess); err == nil {
		t.Fatal("expected error for missing spec")
	}
}

func TestController_SetupComponents_UnpatchableKind_NoComponent(t *testing.T) {
	t.Parallel()
	// Controller only allows "AllowedKind" — FooCR should be ignored.
	c := temporarypatch.NewController([]string{"AllowedKind"})
	cr := patchCRManifest("p1", "foo", "default")
	dm := deploymanager.NewDryRunDeployManager(nil, targetCR("foo", "default"))
	sess, err := newSession(dm, cr)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	if err := c.SetupComponents(context.Background(), sess); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Graph has a synthetic root node always; real components should be 0.
	nodes := sess.Graph.Nodes()
	// Graph.Nodes() excludes the synthetic root node already, so all returned
	// nodes are real component nodes.
	if len(nodes) != 0 {
		t.Errorf("expected 0 component nodes for non-patchable kind, got %d", len(nodes))
	}
}
