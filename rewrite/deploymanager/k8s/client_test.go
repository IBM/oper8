package k8s_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/example/oper8-go/deploymanager"
	k8sdm "github.com/example/oper8-go/deploymanager/k8s"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func fakeClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource()
	if len(objs) > 0 {
		b = b.WithObjects(objs...)
	}
	return b.Build()
}

func configMapManifest(name, namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"data": map[string]any{
			"key": "value",
		},
	}
}

func existingCM(name, namespace string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string]string{"key": "old-value"},
	}
}

// ── Get ───────────────────────────────────────────────────────────────────────

func TestK8sDeployManager_Get_NotFound(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	got, err := dm.Get(context.Background(), "v1", "ConfigMap", "missing", "default")
	if err != nil {
		t.Fatalf("expected nil error on not-found, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil result on not-found, got %v", got)
	}
}

func TestK8sDeployManager_Get_Exists(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient(existingCM("mymap", "default")))
	got, err := dm.Get(context.Background(), "v1", "ConfigMap", "mymap", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected object, got nil")
	}
	meta, _ := got["metadata"].(map[string]any)
	if meta["name"] != "mymap" {
		t.Errorf("expected name=mymap, got %v", meta["name"])
	}
}

func TestK8sDeployManager_Get_BadAPIVersion(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	_, err := dm.Get(context.Background(), "not/valid/version", "ConfigMap", "x", "default")
	if err == nil {
		t.Fatal("expected error on bad apiVersion, got nil")
	}
}

func TestK8sDeployManager_Get_ClusterScoped(t *testing.T) {
	// Cluster-scoped resource: empty namespace must work.
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	got, err := dm.Get(context.Background(), "v1", "Namespace", "default", "")
	if err != nil {
		t.Fatalf("unexpected error for cluster-scoped get: %v", err)
	}
	_ = got // not pre-seeded — nil is fine
}

// ── Deploy ────────────────────────────────────────────────────────────────────

func TestK8sDeployManager_Deploy_SSA_Create(t *testing.T) {
	// Server-side apply falls back to Create when object absent.
	t.Parallel()
	dm := k8sdm.New(fakeClient())

	changed, err := dm.Deploy(context.Background(),
		[]map[string]any{configMapManifest("cm1", "default")},
		deploymanager.DeployMethodDefault, false)
	if err != nil {
		t.Fatalf("Deploy SSA: %v", err)
	}
	if !changed {
		t.Error("expected changed=true")
	}
	got, err := dm.Get(context.Background(), "v1", "ConfigMap", "cm1", "default")
	if err != nil || got == nil {
		t.Fatalf("object not found after Deploy: err=%v obj=%v", err, got)
	}
}

func TestK8sDeployManager_Deploy_SSA_Idempotent(t *testing.T) {
	// Deploying the same manifest twice must not error.
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	manifest := configMapManifest("cm-idem", "default")
	for i := 0; i < 2; i++ {
		_, err := dm.Deploy(context.Background(),
			[]map[string]any{manifest},
			deploymanager.DeployMethodDefault, false)
		if err != nil {
			t.Fatalf("Deploy iteration %d: %v", i, err)
		}
	}
}

func TestK8sDeployManager_Deploy_Update(t *testing.T) {
	t.Parallel()
	existing := existingCM("cm-update", "default")
	c := fakeClient(existing)
	dm := k8sdm.New(c)

	// Fetch resourceVersion required by Update.
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	_ = c.Get(context.Background(), client.ObjectKey{Name: "cm-update", Namespace: "default"}, u)
	rv := u.GetResourceVersion()

	manifest := configMapManifest("cm-update", "default")
	manifest["metadata"].(map[string]any)["resourceVersion"] = rv
	manifest["data"] = map[string]any{"key": "new-value"}

	changed, err := dm.Deploy(context.Background(),
		[]map[string]any{manifest},
		deploymanager.DeployMethodUpdate, false)
	if err != nil {
		t.Fatalf("Deploy Update: %v", err)
	}
	if !changed {
		t.Error("expected changed=true")
	}
}

func TestK8sDeployManager_Deploy_Replace(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient(existingCM("cm-replace", "default")))

	changed, err := dm.Deploy(context.Background(),
		[]map[string]any{configMapManifest("cm-replace", "default")},
		deploymanager.DeployMethodReplace, false)
	if err != nil {
		t.Fatalf("Deploy Replace: %v", err)
	}
	if !changed {
		t.Error("expected changed=true")
	}
}

func TestK8sDeployManager_Deploy_Replace_NonExistent(t *testing.T) {
	// Replace of an absent object must still succeed (delete is a no-op, then create).
	t.Parallel()
	dm := k8sdm.New(fakeClient())

	changed, err := dm.Deploy(context.Background(),
		[]map[string]any{configMapManifest("cm-new", "default")},
		deploymanager.DeployMethodReplace, false)
	if err != nil {
		t.Fatalf("Replace non-existent: %v", err)
	}
	if !changed {
		t.Error("expected changed=true after Replace-Create")
	}
}

func TestK8sDeployManager_Deploy_EmptyList(t *testing.T) {
	// Deploying an empty list must return changed=false with no error.
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	changed, err := dm.Deploy(context.Background(), nil, deploymanager.DeployMethodDefault, false)
	if err != nil {
		t.Fatalf("Deploy empty list: %v", err)
	}
	if changed {
		t.Error("expected changed=false for empty manifest list")
	}
}

// ── Delete ────────────────────────────────────────────────────────────────────

func TestK8sDeployManager_Delete(t *testing.T) {
	t.Parallel()
	c := fakeClient(existingCM("cm-del", "default"))
	dm := k8sdm.New(c)

	changed, err := dm.Delete(context.Background(),
		[]map[string]any{configMapManifest("cm-del", "default")})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !changed {
		t.Error("expected changed=true after delete")
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	err = c.Get(context.Background(), client.ObjectKey{Name: "cm-del", Namespace: "default"}, u)
	if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound after delete, got %v", err)
	}
}

func TestK8sDeployManager_Delete_NotFound(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	changed, err := dm.Delete(context.Background(),
		[]map[string]any{configMapManifest("missing", "default")})
	if err != nil {
		t.Fatalf("expected no error deleting non-existent object, got %v", err)
	}
	if changed {
		t.Error("expected changed=false when object not found")
	}
}

func TestK8sDeployManager_Delete_EmptyList(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	changed, err := dm.Delete(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Error("expected changed=false for empty delete list")
	}
}

// ── List ──────────────────────────────────────────────────────────────────────

func TestK8sDeployManager_List(t *testing.T) {
	t.Parallel()
	c := fakeClient(existingCM("a", "default"), existingCM("b", "default"))
	dm := k8sdm.New(c)

	items, err := dm.List(context.Background(), "v1", "ConfigMap", "default", deploymanager.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
}

func TestK8sDeployManager_List_Empty(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	items, err := dm.List(context.Background(), "v1", "ConfigMap", "default", deploymanager.ListOptions{})
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected 0 items, got %d", len(items))
	}
}

func TestK8sDeployManager_List_BadAPIVersion(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	_, err := dm.List(context.Background(), "not//valid", "ConfigMap", "default", deploymanager.ListOptions{})
	if err == nil {
		t.Fatal("expected error on bad apiVersion, got nil")
	}
}

// ── Watch ─────────────────────────────────────────────────────────────────────

func TestK8sDeployManager_Watch_ReturnsError(t *testing.T) {
	t.Parallel()
	dm := k8sdm.New(fakeClient())
	ch, err := dm.Watch(context.Background(), "v1", "ConfigMap", "default", deploymanager.ListOptions{})
	if err == nil {
		t.Fatal("expected error from Watch, got nil")
	}
	if ch != nil {
		t.Error("expected nil channel from Watch")
	}
}
