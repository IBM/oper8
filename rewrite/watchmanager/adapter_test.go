package watchmanager_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/example/oper8-go/api/v1alpha1"
	"github.com/example/oper8-go/component"
	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/reconcilemanager"
	"github.com/example/oper8-go/session"
	"github.com/example/oper8-go/watchmanager"
)

// fooGVK is the GVK for FooCR.
var fooGVK = schema.GroupVersionKind{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"}

// fakeScheme returns a runtime.Scheme with FooCR and core types registered.
func fakeScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// fakeClientWithFooCR builds a fake client pre-loaded with a FooCR.
func fakeClientWithFooCR(name, namespace string, extra ...func(*unstructured.Unstructured)) client.Client {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(fooGVK)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetGeneration(1)
	for _, f := range extra {
		f(obj)
	}
	return fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(obj).
		Build()
}

// noopController is a minimal controller.Controller for testing.
type noopController struct {
	controller.BaseController
}

func (n *noopController) SetupComponents(_ context.Context, sess *session.Session) error {
	// Register one no-op component.
	comp := &noopComponent{name: "noop"}
	sess.AddComponent(comp)
	return nil
}

type noopComponent struct {
	name string
}

func (c *noopComponent) Name() string                                          { return c.name }
func (c *noopComponent) Disabled() bool                                        { return false }
func (c *noopComponent) Setup(_ context.Context, _ *session.Session) error     { return nil }
func (c *noopComponent) Deploy(_ context.Context, _ *session.Session) error    { return nil }
func (c *noopComponent) Verify(_ context.Context, _ *session.Session) (component.VerifyResult, error) {
	return component.VerifyResult{Verified: true}, nil
}

// ensure noopComponent satisfies component.Component interface at compile time.
var _ component.Component = (*noopComponent)(nil)

func newAdapter(c client.Client) *watchmanager.Adapter {
	ctrl := &noopController{}
	return watchmanager.New(ctrl, c, fooGVK, reconcilemanager.Options{})
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestAdapter_Reconcile_HappyPath(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	a := newAdapter(c)

	result, err := a.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "foo", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// noop component verifies immediately — no requeue expected.
	if result.Requeue {
		t.Error("expected Requeue=false for fully-verified component")
	}
}

func TestAdapter_Reconcile_NotFound(t *testing.T) {
	t.Parallel()
	// Empty cluster — no FooCR.
	c := fake.NewClientBuilder().WithScheme(fakeScheme()).Build()
	a := newAdapter(c)

	result, err := a.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("NotFound should return nil error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected Requeue=false when object not found")
	}
}

func TestAdapter_Reconcile_IsFinalizer(t *testing.T) {
	t.Parallel()
	finalizerName := "oper8.org/finalizer" // BaseController default

	c := fakeClientWithFooCR("foo", "default", func(obj *unstructured.Unstructured) {
		now := metav1.Now()
		obj.SetDeletionTimestamp(&now)
		obj.SetFinalizers([]string{finalizerName})
	})

	// Use a controller that reports it has a finalizer.
	ctrl := &finalizerController{finalizer: finalizerName}
	a := watchmanager.New(ctrl, c, fooGVK, reconcilemanager.Options{})

	result, err := a.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "foo", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error in finalizer path: %v", err)
	}
	_ = result // result state depends on FinalizeComponents impl
}

// finalizerController is a controller that declares a finalizer.
type finalizerController struct {
	controller.BaseController
	finalizer string
}

func (f *finalizerController) HasFinalizer() bool   { return true }
func (f *finalizerController) Finalizer() string    { return f.finalizer }
func (f *finalizerController) SetupComponents(_ context.Context, sess *session.Session) error {
	sess.AddComponent(&noopComponent{name: "noop"})
	return nil
}

func TestAdapter_Reconcile_RequeueAfter(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")

	// Use a controller whose ShouldRequeue always returns true.
	ctrl := &requeueController{}
	a := watchmanager.New(ctrl, c, fooGVK, reconcilemanager.Options{})

	result, err := a.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "foo", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when ShouldRequeue=true")
	}
}

type requeueController struct{ controller.BaseController }

func (r *requeueController) SetupComponents(_ context.Context, sess *session.Session) error {
	sess.AddComponent(&noopComponent{name: "noop"})
	return nil
}
func (r *requeueController) ShouldRequeue(_ context.Context, _ *session.Session) bool { return true }

func TestAdapter_Reconcile_RequeueAfterDuration(t *testing.T) {
	t.Parallel()
	// RequeueAfter on ReconcileResult is mapped straight to ctrl.Result.RequeueAfter.
	// We test this at the mapping level: a controller that returns ShouldRequeue
	// and the result propagates (RequeueAfter is set externally; here we verify
	// the zero case doesn't break anything).
	c := fakeClientWithFooCR("foo", "default")
	a := newAdapter(c)
	result, err := a.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "foo", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter < 0 {
		t.Errorf("RequeueAfter must not be negative, got %v", result.RequeueAfter)
	}
}

// ── Predicate tests ───────────────────────────────────────────────────────────

func TestGenerationChangedOrDeleted_Create(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	obj := &unstructured.Unstructured{}
	obj.SetGeneration(1)
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("Create event should always pass")
	}
}

func TestGenerationChangedOrDeleted_Delete(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	obj := &unstructured.Unstructured{}
	if !p.Delete(event.DeleteEvent{Object: obj}) {
		t.Error("Delete event should always pass")
	}
}

func TestGenerationChangedOrDeleted_Update_SameGeneration(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	old := &unstructured.Unstructured{}
	old.SetGeneration(5)
	newObj := &unstructured.Unstructured{}
	newObj.SetGeneration(5)
	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: newObj}) {
		t.Error("Update with unchanged generation should be filtered out")
	}
}

func TestGenerationChangedOrDeleted_Update_GenerationChanged(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	old := &unstructured.Unstructured{}
	old.SetGeneration(5)
	newObj := &unstructured.Unstructured{}
	newObj.SetGeneration(6)
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: newObj}) {
		t.Error("Update with changed generation should pass")
	}
}

func TestGenerationChangedOrDeleted_Update_Deleting(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	old := &unstructured.Unstructured{}
	old.SetGeneration(5)
	newObj := &unstructured.Unstructured{}
	newObj.SetGeneration(5)
	now := metav1.Now()
	newObj.SetDeletionTimestamp(&now)
	// Same generation but DeletionTimestamp set → should pass.
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: newObj}) {
		t.Error("Update with DeletionTimestamp set should pass even if generation unchanged")
	}
}

func TestNotPaused_NoAnnotation(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("Object without pause annotation should pass")
	}
}

func TestNotPaused_PausedTrue(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	obj.SetAnnotations(map[string]string{"oper8.org/pause-reconciliation": "true"})
	if p.Create(event.CreateEvent{Object: obj}) {
		t.Error("Paused object should be filtered out")
	}
}

func TestNotPaused_PausedFalse(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	obj.SetAnnotations(map[string]string{"oper8.org/pause-reconciliation": "false"})
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("pause=false should NOT be filtered")
	}
}

func TestGVKFromString_Valid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		want    schema.GroupVersionKind
		wantErr bool
	}{
		{
			input: "example.com/v1alpha1/FooCR",
			want:  schema.GroupVersionKind{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"},
		},
		{
			input: "v1/ConfigMap",
			want:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		},
		{
			input:   "bad",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got, err := watchmanager.GVKFromString(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GVKFromString(%q) err=%v wantErr=%v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Ensure time import is used.
var _ = time.Second
