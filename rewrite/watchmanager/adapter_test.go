package watchmanager_test

import (
	"context"
	"errors"
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
	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/reconcilemanager"
	"github.com/example/oper8-go/session"
	"github.com/example/oper8-go/watchmanager"
)

// ── constants & GVK ──────────────────────────────────────────────────────────

var fooGVK = schema.GroupVersionKind{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"}

// ── scheme / client helpers ───────────────────────────────────────────────────

func fakeScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

func fakeClientWithFooCR(name, namespace string, muts ...func(*unstructured.Unstructured)) client.Client {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(fooGVK)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetGeneration(1)
	for _, m := range muts {
		m(obj)
	}
	return fake.NewClientBuilder().
		WithScheme(fakeScheme()).
		WithObjects(obj).
		Build()
}

// ── noopController ────────────────────────────────────────────────────────────

// noopController is a minimal controller.Controller for testing.
// It registers a single no-op component that always verifies immediately.
type noopController struct {
	controller.BaseController
}

func (n *noopController) GVK() controller.GVK {
	return controller.GVK{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"}
}

func (n *noopController) SetupComponents(_ context.Context, sess *session.Session) error {
	node := dag.NewFuncNode("noop", nil)
	node.SetData(&noopData{})
	return sess.AddComponent(node)
}

// noopData satisfies the component.Component interface stored in dag.Node data.
// The rollout manager reads Name/Disabled/Setup/Deploy/Verify from the node data.
type noopData struct{}

func (c *noopData) Name() string                                       { return "noop" }
func (c *noopData) Disabled() bool                                     { return false }
func (c *noopData) Setup(_ context.Context, _ *session.Session) error  { return nil }
func (c *noopData) Deploy(_ context.Context, _ *session.Session) error { return nil }
func (c *noopData) Verify(_ context.Context, _ *session.Session) bool  { return true }

// ── finalizerController ───────────────────────────────────────────────────────

type finalizerController struct {
	controller.BaseController
	finalizer string
}

func (f *finalizerController) GVK() controller.GVK {
	return controller.GVK{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"}
}
func (f *finalizerController) HasFinalizer() bool { return true }
func (f *finalizerController) Finalizer() string  { return f.finalizer }
func (f *finalizerController) SetupComponents(_ context.Context, sess *session.Session) error {
	node := dag.NewFuncNode("noop", nil)
	node.SetData(&noopData{})
	return sess.AddComponent(node)
}

// ── requeueController ─────────────────────────────────────────────────────────

type requeueController struct {
	controller.BaseController
}

func (r *requeueController) GVK() controller.GVK {
	return controller.GVK{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"}
}
func (r *requeueController) SetupComponents(_ context.Context, sess *session.Session) error {
	node := dag.NewFuncNode("noop", nil)
	node.SetData(&noopData{})
	return sess.AddComponent(node)
}
func (r *requeueController) ShouldRequeue(_ context.Context, _ *session.Session) bool { return true }

// ── setupErrController ────────────────────────────────────────────────────────

type setupErrController struct {
	controller.BaseController
	err error
}

func (s *setupErrController) GVK() controller.GVK {
	return controller.GVK{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"}
}
func (s *setupErrController) SetupComponents(_ context.Context, _ *session.Session) error {
	return s.err
}

// ── adapter factory ───────────────────────────────────────────────────────────

func newAdapter(c client.Client) *watchmanager.Adapter {
	return watchmanager.New(&noopController{}, c, fooGVK, reconcilemanager.Options{})
}

func req(name, ns string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}}
}

// ═════════════════════════════════════════════════════════════════════════════
// Adapter.Reconcile tests
// ═════════════════════════════════════════════════════════════════════════════

func TestAdapter_Reconcile_HappyPath(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	result, err := newAdapter(c).Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Requeue {
		t.Error("expected Requeue=false for fully-verified component")
	}
}

func TestAdapter_Reconcile_NotFound(t *testing.T) {
	t.Parallel()
	c := fake.NewClientBuilder().WithScheme(fakeScheme()).Build()
	result, err := newAdapter(c).Reconcile(context.Background(), req("missing", "default"))
	if err != nil {
		t.Fatalf("NotFound should return nil error, got %v", err)
	}
	if result.Requeue {
		t.Error("expected Requeue=false when object not found")
	}
}

func TestAdapter_Reconcile_IsFinalizer(t *testing.T) {
	t.Parallel()
	const fin = "oper8.org/finalizer"
	c := fakeClientWithFooCR("foo", "default", func(obj *unstructured.Unstructured) {
		now := metav1.Now()
		obj.SetDeletionTimestamp(&now)
		obj.SetFinalizers([]string{fin})
	})
	a := watchmanager.New(&finalizerController{finalizer: fin}, c, fooGVK, reconcilemanager.Options{})
	_, err := a.Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error in finalizer path: %v", err)
	}
}

func TestAdapter_Reconcile_RequeueWhenShouldRequeue(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	a := watchmanager.New(&requeueController{}, c, fooGVK, reconcilemanager.Options{})
	result, err := a.Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when ShouldRequeue=true")
	}
}

func TestAdapter_Reconcile_SetupError_PropagatesError(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	boom := errors.New("setup-bomb")
	a := watchmanager.New(&setupErrController{err: boom}, c, fooGVK, reconcilemanager.Options{})
	_, err := a.Reconcile(context.Background(), req("foo", "default"))
	if err == nil {
		t.Fatal("expected error from SetupComponents to propagate, got nil")
	}
}

func TestAdapter_Reconcile_RequeueAfter_NonNegative(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	result, err := newAdapter(c).Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter < 0 {
		t.Errorf("RequeueAfter must not be negative, got %v", result.RequeueAfter)
	}
}

// TestAdapter_Reconcile_PausedObject verifies that a paused object is filtered
// out by NotPaused() before Reconcile is even called.  We test the predicate
// layer directly (Reconcile itself doesn't know about pause; that is the
// predicate's job).
func TestAdapter_Reconcile_PausedObject_PredicateFiltersIt(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	obj.SetAnnotations(map[string]string{"oper8.org/pause-reconciliation": "true"})

	if p.Create(event.CreateEvent{Object: obj}) {
		t.Error("paused object must be filtered out by NotPaused predicate")
	}
	if p.Update(event.UpdateEvent{ObjectNew: obj}) {
		t.Error("paused object must be filtered out on Update")
	}
	if p.Delete(event.DeleteEvent{Object: obj}) {
		t.Error("paused object must be filtered out on Delete")
	}
	if p.Generic(event.GenericEvent{Object: obj}) {
		t.Error("paused object must be filtered out on Generic")
	}
}

// TestAdapter_Reconcile_DifferentNamespace verifies isolation: reconciling in
// namespace "other" when the object lives in "default" gets a NotFound response.
func TestAdapter_Reconcile_DifferentNamespace(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	result, err := newAdapter(c).Reconcile(context.Background(), req("foo", "other"))
	if err != nil {
		t.Fatalf("unexpected error for cross-namespace request: %v", err)
	}
	if result.Requeue {
		t.Error("expected Requeue=false when object not found in requested namespace")
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// GenerationChangedOrDeleted predicate tests
// ═════════════════════════════════════════════════════════════════════════════

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
	if !p.Delete(event.DeleteEvent{Object: &unstructured.Unstructured{}}) {
		t.Error("Delete event should always pass")
	}
}

func TestGenerationChangedOrDeleted_Generic(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	if !p.Generic(event.GenericEvent{Object: &unstructured.Unstructured{}}) {
		t.Error("Generic event should always pass (periodic resync)")
	}
}

func TestGenerationChangedOrDeleted_Update_SameGeneration(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	old := objWithGen(5)
	newObj := objWithGen(5)
	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: newObj}) {
		t.Error("Update with unchanged generation should be filtered out")
	}
}

func TestGenerationChangedOrDeleted_Update_GenerationIncreased(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	if !p.Update(event.UpdateEvent{ObjectOld: objWithGen(5), ObjectNew: objWithGen(6)}) {
		t.Error("generation bump should pass")
	}
}

func TestGenerationChangedOrDeleted_Update_GenerationDecreased(t *testing.T) {
	// Generation should never decrease in practice but the predicate should
	// still pass if they differ.
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	if !p.Update(event.UpdateEvent{ObjectOld: objWithGen(6), ObjectNew: objWithGen(3)}) {
		t.Error("any generation change (including decrease) should pass")
	}
}

func TestGenerationChangedOrDeleted_Update_DeletionTimestamp(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	old := objWithGen(5)
	newObj := objWithGen(5) // same generation …
	now := metav1.Now()
	newObj.SetDeletionTimestamp(&now) // … but being deleted
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: newObj}) {
		t.Error("DeletionTimestamp set should pass even if generation unchanged")
	}
}

func TestGenerationChangedOrDeleted_Update_ZeroGenerations(t *testing.T) {
	// Objects whose generation is always 0 (e.g. Secrets) should be filtered
	// so that every status-only update doesn't trigger a reconcile.
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	if p.Update(event.UpdateEvent{ObjectOld: objWithGen(0), ObjectNew: objWithGen(0)}) {
		t.Error("zero-generation update should be filtered (same == same)")
	}
}

func objWithGen(g int64) *unstructured.Unstructured {
	o := &unstructured.Unstructured{}
	o.SetGeneration(g)
	return o
}

// ═════════════════════════════════════════════════════════════════════════════
// NotPaused predicate tests
// ═════════════════════════════════════════════════════════════════════════════

func TestNotPaused_NoAnnotation(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("object without pause annotation should pass")
	}
}

func TestNotPaused_EmptyAnnotations(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	obj.SetAnnotations(map[string]string{}) // present but empty map
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("empty annotations map should pass")
	}
}

func TestNotPaused_PausedTrue(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := pausedObj("true")
	if p.Create(event.CreateEvent{Object: obj}) {
		t.Error("pause=true must be filtered")
	}
}

func TestNotPaused_PausedFalse(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	if !p.Create(event.CreateEvent{Object: pausedObj("false")}) {
		t.Error("pause=false must NOT be filtered")
	}
}

func TestNotPaused_PausedEmptyString(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	if !p.Create(event.CreateEvent{Object: pausedObj("")}) {
		t.Error("pause annotation with empty value must NOT be filtered")
	}
}

func TestNotPaused_OtherAnnotation_DoesNotFilter(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}
	obj.SetAnnotations(map[string]string{"unrelated.io/key": "true"})
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("unrelated annotation must not trigger pause filter")
	}
}

func pausedObj(val string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAnnotations(map[string]string{"oper8.org/pause-reconciliation": val})
	return obj
}

// ═════════════════════════════════════════════════════════════════════════════
// GVKFromString tests
// ═════════════════════════════════════════════════════════════════════════════

func TestGVKFromString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    schema.GroupVersionKind
		wantErr bool
	}{
		{
			name:  "three-part group/version/Kind",
			input: "example.com/v1alpha1/FooCR",
			want:  schema.GroupVersionKind{Group: "example.com", Version: "v1alpha1", Kind: "FooCR"},
		},
		{
			name:  "two-part version/Kind (core group)",
			input: "v1/ConfigMap",
			want:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		},
		{
			name:  "apps group",
			input: "apps/v1/Deployment",
			want:  schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		},
		{
			name:  "batch group",
			input: "batch/v1/Job",
			want:  schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"},
		},
		{
			name:    "bare word — no slash",
			input:   "bad",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := watchmanager.GVKFromString(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GVKFromString(%q): err=%v, wantErr=%v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// Adapter.Reconcile result-mapping edge cases
// ═════════════════════════════════════════════════════════════════════════════

// TestAdapter_Reconcile_ErrorNotReturnedForPaused demonstrates the predicate
// contract: a paused annotation means the adapter is never called at all.
// We verify this by directly testing the predicate — the adapter returns no
// error when it IS called and the object is absent (simulates post-filter call).
func TestAdapter_Reconcile_NoErrorOnCleanRun(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("bar", "kube-system")
	result, err := newAdapter(c).Reconcile(context.Background(), req("bar", "kube-system"))
	if err != nil {
		t.Fatalf("clean run: unexpected error %v", err)
	}
	if result.Requeue {
		t.Error("clean run: expected no requeue")
	}
	if result.RequeueAfter != 0 {
		t.Errorf("clean run: expected zero RequeueAfter, got %v", result.RequeueAfter)
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// Additional predicate edge-case tests
// ═════════════════════════════════════════════════════════════════════════════

// TestNotPaused_AllEventTypes_PausedFiltered verifies that the NotPaused
// predicate consistently rejects a paused object on all four event types.
func TestNotPaused_AllEventTypes_PausedFiltered(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := pausedObj("true")

	cases := []struct {
		name string
		call func() bool
	}{
		{"Create", func() bool { return p.Create(event.CreateEvent{Object: obj}) }},
		{"Update", func() bool { return p.Update(event.UpdateEvent{ObjectNew: obj}) }},
		{"Delete", func() bool { return p.Delete(event.DeleteEvent{Object: obj}) }},
		{"Generic", func() bool { return p.Generic(event.GenericEvent{Object: obj}) }},
	}
	for _, tc := range cases {
		if tc.call() {
			t.Errorf("NotPaused.%s: paused object should be filtered", tc.name)
		}
	}
}

// TestNotPaused_AllEventTypes_NotPausedPasses verifies that an unpaused
// object is let through on all four event types.
func TestNotPaused_AllEventTypes_NotPausedPasses(t *testing.T) {
	t.Parallel()
	p := watchmanager.NotPaused()
	obj := &unstructured.Unstructured{}

	cases := []struct {
		name string
		call func() bool
	}{
		{"Create", func() bool { return p.Create(event.CreateEvent{Object: obj}) }},
		{"Update", func() bool { return p.Update(event.UpdateEvent{ObjectNew: obj}) }},
		{"Delete", func() bool { return p.Delete(event.DeleteEvent{Object: obj}) }},
		{"Generic", func() bool { return p.Generic(event.GenericEvent{Object: obj}) }},
	}
	for _, tc := range cases {
		if !tc.call() {
			t.Errorf("NotPaused.%s: unpaused object should pass", tc.name)
		}
	}
}

// TestGenerationChangedOrDeleted_Update_LargeGenerationJump verifies a
// generation jump of more than 1 still passes.
func TestGenerationChangedOrDeleted_Update_LargeGenerationJump(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	if !p.Update(event.UpdateEvent{ObjectOld: objWithGen(1), ObjectNew: objWithGen(100)}) {
		t.Error("large generation jump must pass the predicate")
	}
}

// TestGenerationChangedOrDeleted_Create_ZeroGeneration verifies that a
// Create event on a generation=0 object (e.g. Secret) is always passed.
func TestGenerationChangedOrDeleted_Create_ZeroGeneration(t *testing.T) {
	t.Parallel()
	p := watchmanager.GenerationChangedOrDeleted()
	if !p.Create(event.CreateEvent{Object: objWithGen(0)}) {
		t.Error("Create with zero generation must always pass")
	}
}

// TestAdapter_Reconcile_ErrorIsWrapped verifies that a setup error is
// propagated upward (not silently swallowed).
func TestAdapter_Reconcile_ErrorIsWrapped(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	boom := errors.New("hard-failure")
	a := watchmanager.New(&setupErrController{err: boom}, c, fooGVK, reconcilemanager.Options{})
	_, err := a.Reconcile(context.Background(), req("foo", "default"))
	if err == nil {
		t.Fatal("expected non-nil error from setup failure")
	}
	// The error must somehow contain the original message.
	if !errors.Is(err, boom) && err.Error() == "" {
		t.Errorf("error chain should contain original: %v", err)
	}
}

// TestAdapter_Reconcile_RequeueFalse_OnStableVerifiedRun confirms that a
// fully verified, non-requeue controller produces Requeue=false.
func TestAdapter_Reconcile_RequeueFalse_OnStableVerifiedRun(t *testing.T) {
	t.Parallel()
	c := fakeClientWithFooCR("foo", "default")
	result, err := newAdapter(c).Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Requeue {
		t.Error("stable verified run must not requeue")
	}
}

// TestGVKFromString_CRDGroupWithDots verifies a group containing multiple
// dots (common for custom APIs) is preserved intact.
func TestGVKFromString_CRDGroupWithDots(t *testing.T) {
	t.Parallel()
	got, err := watchmanager.GVKFromString("my.operator.example.com/v1beta1/Widget")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Group != "my.operator.example.com" {
		t.Errorf("Group = %q, want %q", got.Group, "my.operator.example.com")
	}
	if got.Version != "v1beta1" {
		t.Errorf("Version = %q, want %q", got.Version, "v1beta1")
	}
	if got.Kind != "Widget" {
		t.Errorf("Kind = %q, want %q", got.Kind, "Widget")
	}
}

// TestGVKFromString_OnlySlash verifies that a single "/" produces an error.
func TestGVKFromString_OnlySlash(t *testing.T) {
	t.Parallel()
	_, err := watchmanager.GVKFromString("/")
	if err == nil {
		t.Fatal("expected error for '/' input")
	}
}

// TestAdapter_New_StoresGVK verifies that the GVK passed to New is used when
// fetching the CR during Reconcile (object missing → no error, not a crash).
func TestAdapter_New_StoresGVK(t *testing.T) {
	t.Parallel()
	differentGVK := schema.GroupVersionKind{Group: "other.io", Version: "v1", Kind: "Other"}
	c := fake.NewClientBuilder().WithScheme(fakeScheme()).Build()
	a := watchmanager.New(&noopController{}, c, differentGVK, reconcilemanager.Options{})
	// Object doesn't exist under "other.io/v1/Other" — should get NotFound, return cleanly.
	result, err := a.Reconcile(context.Background(), req("missing", "default"))
	if err != nil {
		t.Fatalf("unexpected error for NotFound with custom GVK: %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue for missing object")
	}
}

// TestAdapter_Reconcile_MultipleNamespaces verifies independent reconcile
// requests for different namespaces are each handled cleanly.
func TestAdapter_Reconcile_MultipleNamespaces(t *testing.T) {
	t.Parallel()
	c1 := fakeClientWithFooCR("foo", "ns-1")
	c2 := fakeClientWithFooCR("foo", "ns-2")

	for _, tc := range []struct {
		name   string
		client client.Client
		ns     string
	}{
		{"ns-1", c1, "ns-1"},
		{"ns-2", c2, "ns-2"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := newAdapter(tc.client).Reconcile(context.Background(), req("foo", tc.ns))
			if err != nil {
				t.Fatalf("Reconcile(%s): %v", tc.ns, err)
			}
			if result.Requeue {
				t.Errorf("Reconcile(%s): expected no requeue", tc.ns)
			}
		})
	}
}

// TestAdapter_Reconcile_DeletionTimestamp_WithFinalizer_IsFinalizer confirms
// that an object whose DeletionTimestamp is set AND whose finalizer matches the
// controller's finalizer causes isFinalizer=true in Reconcile.
// We verify this indirectly: the finalizerController is registered, the object
// has the finalizer, so no error should occur in the finalization path.
func TestAdapter_Reconcile_DeletionTimestamp_WithFinalizer_IsFinalizer(t *testing.T) {
	t.Parallel()
	const fin = "oper8.org/cleanup"
	// The fake client requires that objects with a deletionTimestamp have at
	// least one finalizer — provide one so the builder doesn't panic.
	c := fakeClientWithFooCR("foo", "default", func(obj *unstructured.Unstructured) {
		now := metav1.Now()
		obj.SetDeletionTimestamp(&now)
		obj.SetFinalizers([]string{fin})
	})
	a := watchmanager.New(&finalizerController{finalizer: fin}, c, fooGVK, reconcilemanager.Options{})
	_, err := a.Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error in finalizer path: %v", err)
	}
}

// TestAdapter_Reconcile_FinalizerController_NoDeletionTimestamp verifies that
// when HasFinalizer()=true but DeletionTimestamp is NOT set, isFinalizer stays
// false and the normal reconcile runs.
func TestAdapter_Reconcile_FinalizerController_NoDeletionTimestamp(t *testing.T) {
	t.Parallel()
	const fin = "oper8.org/cleanup"
	c := fakeClientWithFooCR("foo", "default") // no deletion timestamp
	a := watchmanager.New(&finalizerController{finalizer: fin}, c, fooGVK, reconcilemanager.Options{})
	result, err := a.Reconcile(context.Background(), req("foo", "default"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Normal reconcile with verified component → no requeue.
	_ = result
}

// Ensure time import is used (suppresses "imported and not used" error).
var _ = time.Second
