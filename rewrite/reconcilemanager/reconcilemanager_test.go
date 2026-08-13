package reconcilemanager_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/reconcilemanager"
	"github.com/example/oper8-go/session"
)

// ── Test helpers ──────────────────────────────────────────────────────────────

// minimalCR returns the smallest valid CR map accepted by session.New.
func minimalCR(name, namespace, kind, apiVersion string) map[string]any {
	return map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"uid":       "test-uid-1234",
		},
	}
}

// ── stubComponent ─────────────────────────────────────────────────────────────

// stubComponent implements the full Component interface.
type stubComponent struct {
	name       string
	disabled   bool
	setupErr   error
	deployErr  error
	verifyOK   bool
	setupCalls int
}

func (c *stubComponent) Name() string   { return c.name }
func (c *stubComponent) Disabled() bool { return c.disabled }

func (c *stubComponent) Setup(_ context.Context, _ *session.Session) error {
	c.setupCalls++
	return c.setupErr
}

func (c *stubComponent) Deploy(_ context.Context, _ *session.Session) error {
	return c.deployErr
}

func (c *stubComponent) Verify(_ context.Context, _ *session.Session) bool {
	return c.verifyOK
}

// ── stubController ────────────────────────────────────────────────────────────

// stubController uses BaseController for all hook no-ops and supplies a
// configurable SetupComponents function.
type stubController struct {
	controller.BaseController
	gvk           controller.GVK
	setupFunc     func(ctx context.Context, sess *session.Session) error
	shouldRequeue bool
}

func (c *stubController) GVK() controller.GVK { return c.gvk }

func (c *stubController) SetupComponents(ctx context.Context, sess *session.Session) error {
	if c.setupFunc != nil {
		return c.setupFunc(ctx, sess)
	}
	return nil
}

// ShouldRequeue returns the configured value (default false).
func (c *stubController) ShouldRequeue(_ context.Context, _ *session.Session) bool {
	return c.shouldRequeue
}

// ── finCtrl ───────────────────────────────────────────────────────────────────

// finCtrl overrides FinalizeComponents on a stubController and adds a finalizer.
type finCtrl struct {
	*stubController
	onFinalize func(context.Context, *session.Session) error
}

func (c *finCtrl) HasFinalizer() bool { return true }
func (c *finCtrl) Finalizer() string  { return "test.example.com/finalizer" }

func (c *finCtrl) FinalizeComponents(ctx context.Context, sess *session.Session) error {
	if c.onFinalize != nil {
		return c.onFinalize(ctx, sess)
	}
	return nil
}

// ── shared setup ─────────────────────────────────────────────────────────────

var testGVK = controller.GVK{Group: "test.example.com", Version: "v1alpha1", Kind: "Foo"}

// newDM returns a DryRunDeployManager pre-seeded with the given CR.
func newDM(cr map[string]any) *deploymanager.DryRunDeployManager {
	return deploymanager.NewDryRunDeployManager(nil, cr)
}

// newRM returns a ReconcileManager with status management disabled.
func newRM() *reconcilemanager.ReconcileManager {
	return reconcilemanager.New(reconcilemanager.Options{ManageStatus: false})
}

// addComp registers a stubComponent on the session graph.
func addComp(sess *session.Session, comp *stubComponent) error {
	n := dag.NewFuncNode(comp.Name(), nil)
	n.SetData(comp)
	return sess.AddComponent(n)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestReconcile_EmptyGraph verifies a controller with no components succeeds
// (empty graph is fully verified by definition).
func TestReconcile_EmptyGraph(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	result := newRM().Reconcile(context.Background(), &stubController{gvk: testGVK}, cr, newDM(cr), false)
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if result.Requeue {
		t.Error("expected Requeue=false for an empty-graph reconcile")
	}
}

// TestReconcile_SingleComponentVerified deploys one component that reports
// ready immediately; expects no error and Requeue=false.
func TestReconcile_SingleComponentVerified(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", verifyOK: true}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if comp.setupCalls != 1 {
		t.Errorf("Setup called %d times, want 1", comp.setupCalls)
	}
	if result.Requeue {
		t.Error("expected Requeue=false when all components verified")
	}
}

// TestReconcile_SetupError verifies a setup error aborts the reconcile.
func TestReconcile_SetupError(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, _ *session.Session) error {
			return context.DeadlineExceeded
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err == nil {
		t.Fatal("expected error from setup, got nil")
	}
	if !result.Requeue {
		t.Error("expected Requeue=true after setup error")
	}
}

// TestReconcile_DeployError verifies a component deploy failure causes a requeue.
func TestReconcile_DeployError(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", deployErr: context.DeadlineExceeded}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	// Deploy error → fatal HaltError → failed node → requeue.
	if !result.Requeue {
		t.Error("expected Requeue=true after deploy error")
	}
}

// TestReconcile_VerifyNotReady verifies that a not-ready component produces
// no error but does requeue (verify incomplete → !VerifyCompleted()).
func TestReconcile_VerifyNotReady(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", verifyOK: false}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when verify is incomplete")
	}
}

// TestReconcile_Precondition verifies that a failing precondition blocks
// SetupComponents from running.
func TestReconcile_Precondition(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	setupCalled := false

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, _ *session.Session) error {
			setupCalled = true
			return nil
		},
	}

	rm := reconcilemanager.New(reconcilemanager.Options{
		ManageStatus: false,
		Preconditions: []reconcilemanager.PreconditionFunc{
			func(_ context.Context, _ *session.Session) error {
				return context.DeadlineExceeded
			},
		},
	})

	result := rm.Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error (precondition should requeue cleanly): %v", result.Err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when precondition fails")
	}
	if setupCalled {
		t.Error("SetupComponents must not be called when a precondition fails")
	}
}

// TestReconcile_TwoComponentsOrdered verifies A deploys before B when B
// declares a dependency on A.
func TestReconcile_TwoComponentsOrdered(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	compA := &stubComponent{name: "a", verifyOK: true}
	compB := &stubComponent{name: "b", verifyOK: true}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			nA := dag.NewFuncNode(compA.Name(), nil)
			nA.SetData(compA)
			nB := dag.NewFuncNode(compB.Name(), nil)
			nB.SetData(compB)
			if err := sess.AddComponent(nA); err != nil {
				return err
			}
			if err := sess.AddComponent(nB); err != nil {
				return err
			}
			return sess.AddDependency(nB, nA, nil) // B waits for A
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if compA.setupCalls != 1 {
		t.Errorf("compA.Setup called %d times, want 1", compA.setupCalls)
	}
	if compB.setupCalls != 1 {
		t.Errorf("compB.Setup called %d times, want 1", compB.setupCalls)
	}
}

// TestReconcile_InvalidCR verifies a malformed CR returns an error.
func TestReconcile_InvalidCR(t *testing.T) {
	badCR := map[string]any{"kind": "Foo"} // missing apiVersion + metadata
	dm := deploymanager.NewDryRunDeployManager(nil)

	result := newRM().Reconcile(context.Background(), &stubController{gvk: testGVK}, badCR, dm, false)

	if result.Err == nil {
		t.Fatal("expected error for malformed CR, got nil")
	}
}

// TestReconcile_Finalizer verifies isFinalizer=true calls FinalizeComponents
// instead of SetupComponents.
func TestReconcile_Finalizer(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	setupCalled := false
	finalizeCalled := false

	ctrl := &finCtrl{
		stubController: &stubController{
			gvk: testGVK,
			setupFunc: func(_ context.Context, _ *session.Session) error {
				setupCalled = true
				return nil
			},
		},
		onFinalize: func(_ context.Context, _ *session.Session) error {
			finalizeCalled = true
			return nil
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), true)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if setupCalled {
		t.Error("SetupComponents must not be called during finalization")
	}
	if !finalizeCalled {
		t.Error("FinalizeComponents must be called during finalization")
	}
}

// ── NEW CASES (cases 10–24) ───────────────────────────────────────────────────

// TestReconcile_ShouldRequeue_True verifies that when ShouldRequeue returns
// true, the result requeues even when verify is complete.
func TestReconcile_ShouldRequeue_True(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", verifyOK: true}

	ctrl := &stubController{
		gvk:           testGVK,
		shouldRequeue: true,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when ShouldRequeue returns true")
	}
}

// TestReconcile_ShouldRequeue_False_Stable verifies that a stable,
// fully-verified reconcile does NOT requeue when ShouldRequeue is false.
func TestReconcile_ShouldRequeue_False_Stable(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", verifyOK: true}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Requeue {
		t.Error("expected Requeue=false for stable+verified reconcile with ShouldRequeue=false")
	}
}

// TestReconcile_MultiplePreconditions_StopsOnFirst verifies that the second
// precondition is not called when the first fails.
func TestReconcile_MultiplePreconditions_StopsOnFirst(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	secondCalled := false

	rm := reconcilemanager.New(reconcilemanager.Options{
		ManageStatus: false,
		Preconditions: []reconcilemanager.PreconditionFunc{
			func(_ context.Context, _ *session.Session) error {
				return errors.New("first fails")
			},
			func(_ context.Context, _ *session.Session) error {
				secondCalled = true
				return nil
			},
		},
	})

	result := rm.Reconcile(context.Background(), &stubController{gvk: testGVK}, cr, newDM(cr), false)

	if !result.Requeue {
		t.Error("expected Requeue=true when first precondition fails")
	}
	if secondCalled {
		t.Error("second precondition must not run when first fails")
	}
}

// TestReconcile_AllPreconditionsPass verifies that all-passing preconditions
// do not block the reconcile.
func TestReconcile_AllPreconditionsPass(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	setupCalled := false

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, _ *session.Session) error {
			setupCalled = true
			return nil
		},
	}

	rm := reconcilemanager.New(reconcilemanager.Options{
		ManageStatus: false,
		Preconditions: []reconcilemanager.PreconditionFunc{
			func(_ context.Context, _ *session.Session) error { return nil },
			func(_ context.Context, _ *session.Session) error { return nil },
		},
	})

	result := rm.Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if !setupCalled {
		t.Error("SetupComponents must be called when all preconditions pass")
	}
}

// TestReconcile_FinalizerError verifies that a FinalizeComponents error
// returns Requeue=true with the error set.
func TestReconcile_FinalizerError(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	finalizeErr := errors.New("finalize-bomb")

	ctrl := &finCtrl{
		stubController: &stubController{gvk: testGVK},
		onFinalize: func(_ context.Context, _ *session.Session) error {
			return finalizeErr
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), true)

	if result.Err == nil {
		t.Fatal("expected error from FinalizeComponents")
	}
	if !result.Requeue {
		t.Error("expected Requeue=true after finalize error")
	}
}

// TestReconcile_AddFinalizer_StampsObject verifies that when HasFinalizer()
// is true and the reconcile is not a finalizer pass, the finalizer string
// is written onto the stored object.
func TestReconcile_AddFinalizer_StampsObject(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)

	ctrl := &finCtrl{
		stubController: &stubController{gvk: testGVK},
		onFinalize:     nil,
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, dm, false)
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	// The DM should have stored the updated object with the finalizer.
	stored, err := dm.Get(context.Background(), "test.example.com/v1alpha1", "Foo", "foo", "default")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	meta, _ := stored["metadata"].(map[string]any)
	finalizers, _ := meta["finalizers"].([]any)
	found := false
	for _, f := range finalizers {
		if f == "test.example.com/finalizer" {
			found = true
		}
	}
	if !found {
		t.Errorf("finalizer not stamped onto object; finalizers=%v", finalizers)
	}
}

// TestReconcile_FailedComponentBlocksDependent verifies that a component
// that fails to deploy prevents its downstream dependency from running.
func TestReconcile_FailedComponentBlocksDependent(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	compA := &stubComponent{name: "a", deployErr: errors.New("a-fails")}
	compB := &stubComponent{name: "b", verifyOK: true}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			nA := dag.NewFuncNode(compA.Name(), nil)
			nA.SetData(compA)
			nB := dag.NewFuncNode(compB.Name(), nil)
			nB.SetData(compB)
			if err := sess.AddComponent(nA); err != nil {
				return err
			}
			if err := sess.AddComponent(nB); err != nil {
				return err
			}
			return sess.AddDependency(nB, nA, nil)
		},
	}

	newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if compB.setupCalls > 0 {
		t.Error("B must not run Setup when its upstream A has failed")
	}
}

// TestReconcile_DisabledComponent_Transparent verifies that a disabled
// component is treated as a no-op success (not blocking verify completion).
func TestReconcile_DisabledComponent_Transparent(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", disabled: true}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if comp.setupCalls != 0 {
		t.Errorf("Setup must not be called for a disabled component, got %d calls", comp.setupCalls)
	}
	if result.Requeue {
		t.Error("disabled component must not cause requeue (it is a no-op success)")
	}
}

// TestReconcile_ManageStatus_False_WritesNothing verifies that no status
// writes occur when ManageStatus=false (the DM should see only finalizer ops,
// not SetStatus calls).
func TestReconcile_ManageStatus_False_WritesNothing(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)
	comp := &stubComponent{name: "widget", verifyOK: true}

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	// ManageStatus defaults to false in newRM().
	result := newRM().Reconcile(context.Background(), ctrl, cr, dm, false)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	// If status management is disabled, SetStatus is never called.
	// We verify indirectly: the stored object has no "status" key written by us.
	stored, err := dm.Get(context.Background(), "test.example.com/v1alpha1", "Foo", "foo", "default")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, hasStatus := stored["status"]; hasStatus {
		t.Error("status must not be written when ManageStatus=false")
	}
}

// TestReconcile_RequeueAfter_Default verifies that ReconcileResult.RequeueAfter
// is zero when no explicit requeue duration is requested.
func TestReconcile_RequeueAfter_Default(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	result := newRM().Reconcile(context.Background(), &stubController{gvk: testGVK}, cr, newDM(cr), false)

	if result.RequeueAfter != 0 {
		t.Errorf("expected zero RequeueAfter by default, got %v", result.RequeueAfter)
	}
}

// TestReconcile_RequeueAfter_NotSetByReconcileManager verifies that
// ReconcileManager itself never sets RequeueAfter (that is the caller's job).
func TestReconcile_RequeueAfter_NotSetByReconcileManager(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", verifyOK: false} // not stable → Requeue=true

	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)

	if result.RequeueAfter != 0 {
		t.Errorf("ReconcileManager must not set RequeueAfter; got %v", result.RequeueAfter)
	}
	_ = time.Second // keep time import used
}
