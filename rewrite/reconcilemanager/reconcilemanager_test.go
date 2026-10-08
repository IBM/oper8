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
	"github.com/example/oper8-go/status"
)

// ── helpers ───────────────────────────────────────────────────────────────────

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

type stubComponent struct {
	name       string
	setupErr   error
	deployErr  error
	verifyOK   bool
	setupCalls int
}

func (c *stubComponent) Name() string { return c.name }
func (c *stubComponent) Setup(_ context.Context, _ *session.Session) error {
	c.setupCalls++
	return c.setupErr
}
func (c *stubComponent) Deploy(_ context.Context, _ *session.Session) error { return c.deployErr }
func (c *stubComponent) Verify(_ context.Context, _ *session.Session) bool  { return c.verifyOK }

// stubController provides a configurable controller for tests.
type stubController struct {
	controller.BaseController
	gvk           controller.GVK
	setupFunc     func(ctx context.Context, sess *session.Session) error
	shouldRequeue bool
	hasFinalizer  bool
	finalizerStr  string
}

func (c *stubController) GVK() controller.GVK { return c.gvk }

func (c *stubController) SetupComponents(ctx context.Context, sess *session.Session) error {
	if c.setupFunc != nil {
		return c.setupFunc(ctx, sess)
	}
	return nil
}

func (c *stubController) ShouldRequeue(_ context.Context, _ *session.Session) bool {
	return c.shouldRequeue
}

func (c *stubController) HasFinalizer() bool { return c.hasFinalizer }
func (c *stubController) Finalizer() string  { return c.finalizerStr }

// finCtrl overrides FinalizeComponents.
type finCtrl struct {
	*stubController
	onFinalize func(context.Context, *session.Session) error
}

func (c *finCtrl) FinalizeComponents(ctx context.Context, sess *session.Session) error {
	return c.onFinalize(ctx, sess)
}

var testGVK = controller.GVK{Group: "test.example.com", Version: "v1alpha1", Kind: "Foo"}

func newDM(cr map[string]any) *deploymanager.DryRunDeployManager {
	return deploymanager.NewDryRunDeployManager(nil, cr)
}

func newRM() *reconcilemanager.ReconcileManager {
	return reconcilemanager.New(reconcilemanager.Options{ManageStatus: false})
}

func newRMWithStatus() *reconcilemanager.ReconcileManager {
	return reconcilemanager.New(reconcilemanager.Options{ManageStatus: true})
}

func addComp(sess *session.Session, comp *stubComponent) error {
	n := dag.NewFuncNode(comp.Name(), nil)
	n.SetData(comp)
	return sess.AddComponent(n)
}

func defaultCtrl() *stubController { return &stubController{gvk: testGVK} }

// ── basic reconcile ───────────────────────────────────────────────────────────

func TestReconcile_EmptyGraph(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	result := newRM().Reconcile(context.Background(), defaultCtrl(), cr, newDM(cr), false)
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if result.Requeue {
		t.Error("expected Requeue=false for empty-graph reconcile")
	}
}

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
}

func TestReconcile_SetupError(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, _ *session.Session) error {
			return errors.New("config-error")
		},
	}
	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if result.Err == nil {
		t.Fatal("expected error from setup")
	}
	if !result.Requeue {
		t.Error("expected Requeue=true after setup error")
	}
}

func TestReconcile_DeployError(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	comp := &stubComponent{name: "widget", deployErr: errors.New("deploy-fail")}
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}
	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if !result.Requeue {
		t.Error("expected Requeue=true after deploy error")
	}
}

func TestReconcile_VerifyNotReady_NoError(t *testing.T) {
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
		t.Fatalf("verify-not-ready should not produce an error: %v", result.Err)
	}
}

// ── requeue control ───────────────────────────────────────────────────────────

func TestReconcile_ShouldRequeue_True(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	ctrl := &stubController{gvk: testGVK, shouldRequeue: true}
	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if !result.Requeue {
		t.Error("expected Requeue=true when ShouldRequeue returns true")
	}
}

func TestReconcile_ShouldRequeue_False(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	ctrl := &stubController{gvk: testGVK, shouldRequeue: false}
	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if result.Requeue {
		t.Error("expected Requeue=false when ShouldRequeue returns false")
	}
}

// ── preconditions ─────────────────────────────────────────────────────────────

func TestReconcile_Precondition_Fails_BlocksSetup(t *testing.T) {
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
				return errors.New("not ready yet")
			},
		},
	})
	result := rm.Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if result.Err != nil {
		t.Fatalf("precondition should requeue cleanly: %v", result.Err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when precondition fails")
	}
	if setupCalled {
		t.Error("SetupComponents must not be called after failing precondition")
	}
}

func TestReconcile_Precondition_MultiplePasses_AllMustPass(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	calls := 0
	rm := reconcilemanager.New(reconcilemanager.Options{
		ManageStatus: false,
		Preconditions: []reconcilemanager.PreconditionFunc{
			func(_ context.Context, _ *session.Session) error { calls++; return nil },
			func(_ context.Context, _ *session.Session) error { calls++; return errors.New("stop") },
			func(_ context.Context, _ *session.Session) error { calls++; return nil },
		},
	})
	result := rm.Reconcile(context.Background(), defaultCtrl(), cr, newDM(cr), false)
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when second precondition fails")
	}
	// Third precondition must not run.
	if calls != 2 {
		t.Errorf("expected 2 precondition calls before stop, got %d", calls)
	}
}

func TestReconcile_Precondition_AllPass_ProceedsToSetup(t *testing.T) {
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
		},
	})
	rm.Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if !setupCalled {
		t.Error("SetupComponents should be called when all preconditions pass")
	}
}

// ── finalizers ────────────────────────────────────────────────────────────────

func TestReconcile_Finalizer_CallsFinalizeNotSetup(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	setupCalled, finalizeCalled := false, false

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

func TestReconcile_Finalizer_Error(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	ctrl := &finCtrl{
		stubController: &stubController{gvk: testGVK},
		onFinalize: func(_ context.Context, _ *session.Session) error {
			return errors.New("finalize-error")
		},
	}
	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), true)
	if result.Err == nil {
		t.Fatal("expected error from FinalizeComponents")
	}
}

func TestReconcile_AddFinalizer_WhenHasFinalizer(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)
	ctrl := &stubController{
		gvk:          testGVK,
		hasFinalizer: true,
		finalizerStr: "finalizers.foo.test.example.com",
	}
	newRM().Reconcile(context.Background(), ctrl, cr, dm, false)

	// Check that the finalizer was stamped onto the stored object.
	stored := dm.GetStored("default", "Foo", "test.example.com/v1alpha1", "foo")
	if stored == nil {
		t.Fatal("object not found in DM store")
	}
	meta, _ := stored["metadata"].(map[string]any)
	finalizers, _ := meta["finalizers"].([]any)
	found := false
	for _, f := range finalizers {
		if f == "finalizers.foo.test.example.com" {
			found = true
		}
	}
	if !found {
		t.Errorf("finalizer not found in stored object; finalizers=%v", finalizers)
	}
}

// ── invalid CR ────────────────────────────────────────────────────────────────

func TestReconcile_InvalidCR_MissingKind(t *testing.T) {
	bad := map[string]any{"apiVersion": "test.io/v1", "metadata": map[string]any{"name": "a", "namespace": "b"}}
	result := newRM().Reconcile(context.Background(), defaultCtrl(), bad, deploymanager.NewDryRunDeployManager(nil), false)
	if result.Err == nil {
		t.Fatal("expected error for CR missing kind")
	}
}

func TestReconcile_InvalidCR_MissingAPIVersion(t *testing.T) {
	bad := map[string]any{"kind": "Foo", "metadata": map[string]any{"name": "a", "namespace": "b"}}
	result := newRM().Reconcile(context.Background(), defaultCtrl(), bad, deploymanager.NewDryRunDeployManager(nil), false)
	if result.Err == nil {
		t.Fatal("expected error for CR missing apiVersion")
	}
}

func TestReconcile_InvalidCR_MissingMetadata(t *testing.T) {
	bad := map[string]any{"kind": "Foo", "apiVersion": "test.io/v1"}
	result := newRM().Reconcile(context.Background(), defaultCtrl(), bad, deploymanager.NewDryRunDeployManager(nil), false)
	if result.Err == nil {
		t.Fatal("expected error for CR missing metadata")
	}
}

// ── multi-component ordering ──────────────────────────────────────────────────

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
			return sess.AddDependency(nB, nA, nil)
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

func TestReconcile_FailedComponent_BlocksDependent(t *testing.T) {
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
			_ = sess.AddComponent(nA)
			_ = sess.AddComponent(nB)
			return sess.AddDependency(nB, nA, nil)
		},
	}
	result := newRM().Reconcile(context.Background(), ctrl, cr, newDM(cr), false)
	if compB.setupCalls > 0 {
		t.Error("compB should not run when compA fails")
	}
	if !result.Requeue {
		t.Error("expected Requeue=true after fatal deploy failure")
	}
}

// ── status management ─────────────────────────────────────────────────────────

func TestReconcile_ManageStatus_WritesConditionsOnSuccess(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)
	comp := &stubComponent{name: "w", verifyOK: true}
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	newRMWithStatus().Reconcile(context.Background(), ctrl, cr, dm, false)

	stored := dm.GetStored("default", "Foo", "test.example.com/v1alpha1", "foo")
	if stored == nil {
		t.Fatal("object not found")
	}
	st, _ := stored["status"].(map[string]any)
	if st == nil {
		t.Fatal("status not written")
	}
	readyCond := status.GetCondition(status.ConditionReady, st)
	if readyCond == nil {
		t.Fatal("Ready condition not written")
	}
	if readyCond["reason"] != string(status.ReadyStable) {
		t.Errorf("Ready reason = %q, want %q", readyCond["reason"], status.ReadyStable)
	}
}

func TestReconcile_ManageStatus_WritesErrorOnSetupFailure(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, _ *session.Session) error {
			return errors.New("config broken")
		},
	}

	newRMWithStatus().Reconcile(context.Background(), ctrl, cr, dm, false)

	stored := dm.GetStored("default", "Foo", "test.example.com/v1alpha1", "foo")
	if stored == nil {
		t.Fatal("object not found")
	}
	st, _ := stored["status"].(map[string]any)
	if st == nil {
		t.Fatal("status not written after error")
	}
	readyCond := status.GetCondition(status.ConditionReady, st)
	if readyCond == nil {
		t.Fatal("Ready condition not written")
	}
	if readyCond["reason"] != string(status.ReadyErrored) {
		t.Errorf("Ready reason = %q, want %q", readyCond["reason"], status.ReadyErrored)
	}
}

func TestReconcile_ManageStatus_VerifyWaitWhenNotReady(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)
	comp := &stubComponent{name: "w", verifyOK: false}
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}

	newRMWithStatus().Reconcile(context.Background(), ctrl, cr, dm, false)

	stored := dm.GetStored("default", "Foo", "test.example.com/v1alpha1", "foo")
	st, _ := stored["status"].(map[string]any)
	updatingCond := status.GetCondition(status.ConditionUpdating, st)
	if updatingCond == nil {
		t.Fatal("Updating condition not written")
	}
	if updatingCond["reason"] != string(status.UpdatingVerifyWait) {
		t.Errorf("Updating reason = %q, want %q", updatingCond["reason"], status.UpdatingVerifyWait)
	}
}

// ── ID generation ─────────────────────────────────────────────────────────────

func TestReconcileResult_Defaults(t *testing.T) {
	r := reconcilemanager.ReconcileResult{}
	if r.Requeue {
		t.Error("default Requeue should be false")
	}
	if r.Err != nil {
		t.Error("default Err should be nil")
	}
	if r.RequeueAfter != 0 {
		t.Error("default RequeueAfter should be zero")
	}
}

func TestReconcileResult_WithRequeueAfter(t *testing.T) {
	r := reconcilemanager.ReconcileResult{
		Requeue:      true,
		RequeueAfter: 30 * time.Second,
	}
	if r.RequeueAfter != 30*time.Second {
		t.Errorf("RequeueAfter = %v, want 30s", r.RequeueAfter)
	}
}

// ── options ───────────────────────────────────────────────────────────────────

func TestOptions_ManageStatus_Default_False(t *testing.T) {
	// When ManageStatus=false, no status writes happen even after a successful reconcile.
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	dm := newDM(cr)
	comp := &stubComponent{name: "w", verifyOK: true}
	ctrl := &stubController{
		gvk: testGVK,
		setupFunc: func(_ context.Context, sess *session.Session) error {
			return addComp(sess, comp)
		},
	}
	newRM().Reconcile(context.Background(), ctrl, cr, dm, false) // ManageStatus=false

	stored := dm.GetStored("default", "Foo", "test.example.com/v1alpha1", "foo")
	st, _ := stored["status"].(map[string]any)
	// Status may be nil or empty — key point is no conditions were written.
	conds, _ := st["conditions"].([]any)
	if len(conds) > 0 {
		t.Errorf("no status conditions should be written when ManageStatus=false, got %v", conds)
	}
}
