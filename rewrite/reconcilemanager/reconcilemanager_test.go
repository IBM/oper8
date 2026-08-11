package reconcilemanager_test

import (
	"context"
	"testing"

	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/reconcilemanager"
	"github.com/example/oper8-go/session"
)

// ── Test helpers ──────────────────────────────────────────────────────────────

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

func (c *stubComponent) Deploy(_ context.Context, _ *session.Session) error {
	return c.deployErr
}

func (c *stubComponent) Verify(_ context.Context, _ *session.Session) bool {
	return c.verifyOK
}

// ── stubController ────────────────────────────────────────────────────────────

type stubController struct {
	controller.BaseController
	gvk       controller.GVK
	setupFunc func(ctx context.Context, sess *session.Session) error
}

func (c *stubController) GVK() controller.GVK { return c.gvk }

func (c *stubController) SetupComponents(ctx context.Context, sess *session.Session) error {
	if c.setupFunc != nil {
		return c.setupFunc(ctx, sess)
	}
	return nil
}

func (c *stubController) ShouldRequeue(_ context.Context, _ *session.Session) bool { return false }

// ── finCtrl ───────────────────────────────────────────────────────────────────

type finCtrl struct {
	*stubController
	onFinalize func(context.Context, *session.Session) error
}

func (c *finCtrl) FinalizeComponents(ctx context.Context, sess *session.Session) error {
	return c.onFinalize(ctx, sess)
}

// ── shared setup ─────────────────────────────────────────────────────────────

var testGVK = controller.GVK{Group: "test.example.com", Version: "v1alpha1", Kind: "Foo"}

func newDM(cr map[string]any) *deploymanager.DryRunDeployManager {
	return deploymanager.NewDryRunDeployManager(nil, cr)
}

func newRM() *reconcilemanager.ReconcileManager {
	return reconcilemanager.New(reconcilemanager.Options{ManageStatus: false})
}

func addComp(sess *session.Session, comp *stubComponent) error {
	n := dag.NewFuncNode(comp.Name(), nil)
	n.SetData(comp)
	return sess.AddComponent(n)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestReconcile_EmptyGraph(t *testing.T) {
	cr := minimalCR("foo", "default", "Foo", "test.example.com/v1alpha1")
	result := newRM().Reconcile(context.Background(), &stubController{gvk: testGVK}, cr, newDM(cr), false)
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

	// Deploy error → fatal HaltError → failed node → Requeue=true.
	if !result.Requeue {
		t.Error("expected Requeue=true after deploy error")
	}
}

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

	// Not-ready verify is not an error — just incomplete.
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
}

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
		t.Fatalf("precondition should requeue cleanly (no error), got: %v", result.Err)
	}
	if !result.Requeue {
		t.Error("expected Requeue=true when precondition fails")
	}
	if setupCalled {
		t.Error("SetupComponents must not be called when a precondition fails")
	}
}

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

func TestReconcile_InvalidCR(t *testing.T) {
	badCR := map[string]any{"kind": "Foo"} // missing apiVersion + metadata
	dm := deploymanager.NewDryRunDeployManager(nil)

	result := newRM().Reconcile(context.Background(), &stubController{gvk: testGVK}, badCR, dm, false)

	if result.Err == nil {
		t.Fatal("expected error for malformed CR, got nil")
	}
}

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
