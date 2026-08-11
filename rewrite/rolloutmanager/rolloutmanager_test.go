package rolloutmanager_test

import (
	"context"
	"errors"
	"testing"

	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/rolloutmanager"
	"github.com/example/oper8-go/session"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func minCR(name, ns string) map[string]any {
	return map[string]any{
		"apiVersion": "test.io/v1",
		"kind":       "Foo",
		"metadata":   map[string]any{"name": name, "namespace": ns, "uid": "u1"},
	}
}

func newSess(t *testing.T, cr map[string]any) *session.Session {
	t.Helper()
	dm := deploymanager.NewDryRunDeployManager(nil, cr)
	sess, err := session.New(context.Background(), "id", cr, dm)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	return sess
}

// stubComp is a fully-controllable Component for rolloutmanager tests.
type stubComp struct {
	name         string
	setupErr     error
	deployErr    error
	verifyOK     bool
	setupCalled  int
	deployCalled int
	verifyCalled int
}

func (c *stubComp) Name() string { return c.name }
func (c *stubComp) Setup(_ context.Context, _ *session.Session) error {
	c.setupCalled++
	return c.setupErr
}
func (c *stubComp) Deploy(_ context.Context, _ *session.Session) error {
	c.deployCalled++
	return c.deployErr
}
func (c *stubComp) Verify(_ context.Context, _ *session.Session) bool {
	c.verifyCalled++
	return c.verifyOK
}

// hookCtrl lets tests inject hook behaviour while delegating everything else
// to BaseController.
type hookCtrl struct {
	controller.BaseController
	afterDeploy             func() controller.HookResult
	afterDeployUnsuccessful func() controller.HookResult
	afterVerify             func() controller.HookResult
	afterVerifyUnsuccessful func() controller.HookResult
}

func (c *hookCtrl) GVK() controller.GVK                                         { return controller.GVK{} }
func (c *hookCtrl) SetupComponents(_ context.Context, _ *session.Session) error { return nil }
func (c *hookCtrl) ShouldRequeue(_ context.Context, _ *session.Session) bool    { return false }

func (c *hookCtrl) AfterDeploy(_ context.Context, _ *session.Session, _ *dag.CompletionState) controller.HookResult {
	if c.afterDeploy != nil {
		return c.afterDeploy()
	}
	return controller.OK()
}
func (c *hookCtrl) AfterDeployUnsuccessful(_ context.Context, _ *session.Session, _ bool, _ *dag.CompletionState) controller.HookResult {
	if c.afterDeployUnsuccessful != nil {
		return c.afterDeployUnsuccessful()
	}
	return controller.OK()
}
func (c *hookCtrl) AfterVerify(_ context.Context, _ *session.Session, _, _ *dag.CompletionState) controller.HookResult {
	if c.afterVerify != nil {
		return c.afterVerify()
	}
	return controller.OK()
}
func (c *hookCtrl) AfterVerifyUnsuccessful(_ context.Context, _ *session.Session, _ bool, _, _ *dag.CompletionState) controller.HookResult {
	if c.afterVerifyUnsuccessful != nil {
		return c.afterVerifyUnsuccessful()
	}
	return controller.OK()
}

func addComp(t *testing.T, sess *session.Session, comp *stubComp) *dag.Node {
	t.Helper()
	n := dag.NewFuncNode(comp.Name(), nil)
	n.SetData(comp)
	if err := sess.AddComponent(n); err != nil {
		t.Fatalf("AddComponent(%q): %v", comp.Name(), err)
	}
	return n
}

func rollout(sess *session.Session, ctrl controller.Controller) *dag.CompletionState {
	return rolloutmanager.New(sess, ctrl, 0).Rollout(context.Background())
}

// ── happy path ────────────────────────────────────────────────────────────────

func TestRollout_EmptyGraph(t *testing.T) {
	cs := rollout(newSess(t, minCR("app", "ns")), &hookCtrl{})
	if !cs.VerifyCompleted() {
		t.Errorf("empty graph should be verify-complete; got %s", cs)
	}
}

func TestRollout_SingleComponentHappyPath(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "widget", verifyOK: true}
	addComp(t, sess, comp)

	cs := rollout(sess, &hookCtrl{})

	if !cs.VerifyCompleted() {
		t.Errorf("expected verify-complete; state: %s", cs)
	}
	if comp.setupCalled != 1 {
		t.Errorf("Setup called %d times, want 1", comp.setupCalled)
	}
	if comp.deployCalled != 1 {
		t.Errorf("Deploy called %d times, want 1", comp.deployCalled)
	}
	if comp.verifyCalled != 1 {
		t.Errorf("Verify called %d times, want 1", comp.verifyCalled)
	}
}

// ── deploy failures ───────────────────────────────────────────────────────────

func TestRollout_SetupError_FatalFailure(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "widget", setupErr: errors.New("boom")})

	cs := rollout(sess, &hookCtrl{})
	if !cs.AnyFailed() {
		t.Error("expected failed state after setup error")
	}
}

func TestRollout_DeployError_FatalFailure(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "widget", deployErr: errors.New("deploy-boom")})

	cs := rollout(sess, &hookCtrl{})
	if !cs.AnyFailed() {
		t.Error("expected failed state after deploy error")
	}
}

func TestRollout_DeployError_BlocksDownstream(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	compA := &stubComp{name: "a", deployErr: errors.New("a-fails")}
	compB := &stubComp{name: "b", verifyOK: true}
	nA := addComp(t, sess, compA)
	nB := addComp(t, sess, compB)
	_ = sess.AddDependency(nB, nA, nil)

	rollout(sess, &hookCtrl{})

	if compB.deployCalled > 0 {
		t.Error("B should not deploy when its upstream A fails")
	}
}

func TestRollout_IndependentBranchRunsDespiteFailure(t *testing.T) {
	// A fails; B is independent — B should still run.
	sess := newSess(t, minCR("app", "ns"))
	compA := &stubComp{name: "a", deployErr: errors.New("a-fails")}
	compB := &stubComp{name: "b", verifyOK: true}
	addComp(t, sess, compA)
	addComp(t, sess, compB)

	rollout(sess, &hookCtrl{})

	if compB.deployCalled == 0 {
		t.Error("independent component B should still deploy even when A fails")
	}
}

// ── verify incomplete ─────────────────────────────────────────────────────────

func TestRollout_VerifyNotReady_NonFatal(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "widget", verifyOK: false})

	cs := rollout(sess, &hookCtrl{})
	if cs.AnyFailed() {
		t.Error("verify-not-ready should not be a fatal failure")
	}
	if cs.VerifyCompleted() {
		t.Error("verify should not be complete when component returns false")
	}
	if len(cs.Unverified) == 0 {
		t.Error("widget should be in Unverified")
	}
}

func TestRollout_VerifyNotReady_BlocksDownstream(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	compA := &stubComp{name: "a", verifyOK: false}
	compB := &stubComp{name: "b", verifyOK: true}
	nA := addComp(t, sess, compA)
	nB := addComp(t, sess, compB)
	_ = sess.AddDependency(nB, nA, nil)

	rollout(sess, &hookCtrl{})

	if compB.verifyCalled > 0 {
		t.Error("B verify should be blocked when A is not verified")
	}
}

// ── after-deploy hooks ────────────────────────────────────────────────────────

func TestRollout_AfterDeploy_CalledOnSuccess(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "w", verifyOK: true})

	called := false
	rollout(sess, &hookCtrl{
		afterDeploy: func() controller.HookResult {
			called = true
			return controller.OK()
		},
	})
	if !called {
		t.Error("AfterDeploy should be called when deploy succeeds")
	}
}

func TestRollout_AfterDeploy_NotCalledOnFailure(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "w", deployErr: errors.New("fail")})

	afterDeployCalled := false
	afterUnsuccessfulCalled := false
	rollout(sess, &hookCtrl{
		afterDeploy: func() controller.HookResult {
			afterDeployCalled = true
			return controller.OK()
		},
		afterDeployUnsuccessful: func() controller.HookResult {
			afterUnsuccessfulCalled = true
			return controller.OK()
		},
	})
	if afterDeployCalled {
		t.Error("AfterDeploy must NOT be called when deploy fails")
	}
	if !afterUnsuccessfulCalled {
		t.Error("AfterDeployUnsuccessful must be called when deploy fails")
	}
}

func TestRollout_AfterDeploy_ErrorPropagated(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "w", verifyOK: true})

	cs := rollout(sess, &hookCtrl{
		afterDeploy: func() controller.HookResult {
			return controller.HookResult{OK: false, Err: errors.New("hook-err")}
		},
	})
	if cs.Err == nil {
		t.Error("expected Err to be set when after-deploy hook returns an error")
	}
}

// ── after-verify hooks ────────────────────────────────────────────────────────

func TestRollout_AfterVerify_CalledWhenFullyVerified(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "w", verifyOK: true})

	called := false
	rollout(sess, &hookCtrl{
		afterVerify: func() controller.HookResult {
			called = true
			return controller.OK()
		},
	})
	if !called {
		t.Error("AfterVerify should be called when all components verify")
	}
}

func TestRollout_AfterVerify_NotCalledWhenIncomplete(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "w", verifyOK: false})

	afterVerifyCalled := false
	afterUnsuccessfulCalled := false
	rollout(sess, &hookCtrl{
		afterVerify: func() controller.HookResult {
			afterVerifyCalled = true
			return controller.OK()
		},
		afterVerifyUnsuccessful: func() controller.HookResult {
			afterUnsuccessfulCalled = true
			return controller.OK()
		},
	})
	if afterVerifyCalled {
		t.Error("AfterVerify must NOT be called when verify is incomplete")
	}
	if !afterUnsuccessfulCalled {
		t.Error("AfterVerifyUnsuccessful must be called when verify is incomplete")
	}
}

func TestRollout_AfterVerify_ErrorPropagated(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	addComp(t, sess, &stubComp{name: "w", verifyOK: true})

	cs := rollout(sess, &hookCtrl{
		afterVerify: func() controller.HookResult {
			return controller.HookResult{OK: false, Err: errors.New("verify-hook-err")}
		},
	})
	if cs.Err == nil {
		t.Error("expected Err from after-verify error")
	}
}

// ── multiple components ───────────────────────────────────────────────────────

func TestRollout_ThreeComponents_AllVerified(t *testing.T) {
	sess := newSess(t, minCR("app", "ns"))
	comps := []*stubComp{
		{name: "a", verifyOK: true},
		{name: "b", verifyOK: true},
		{name: "c", verifyOK: true},
	}
	for _, c := range comps {
		addComp(t, sess, c)
	}

	cs := rollout(sess, &hookCtrl{})
	if !cs.VerifyCompleted() {
		t.Errorf("expected all verified; state: %s", cs)
	}
	for _, c := range comps {
		if c.setupCalled != 1 {
			t.Errorf("%s Setup called %d times", c.name, c.setupCalled)
		}
	}
}

// ── concurrent execution ──────────────────────────────────────────────────────

func TestRollout_Concurrent_AllVerified(t *testing.T) {
	cr := minCR("app", "ns")
	dm := deploymanager.NewDryRunDeployManager(nil, cr)
	sess, _ := session.New(context.Background(), "id", cr, dm)

	for _, name := range []string{"a", "b", "c"} {
		comp := &stubComp{name: name, verifyOK: true}
		n := dag.NewFuncNode(comp.Name(), nil)
		n.SetData(comp)
		_ = sess.AddComponent(n)
	}

	cs := rolloutmanager.New(sess, &hookCtrl{}, 4).Rollout(context.Background())
	if !cs.VerifyCompleted() {
		t.Errorf("concurrent rollout should verify all; state: %s", cs)
	}
}
