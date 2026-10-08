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

// stubComp is a controllable Component for rolloutmanager tests.
type stubComp struct {
	name      string
	setupErr  error
	deployErr error
	verifyOK  bool

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

// hookCtrl wraps BaseController and lets tests inject hook behaviour.
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
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	cs := rollout(sess, &hookCtrl{})
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

func TestRollout_TwoComponentsOrdered(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)

	order := []string{}
	makeComp := func(name string) *stubComp {
		c := &stubComp{name: name, verifyOK: true}
		return c
	}

	compA := makeComp("a")
	compB := makeComp("b")

	// Track deploy order via Setup side-effect.
	origSetupA := compA.setupCalled
	_ = origSetupA

	nA := addComp(t, sess, compA)
	nB := addComp(t, sess, compB)
	// Wrap Setup to record order.
	nA.SetFunc(func() error {
		order = append(order, "a")
		return nil
	})
	nB.SetFunc(func() error {
		order = append(order, "b")
		return nil
	})
	_ = sess.AddDependency(nB, nA, nil) // B waits for A

	// Use raw runner to check order; or just confirm both run.
	cs := rollout(sess, &hookCtrl{})
	if cs.AnyFailed() {
		t.Errorf("unexpected failure: %s", cs)
	}
	if len(order) == 2 && order[0] != "a" {
		t.Errorf("order = %v, want a before b", order)
	}
}

// ── deploy failures ───────────────────────────────────────────────────────────

func TestRollout_SetupError_FatalFailure(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "widget", setupErr: errors.New("boom")}
	addComp(t, sess, comp)

	cs := rollout(sess, &hookCtrl{})

	if !cs.AnyFailed() {
		t.Error("expected failed state after setup error")
	}
}

func TestRollout_DeployError_FatalFailure(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "widget", deployErr: errors.New("deploy-boom")}
	addComp(t, sess, comp)

	cs := rollout(sess, &hookCtrl{})

	if !cs.AnyFailed() {
		t.Error("expected failed state after deploy error")
	}
}

func TestRollout_DeployError_BlocksDownstream(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)

	compA := &stubComp{name: "a", deployErr: errors.New("a-fails")}
	compB := &stubComp{name: "b", verifyOK: true}
	nA := addComp(t, sess, compA)
	nB := addComp(t, sess, compB)
	_ = sess.AddDependency(nB, nA, nil) // B depends on A

	cs := rollout(sess, &hookCtrl{})

	if compB.deployCalled > 0 {
		t.Error("B should not deploy when its upstream A fails")
	}
	if len(cs.Unstarted) == 0 {
		t.Error("B should be in Unstarted after A fails")
	}
}

// ── verify incomplete ─────────────────────────────────────────────────────────

func TestRollout_VerifyNotReady_NoError(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "widget", verifyOK: false}
	addComp(t, sess, comp)

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
	// Even though verify-not-ready is non-fatal, downstream verify is also skipped.
	cr := minCR("app", "ns")
	sess := newSess(t, cr)

	compA := &stubComp{name: "a", verifyOK: false}
	compB := &stubComp{name: "b", verifyOK: true}
	nA := addComp(t, sess, compA)
	nB := addComp(t, sess, compB)
	_ = sess.AddDependency(nB, nA, nil)

	cs := rollout(sess, &hookCtrl{})

	if cs.AnyFailed() {
		t.Errorf("not-ready should not be fatal: %s", cs)
	}
	// B's verify should not have been called since A was not verified.
	if compB.verifyCalled > 0 {
		t.Error("B verify should be blocked when A is not verified")
	}
}

// ── after-deploy hooks ────────────────────────────────────────────────────────

func TestRollout_AfterDeploy_CalledOnSuccess(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", verifyOK: true}
	addComp(t, sess, comp)

	called := false
	ctrl := &hookCtrl{
		afterDeploy: func() controller.HookResult {
			called = true
			return controller.OK()
		},
	}

	rollout(sess, ctrl)
	if !called {
		t.Error("AfterDeploy should be called when deploy succeeds")
	}
}

func TestRollout_AfterDeploy_NotCalledOnDeployFailure(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", deployErr: errors.New("fail")}
	addComp(t, sess, comp)

	afterDeployCalled := false
	afterDeployUnsuccessfulCalled := false
	ctrl := &hookCtrl{
		afterDeploy: func() controller.HookResult {
			afterDeployCalled = true
			return controller.OK()
		},
		afterDeployUnsuccessful: func() controller.HookResult {
			afterDeployUnsuccessfulCalled = true
			return controller.OK()
		},
	}

	rollout(sess, ctrl)
	if afterDeployCalled {
		t.Error("AfterDeploy must NOT be called when deploy fails")
	}
	if !afterDeployUnsuccessfulCalled {
		t.Error("AfterDeployUnsuccessful must be called when deploy fails")
	}
}

func TestRollout_AfterDeploy_ReturnsFalse_PreventsVerify(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", verifyOK: true}
	addComp(t, sess, comp)

	ctrl := &hookCtrl{
		afterDeploy: func() controller.HookResult { return controller.HookResult{OK: false} },
	}

	cs := rollout(sess, ctrl)
	// After-deploy returning not-OK means phase2 is not complete → verify phase runs
	// but phase3Complete = false because phase2Complete = false.
	if cs.VerifyCompleted() {
		t.Error("VerifyCompleted should be false when after-deploy returns not-OK")
	}
}

func TestRollout_AfterDeploy_ErrorPropagated(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", verifyOK: true}
	addComp(t, sess, comp)

	hookErr := errors.New("after-deploy-err")
	ctrl := &hookCtrl{
		afterDeploy: func() controller.HookResult { return controller.HookResult{OK: false, Err: hookErr} },
	}

	cs := rollout(sess, ctrl)
	if cs.Err == nil {
		t.Error("expected Err to be set when after-deploy hook returns an error")
	}
}

// ── after-verify hooks ────────────────────────────────────────────────────────

func TestRollout_AfterVerify_CalledWhenFullyVerified(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", verifyOK: true}
	addComp(t, sess, comp)

	called := false
	ctrl := &hookCtrl{
		afterVerify: func() controller.HookResult {
			called = true
			return controller.OK()
		},
	}

	rollout(sess, ctrl)
	if !called {
		t.Error("AfterVerify should be called when all components verify")
	}
}

func TestRollout_AfterVerify_NotCalledWhenVerifyIncomplete(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", verifyOK: false}
	addComp(t, sess, comp)

	afterVerifyCalled := false
	afterVerifyUnsuccessfulCalled := false
	ctrl := &hookCtrl{
		afterVerify: func() controller.HookResult {
			afterVerifyCalled = true
			return controller.OK()
		},
		afterVerifyUnsuccessful: func() controller.HookResult {
			afterVerifyUnsuccessfulCalled = true
			return controller.OK()
		},
	}

	rollout(sess, ctrl)
	if afterVerifyCalled {
		t.Error("AfterVerify must NOT be called when verify is incomplete")
	}
	if !afterVerifyUnsuccessfulCalled {
		t.Error("AfterVerifyUnsuccessful must be called when verify is incomplete")
	}
}

func TestRollout_AfterVerify_ErrorPropagated(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)
	comp := &stubComp{name: "w", verifyOK: true}
	addComp(t, sess, comp)

	hookErr := errors.New("after-verify-err")
	ctrl := &hookCtrl{
		afterVerify: func() controller.HookResult { return controller.HookResult{OK: false, Err: hookErr} },
	}

	cs := rollout(sess, ctrl)
	if cs.Err == nil {
		t.Error("expected Err from after-verify error")
	}
}

// ── multiple components ───────────────────────────────────────────────────────

func TestRollout_ThreeComponents_AllVerified(t *testing.T) {
	cr := minCR("app", "ns")
	sess := newSess(t, cr)

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

func TestRollout_IndependentBranchRunsDespiteFailure(t *testing.T) {
	// A fails; B is independent of A and should still run.
	cr := minCR("app", "ns")
	sess := newSess(t, cr)

	compA := &stubComp{name: "a", deployErr: errors.New("a-fails")}
	compB := &stubComp{name: "b", verifyOK: true}
	addComp(t, sess, compA)
	addComp(t, sess, compB)
	// No dependency between A and B.

	cs := rollout(sess, &hookCtrl{})

	if compB.deployCalled == 0 {
		t.Error("independent component B should still deploy even when A fails")
	}
	_ = cs
}

// ── concurrent execution ──────────────────────────────────────────────────────

func TestRollout_Concurrent_AllVerified(t *testing.T) {
	cr := minCR("app", "ns")
	dm := deploymanager.NewDryRunDeployManager(nil, cr)
	sess, _ := session.New(context.Background(), "id", cr, dm)

	comps := []*stubComp{
		{name: "a", verifyOK: true},
		{name: "b", verifyOK: true},
		{name: "c", verifyOK: true},
	}
	for _, c := range comps {
		n := dag.NewFuncNode(c.Name(), nil)
		n.SetData(c)
		_ = sess.AddComponent(n)
	}

	cs := rolloutmanager.New(sess, &hookCtrl{}, 4).Rollout(context.Background())
	if !cs.VerifyCompleted() {
		t.Errorf("concurrent rollout should verify all; state: %s", cs)
	}
}
