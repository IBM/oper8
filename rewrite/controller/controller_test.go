package controller_test

import (
	"context"
	"testing"

	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/session"
)

// concreteController embeds BaseController and satisfies the full interface.
type concreteController struct {
	controller.BaseController
	gvk           controller.GVK
	setupFunc     func(context.Context, *session.Session) error
	shouldRequeue bool
	hasFinalizer  bool
	finalizerStr  string
}

func (c *concreteController) GVK() controller.GVK { return c.gvk }

func (c *concreteController) SetupComponents(ctx context.Context, sess *session.Session) error {
	if c.setupFunc != nil {
		return c.setupFunc(ctx, sess)
	}
	return nil
}

func (c *concreteController) ShouldRequeue(_ context.Context, _ *session.Session) bool {
	return c.shouldRequeue
}

func (c *concreteController) HasFinalizer() bool { return c.hasFinalizer }
func (c *concreteController) Finalizer() string  { return c.finalizerStr }

// ── GVK ──────────────────────────────────────────────────────────────────────

func TestGVK_Fields(t *testing.T) {
	gvk := controller.GVK{Group: "mygroup.io", Version: "v1beta1", Kind: "Widget"}
	if gvk.Group != "mygroup.io" {
		t.Errorf("Group = %q", gvk.Group)
	}
	if gvk.Version != "v1beta1" {
		t.Errorf("Version = %q", gvk.Version)
	}
	if gvk.Kind != "Widget" {
		t.Errorf("Kind = %q", gvk.Kind)
	}
}

// ── HookResult ───────────────────────────────────────────────────────────────

func TestOK_ReturnsOKResult(t *testing.T) {
	r := controller.OK()
	if !r.OK {
		t.Error("OK() should return HookResult{OK: true}")
	}
	if r.Err != nil {
		t.Errorf("OK() should have nil Err, got %v", r.Err)
	}
}

func TestHookResult_Failure(t *testing.T) {
	r := controller.HookResult{OK: false}
	if r.OK {
		t.Error("HookResult{OK:false} should not be OK")
	}
}

// ── BaseController defaults ───────────────────────────────────────────────────

func TestBaseController_FinalizeComponents_NoOp(t *testing.T) {
	ctrl := &concreteController{}
	if err := ctrl.FinalizeComponents(context.Background(), nil); err != nil {
		t.Errorf("FinalizeComponents should be a no-op, got %v", err)
	}
}

func TestBaseController_AfterDeploy_ReturnsOK(t *testing.T) {
	ctrl := &concreteController{}
	r := ctrl.AfterDeploy(context.Background(), nil, &dag.CompletionState{})
	if !r.OK {
		t.Error("AfterDeploy should return OK()")
	}
}

func TestBaseController_AfterDeployUnsuccessful_ReturnsOK(t *testing.T) {
	ctrl := &concreteController{}
	r := ctrl.AfterDeployUnsuccessful(context.Background(), nil, false, &dag.CompletionState{})
	if !r.OK {
		t.Error("AfterDeployUnsuccessful should return OK()")
	}
}

func TestBaseController_AfterVerify_ReturnsOK(t *testing.T) {
	ctrl := &concreteController{}
	r := ctrl.AfterVerify(context.Background(), nil, &dag.CompletionState{}, &dag.CompletionState{})
	if !r.OK {
		t.Error("AfterVerify should return OK()")
	}
}

func TestBaseController_AfterVerifyUnsuccessful_ReturnsOK(t *testing.T) {
	ctrl := &concreteController{}
	r := ctrl.AfterVerifyUnsuccessful(context.Background(), nil, false, &dag.CompletionState{}, &dag.CompletionState{})
	if !r.OK {
		t.Error("AfterVerifyUnsuccessful should return OK()")
	}
}

func TestBaseController_ShouldRequeue_DefaultTrue(t *testing.T) {
	var b controller.BaseController
	if !b.ShouldRequeue(context.Background(), nil) {
		t.Error("BaseController.ShouldRequeue should return true by default")
	}
}

func TestBaseController_HasFinalizer_DefaultFalse(t *testing.T) {
	var b controller.BaseController
	if b.HasFinalizer() {
		t.Error("BaseController.HasFinalizer should return false by default")
	}
}

func TestBaseController_Finalizer_DefaultEmpty(t *testing.T) {
	var b controller.BaseController
	if b.Finalizer() != "" {
		t.Errorf("BaseController.Finalizer should return empty, got %q", b.Finalizer())
	}
}

// ── override behaviour ────────────────────────────────────────────────────────

func TestController_ShouldRequeue_Override(t *testing.T) {
	ctrl := &concreteController{shouldRequeue: false}
	if ctrl.ShouldRequeue(context.Background(), nil) {
		t.Error("overridden ShouldRequeue=false should return false")
	}
}

func TestController_HasFinalizer_Override(t *testing.T) {
	ctrl := &concreteController{hasFinalizer: true}
	if !ctrl.HasFinalizer() {
		t.Error("overridden HasFinalizer=true should return true")
	}
}

func TestController_Finalizer_Override(t *testing.T) {
	ctrl := &concreteController{hasFinalizer: true, finalizerStr: "finalizers.k.g"}
	if ctrl.Finalizer() != "finalizers.k.g" {
		t.Errorf("Finalizer() = %q, want %q", ctrl.Finalizer(), "finalizers.k.g")
	}
}
