// Package component_test validates the Component interface contract.
package component_test

import (
	"context"
	"errors"
	"testing"

	"github.com/example/oper8-go/component"
	"github.com/example/oper8-go/session"
)

// ── Test doubles ──────────────────────────────────────────────────────────────

// fullComponent is a fully-implemented Component that records calls.
type fullComponent struct {
	name         string
	disabled     bool
	setupErr     error
	deployErr    error
	verifyResult bool
	setupCalls   int
	deployCalls  int
	verifyCalls  int
}

func (c *fullComponent) Name() string   { return c.name }
func (c *fullComponent) Disabled() bool { return c.disabled }

func (c *fullComponent) Setup(_ context.Context, _ *session.Session) error {
	c.setupCalls++
	return c.setupErr
}

func (c *fullComponent) Deploy(_ context.Context, _ *session.Session) error {
	c.deployCalls++
	return c.deployErr
}

func (c *fullComponent) Verify(_ context.Context, _ *session.Session) bool {
	c.verifyCalls++
	return c.verifyResult
}

// compile-time assertion: *fullComponent satisfies component.Component.
var _ component.Component = (*fullComponent)(nil)

// ── Interface contract tests ──────────────────────────────────────────────────

func TestComponent_Name(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "my-widget"}
	if c.Name() != "my-widget" {
		t.Errorf("Name() = %q, want %q", c.Name(), "my-widget")
	}
}

func TestComponent_Disabled_False(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "x", disabled: false}
	if c.Disabled() {
		t.Error("Disabled() should return false")
	}
}

func TestComponent_Disabled_True(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "x", disabled: true}
	if !c.Disabled() {
		t.Error("Disabled() should return true")
	}
}

func TestComponent_Setup_Success(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "x"}
	if err := c.Setup(context.Background(), nil); err != nil {
		t.Errorf("Setup returned unexpected error: %v", err)
	}
	if c.setupCalls != 1 {
		t.Errorf("Setup called %d times, want 1", c.setupCalls)
	}
}

func TestComponent_Setup_Error(t *testing.T) {
	t.Parallel()
	boom := errors.New("setup failed")
	c := &fullComponent{name: "x", setupErr: boom}
	if err := c.Setup(context.Background(), nil); !errors.Is(err, boom) {
		t.Errorf("Setup should return wrapped error, got %v", err)
	}
}

func TestComponent_Deploy_Success(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "x"}
	if err := c.Deploy(context.Background(), nil); err != nil {
		t.Errorf("Deploy returned unexpected error: %v", err)
	}
	if c.deployCalls != 1 {
		t.Errorf("Deploy called %d times, want 1", c.deployCalls)
	}
}

func TestComponent_Deploy_Error(t *testing.T) {
	t.Parallel()
	boom := errors.New("deploy failed")
	c := &fullComponent{name: "x", deployErr: boom}
	if err := c.Deploy(context.Background(), nil); !errors.Is(err, boom) {
		t.Errorf("Deploy should propagate error, got %v", err)
	}
}

func TestComponent_Verify_Ready(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "x", verifyResult: true}
	if !c.Verify(context.Background(), nil) {
		t.Error("Verify() should return true when ready")
	}
	if c.verifyCalls != 1 {
		t.Errorf("Verify called %d times, want 1", c.verifyCalls)
	}
}

func TestComponent_Verify_NotReady(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "x", verifyResult: false}
	if c.Verify(context.Background(), nil) {
		t.Error("Verify() should return false when not ready")
	}
}

func TestComponent_FullLifecycle(t *testing.T) {
	t.Parallel()
	c := &fullComponent{name: "lifecycle", verifyResult: true}
	ctx := context.Background()

	if err := c.Setup(ctx, nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := c.Deploy(ctx, nil); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !c.Verify(ctx, nil) {
		t.Fatal("Verify should return true")
	}

	if c.setupCalls != 1 || c.deployCalls != 1 || c.verifyCalls != 1 {
		t.Errorf("call counts: setup=%d deploy=%d verify=%d (all want 1)",
			c.setupCalls, c.deployCalls, c.verifyCalls)
	}
}
