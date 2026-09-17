package dag

import (
	"fmt"
	"strings"
)

// NodeState classifies how a node terminated.
type NodeState int

const (
	NodeStateVerified   NodeState = iota // deployed and verified
	NodeStateUnverified                  // deployed but not yet verified (HaltError{Fatal:false})
	NodeStateFailed                      // fatal error during deploy or verify
	NodeStateUnstarted                   // never attempted (upstream failed)
)

// CompletionState captures the terminal state of every node after a
// [Runner.Run] call. It mirrors oper8's Python CompletionState.
type CompletionState struct {
	// Verified holds nodes that completed both deploy and verify.
	Verified []*Node
	// Unverified holds nodes that deployed successfully but did not pass
	// verification (HaltError with Fatal=false).
	Unverified []*Node
	// Failed holds nodes that hit a fatal error.
	Failed []*Node
	// Unstarted holds nodes that were never attempted because an upstream
	// failed.
	Unstarted []*Node
	// Err is the exception from a fatal HaltError, if any.
	Err error
}

// DeployCompleted reports whether the deploy phase finished without any
// failed or unstarted nodes. An empty graph is considered completed.
func (cs *CompletionState) DeployCompleted() bool {
	return len(cs.Failed) == 0 && len(cs.Unstarted) == 0
}

// VerifyCompleted reports whether every node was verified and there was no
// fatal error.
func (cs *CompletionState) VerifyCompleted() bool {
	return len(cs.Unverified) == 0 &&
		len(cs.Failed) == 0 &&
		len(cs.Unstarted) == 0 &&
		cs.Err == nil
}

// Failed reports whether any node failed or a fatal error occurred.
func (cs *CompletionState) AnyFailed() bool {
	return len(cs.Failed) > 0 || cs.isFatalErr()
}

func (cs *CompletionState) isFatalErr() bool {
	if cs.Err == nil {
		return false
	}
	// A HaltError with Fatal=false is not a fatal error at the graph level.
	var h *HaltError
	if asHaltError(cs.Err, &h) {
		return h.Fatal
	}
	return true
}

// String returns a multi-line summary useful for logging.
func (cs *CompletionState) String() string {
	lines := []string{
		fmt.Sprintf("[NODES] Verified:   %s", nodeNames(cs.Verified)),
		fmt.Sprintf("[NODES] Unverified: %s", nodeNames(cs.Unverified)),
		fmt.Sprintf("[NODES] Failed:     %s", nodeNames(cs.Failed)),
		fmt.Sprintf("[NODES] Unstarted:  %s", nodeNames(cs.Unstarted)),
		fmt.Sprintf("Exception: %v", cs.Err),
	}
	return strings.Join(lines, "\n")
}

func nodeNames(nodes []*Node) string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.name
	}
	return "[" + strings.Join(names, ", ") + "]"
}

// ── HaltError ────────────────────────────────────────────────────────────────

// HaltError is returned by a [NodeFunc] to signal the Runner to stop the
// graph. If Fatal is true the node is placed in Failed and all unstarted
// downstream nodes stay Unstarted. If Fatal is false the node is placed in
// Unverified instead.
type HaltError struct {
	Fatal bool
	Cause error
}

func (e *HaltError) Error() string {
	return fmt.Sprintf("HaltError(fatal=%v): %v", e.Fatal, e.Cause)
}

func (e *HaltError) Unwrap() error { return e.Cause }

// asHaltError is a small helper that avoids importing errors in this file.
func asHaltError(err error, target **HaltError) bool {
	if h, ok := err.(*HaltError); ok {
		*target = h
		return true
	}
	return false
}
