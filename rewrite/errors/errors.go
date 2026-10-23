// Package errors defines the oper8-go error hierarchy.
//
// Ported from oper8 Python (exceptions.py).
//
// Errors are divided into two categories:
//
//   - Fatal errors (ConfigError, ClusterError, RolloutError) signal that the
//     reconcile should not be retried automatically; the operator author must
//     fix the root cause. These set an error condition on the CR status.
//
//   - Transient errors (PreconditionError, VerificationError) are expected
//     conditions that will resolve on their own. ReconcileManager treats them
//     as a clean requeue — no error condition is written to the CR.
//
// Usage:
//
//	// fatal — propagated to the status condition as an error
//	return errors.NewConfigError("spec.version must not be empty")
//
//	// transient — clean requeue, status shows "waiting"
//	return errors.NewPreconditionError("database not yet ready")
//
//	// helper assertions (mirrors Python assert_config, assert_cluster, etc.)
//	if err := errors.AssertConfig(cfg.Version != "", "spec.version required"); err != nil {
//	    return err
//	}
package errors

import "fmt"

// ── base ──────────────────────────────────────────────────────────────────────

// Oper8Error is the base type for all oper8-go errors.
// Use IsFatal to distinguish fatal from transient errors.
type Oper8Error struct {
	msg   string
	fatal bool
}

func (e *Oper8Error) Error() string { return e.msg }

// IsFatal reports whether this error represents a fatal (non-retryable)
// failure.
func (e *Oper8Error) IsFatal() bool { return e.fatal }

// fatalChecker is the interface implemented by all oper8 error types.
type fatalChecker interface {
	IsFatal() bool
}

// IsFatal returns true if err is an oper8 error with IsFatal() == true.
// Returns false for nil and non-Oper8Error errors (treat as transient).
func IsFatal(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(fatalChecker); ok {
		return e.IsFatal()
	}
	return false
}

// ── fatal errors ──────────────────────────────────────────────────────────────

// ConfigError signals that user-provided configuration (spec, annotations, etc.)
// is invalid. Fatal: requires operator-author intervention.
type ConfigError struct{ Oper8Error }

// NewConfigError returns a new ConfigError.
func NewConfigError(format string, args ...any) *ConfigError {
	return &ConfigError{Oper8Error{msg: fmt.Sprintf(format, args...), fatal: true}}
}

// ClusterError signals that a required cluster operation failed unexpectedly.
// Fatal: the cluster may be in an inconsistent state.
type ClusterError struct{ Oper8Error }

// NewClusterError returns a new ClusterError.
func NewClusterError(format string, args ...any) *ClusterError {
	return &ClusterError{Oper8Error{msg: fmt.Sprintf(format, args...), fatal: true}}
}

// RolloutError signals that the DAG rollout itself failed fatally.
// Typically wraps the underlying cause.
type RolloutError struct{ Oper8Error }

// NewRolloutError returns a new RolloutError.
func NewRolloutError(format string, args ...any) *RolloutError {
	return &RolloutError{Oper8Error{msg: fmt.Sprintf(format, args...), fatal: true}}
}

// ── transient errors ──────────────────────────────────────────────────────────

// PreconditionError signals that a required precondition is not yet satisfied.
// Transient: ReconcileManager will requeue cleanly without writing an error status.
type PreconditionError struct{ Oper8Error }

// NewPreconditionError returns a new PreconditionError.
func NewPreconditionError(format string, args ...any) *PreconditionError {
	return &PreconditionError{Oper8Error{msg: fmt.Sprintf(format, args...), fatal: false}}
}

// VerificationError signals that a resource is not yet in its desired state.
// Transient: the reconcile will be requeued until verify passes.
type VerificationError struct{ Oper8Error }

// NewVerificationError returns a new VerificationError.
func NewVerificationError(format string, args ...any) *VerificationError {
	return &VerificationError{Oper8Error{msg: fmt.Sprintf(format, args...), fatal: false}}
}

// ── assertion helpers ─────────────────────────────────────────────────────────

// AssertConfig returns a ConfigError if condition is false.
// Mirrors Python's assert_config.
func AssertConfig(condition bool, format string, args ...any) error {
	if !condition {
		return NewConfigError(format, args...)
	}
	return nil
}

// AssertCluster returns a ClusterError if condition is false.
// Mirrors Python's assert_cluster.
func AssertCluster(condition bool, format string, args ...any) error {
	if !condition {
		return NewClusterError(format, args...)
	}
	return nil
}

// AssertPrecondition returns a PreconditionError if condition is false.
// Mirrors Python's assert_precondition.
func AssertPrecondition(condition bool, format string, args ...any) error {
	if !condition {
		return NewPreconditionError(format, args...)
	}
	return nil
}

// AssertVerified returns a VerificationError if condition is false.
// Mirrors Python's assert_verified.
func AssertVerified(condition bool, format string, args ...any) error {
	if !condition {
		return NewVerificationError(format, args...)
	}
	return nil
}
