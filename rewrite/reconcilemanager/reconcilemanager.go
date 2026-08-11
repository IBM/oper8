// Package reconcilemanager is the top-level orchestrator for a single
// oper8-go reconcile pass.
//
// Ported from oper8 Python (reconcile.py).
//
// Responsibilities:
//  1. Generate a unique reconciliation ID.
//  2. Construct the Session (fetches current cluster status).
//  3. Manage CR finalizers.
//  4. Run precondition checks.
//  5. Call controller.SetupComponents (or FinalizeComponents).
//  6. Drive the RolloutManager 4-phase loop.
//  7. Update CR status conditions.
//  8. Return a ReconcileResult indicating whether to requeue.
//
// Design decisions vs Python:
//   - Python ReconcileManager handled VCS checkout, logging config, and dynamic
//     controller reimport. The Go version receives a fully-constructed
//     controller.Controller — no reflection, no module reloading.
//   - Preconditions are a []PreconditionFunc slice instead of decorators.
//   - controller-runtime is not imported here; the thin adapter lives in a
//     future rewrite/reconciler/ package.
package reconcilemanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/rolloutmanager"
	"github.com/example/oper8-go/session"
	"github.com/example/oper8-go/status"
)

// PreconditionFunc is checked before the rollout begins.
// A non-nil return aborts the reconcile and sets status to PreconditionWait.
type PreconditionFunc func(ctx context.Context, sess *session.Session) error

// ReconcileResult is returned by Reconcile.
type ReconcileResult struct {
	// Requeue indicates the reconciler should re-enqueue this request.
	Requeue bool
	// RequeueAfter, when positive, requests a timed re-enqueue.
	RequeueAfter time.Duration
	// Err is any unrecoverable error that terminated the reconcile.
	Err error
}

// Options configures a ReconcileManager.
type Options struct {
	// Preconditions are run in order before SetupComponents.
	Preconditions []PreconditionFunc
	// Concurrency controls the DAG runner goroutine pool size (0 = serial).
	Concurrency int
	// ManageStatus controls whether status conditions are written to the CR.
	// Set false in unit tests to avoid needing a fully-seeded cluster store.
	ManageStatus bool
}

// ReconcileManager orchestrates a single reconcile pass.
type ReconcileManager struct {
	opts Options
}

// New creates a ReconcileManager with the given options.
func New(opts Options) *ReconcileManager {
	return &ReconcileManager{opts: opts}
}

// Reconcile runs the full reconcile lifecycle for the given CR and controller.
// isFinalizer=true skips SetupComponents and calls FinalizeComponents instead.
func (rm *ReconcileManager) Reconcile(
	ctx context.Context,
	ctrl controller.Controller,
	crManifest map[string]any,
	dm deploymanager.DeployManager,
	isFinalizer bool,
) ReconcileResult {
	id := generateID()

	// 1. Construct session.
	sess, err := session.New(ctx, id, crManifest, dm)
	if err != nil {
		return ReconcileResult{Requeue: true, Err: fmt.Errorf("reconcile: session init: %w", err)}
	}

	// 2. Manage finalizers.
	if ctrl.HasFinalizer() && !isFinalizer {
		if err := addFinalizer(ctx, sess, ctrl.Finalizer()); err != nil {
			return ReconcileResult{Requeue: true, Err: err}
		}
	}

	// 3. Update status to in-progress.
	if rm.opts.ManageStatus {
		rm.updateStartStatus(ctx, sess)
	}

	// 4. Preconditions.
	for _, pre := range rm.opts.Preconditions {
		if err := pre(ctx, sess); err != nil {
			if rm.opts.ManageStatus {
				rm.writeStatus(ctx, sess, dm, status.Options{
					UpdatingReason:  status.UpdatingPreconditionWait,
					UpdatingMessage: err.Error(),
				})
			}
			return ReconcileResult{Requeue: true}
		}
	}

	// 5. Setup / finalize components.
	if isFinalizer {
		if err := ctrl.FinalizeComponents(ctx, sess); err != nil {
			return rm.handleError(ctx, sess, dm, err)
		}
	} else {
		if err := ctrl.SetupComponents(ctx, sess); err != nil {
			return rm.handleError(ctx, sess, dm, err)
		}
	}

	// 6. Rollout.
	completionState := rolloutmanager.New(sess, ctrl, rm.opts.Concurrency).Rollout(ctx)

	// 7. Update completion status.
	if rm.opts.ManageStatus {
		rm.updateCompletionStatus(ctx, sess, dm, completionState)
	}

	// 8. Determine requeue.
	// Fatal rollout failures always requeue regardless of ShouldRequeue.
	if completionState.AnyFailed() && completionState.Err != nil {
		return ReconcileResult{Requeue: true, Err: completionState.Err}
	}

	requeue := ctrl.ShouldRequeue(ctx, sess)
	if !requeue && isFinalizer && ctrl.HasFinalizer() {
		if err := removeFinalizer(ctx, sess, ctrl.Finalizer()); err != nil {
			return ReconcileResult{Requeue: true, Err: err}
		}
	}

	return ReconcileResult{Requeue: requeue}
}

// ── Status helpers ────────────────────────────────────────────────────────────

func (rm *ReconcileManager) updateStartStatus(ctx context.Context, sess *session.Session) {
	readyReason := status.ReadyInitializing
	if current := status.GetCondition(status.ConditionReady, sess.Status); current != nil {
		if r, _ := current["reason"].(string); r != "" {
			readyReason = status.ReadyReason(r)
		}
	}
	rm.writeStatus(ctx, sess, sess.DeployManager, status.Options{
		ReadyReason:    readyReason,
		ReadyMessage:   "Reconcile started",
		UpdatingReason: status.UpdatingVerifyWait,
	})
}

func (rm *ReconcileManager) updateCompletionStatus(
	ctx context.Context,
	sess *session.Session,
	dm deploymanager.DeployManager,
	cs *dag.CompletionState,
) {
	opts := status.Options{ComponentState: cs}
	if cs.VerifyCompleted() {
		opts.ReadyReason = status.ReadyStable
		opts.ReadyMessage = "Verify complete"
		opts.UpdatingReason = status.UpdatingStable
		opts.UpdatingMessage = "Rollout complete"
		opts.Version = sess.Version()
	} else {
		opts.UpdatingReason = status.UpdatingVerifyWait
		opts.UpdatingMessage = "Component verification incomplete"
		current := status.GetCondition(status.ConditionReady, sess.Status)
		if r, _ := current["reason"].(string); r != string(status.ReadyInitializing) {
			opts.ReadyReason = status.ReadyInProgress
			opts.ReadyMessage = "Verify in progress"
		}
	}
	rm.writeStatus(ctx, sess, dm, opts)
}

func (rm *ReconcileManager) handleError(
	ctx context.Context,
	sess *session.Session,
	dm deploymanager.DeployManager,
	err error,
) ReconcileResult {
	if rm.opts.ManageStatus {
		rm.writeStatus(ctx, sess, dm, status.Options{
			ReadyReason:     status.ReadyErrored,
			ReadyMessage:    err.Error(),
			UpdatingReason:  status.UpdatingErrored,
			UpdatingMessage: err.Error(),
		})
	}
	return ReconcileResult{Requeue: true, Err: err}
}

func (rm *ReconcileManager) writeStatus(
	ctx context.Context,
	sess *session.Session,
	dm deploymanager.DeployManager,
	opts status.Options,
) {
	obj, err := dm.Get(ctx, sess.APIVersion(), sess.Kind(), sess.Name(), sess.Namespace())
	if err != nil || obj == nil {
		return
	}
	currentStatus, _ := obj["status"].(map[string]any)
	if currentStatus == nil {
		currentStatus = make(map[string]any)
	}
	newStatus := status.UpdateApplicationStatus(currentStatus, opts)
	if !status.StatusChanged(currentStatus, newStatus) {
		return
	}
	_, _ = dm.SetStatus(ctx, sess.APIVersion(), sess.Kind(), sess.Name(), sess.Namespace(), newStatus)
}

// ── Finalizer helpers ─────────────────────────────────────────────────────────

func addFinalizer(ctx context.Context, sess *session.Session, finalizer string) error {
	obj, err := sess.DeployManager.Get(ctx, sess.APIVersion(), sess.Kind(), sess.Name(), sess.Namespace())
	if err != nil || obj == nil {
		return nil
	}
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		return nil
	}
	for _, f := range toStringSlice(meta["finalizers"]) {
		if f == finalizer {
			return nil
		}
	}
	meta["finalizers"] = append(toAnySlice(meta["finalizers"]), finalizer)
	_, err = sess.DeployManager.Deploy(ctx, []map[string]any{obj}, deploymanager.DeployMethodUpdate, false)
	return err
}

func removeFinalizer(ctx context.Context, sess *session.Session, finalizer string) error {
	obj, err := sess.DeployManager.Get(ctx, sess.APIVersion(), sess.Kind(), sess.Name(), sess.Namespace())
	if err != nil || obj == nil {
		return nil
	}
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		return nil
	}
	existing := toAnySlice(meta["finalizers"])
	updated := existing[:0]
	for _, f := range existing {
		if f != finalizer {
			updated = append(updated, f)
		}
	}
	meta["finalizers"] = updated
	_, err = sess.DeployManager.Deploy(ctx, []map[string]any{obj}, deploymanager.DeployMethodUpdate, false)
	return err
}

func toAnySlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func toStringSlice(v any) []string {
	raw, _ := v.([]any)
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ── ID generation ─────────────────────────────────────────────────────────────

// generateID returns a random 22-char hex string unique per reconcile.
func generateID() string {
	b := make([]byte, 11)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
