// Package watchmanager bridges oper8-go's ReconcileManager into
// controller-runtime's reconcile loop.
//
// Design (see rewrite/docs/pr6-spec.md §6a):
//
// The Python WatchManager reimplemented watch streams, work queues, leader
// election, requeue, and per-process isolation. In Go, controller-runtime
// provides all of that for free. This package supplies only the thin adapter
// layer that controller-runtime needs: a Reconciler implementation and two
// predicates (GenerationChangedOrDeleted, NotPaused).
//
// Operator authors:
//  1. Construct an Adapter with their controller.Controller and Options.
//  2. Call adapter.SetupWithManager(mgr) to register the watch.
//  3. Start mgr.Start(ctx) — controller-runtime takes over.
package watchmanager

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/example/oper8-go/controller"
	k8sdm "github.com/example/oper8-go/deploymanager/k8s"
	"github.com/example/oper8-go/reconcilemanager"
)

// pauseAnnotation is the annotation that suppresses reconciliation.
// Mirrors reconcilemanager.PauseAnnotation.
const pauseAnnotation = "oper8.org/pause-reconciliation"

// Adapter bridges a controller.Controller into controller-runtime.
//
// +kubebuilder:rbac:groups=example.com,resources=foocrs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=example.com,resources=foocrs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=example.com,resources=foocrs/finalizers,verbs=update
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
type Adapter struct {
	ctrl   controller.Controller
	rm     *reconcilemanager.ReconcileManager
	client client.Client
	gvk    schema.GroupVersionKind
}

// New creates an Adapter. gvk is the GroupVersionKind of the CR to watch.
// opts are passed to the underlying ReconcileManager.
func New(
	ctrl controller.Controller,
	c client.Client,
	gvk schema.GroupVersionKind,
	opts reconcilemanager.Options,
) *Adapter {
	return &Adapter{
		ctrl:   ctrl,
		rm:     reconcilemanager.New(opts),
		client: c,
		gvk:    gvk,
	}
}

// Reconcile implements reconcile.Reconciler.
// Called by controller-runtime on every watch event that passes the predicates.
func (a *Adapter) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	// 1. Fetch the current CR from the API server.
	var obj unstructured.Unstructured
	obj.SetGroupVersionKind(a.gvk)
	if err := a.client.Get(ctx, req.NamespacedName, &obj); err != nil {
		// Object deleted before we reconciled — nothing to do.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Build a live DeployManager backed by the controller-runtime client.
	dm := k8sdm.New(a.client)

	// 3. Determine whether this is a finalizer pass:
	//    DeletionTimestamp is set AND our finalizer is still listed.
	isFinalizer := !obj.GetDeletionTimestamp().IsZero() &&
		controllerutil.ContainsFinalizer(&obj, a.ctrl.Finalizer())

	// 4. Run the oper8 reconcile loop.
	result := a.rm.Reconcile(ctx, a.ctrl, obj.Object, dm, isFinalizer)

	// 5. Map ReconcileResult → controller-runtime Result.
	if result.Err != nil {
		return ctrl.Result{}, result.Err
	}
	return ctrl.Result{
		Requeue:      result.Requeue,
		RequeueAfter: result.RequeueAfter,
	}, nil
}

// SetupWithManager registers the adapter with a controller-runtime Manager.
// It watches the CR GVK and applies the GenerationChangedOrDeleted and
// NotPaused predicates so that spurious re-reconciles are suppressed.
func (a *Adapter) SetupWithManager(mgr ctrl.Manager) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(a.gvk)

	return ctrl.NewControllerManagedBy(mgr).
		For(obj, builder.WithPredicates(
			predicate.And(
				GenerationChangedOrDeleted(),
				NotPaused(),
			),
		)).
		Complete(a)
}

// ── Predicates ────────────────────────────────────────────────────────────────

// GenerationChangedOrDeleted returns a predicate that passes Update events only
// when metadata.generation changed (i.e. the spec changed), plus all Create and
// Delete events. Generic events (periodic resync) are passed through.
//
// This is equivalent to Python oper8's GenerationFilter + CreationDeletionFilter.
func GenerationChangedOrDeleted() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return true },
		DeleteFunc: func(_ event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() ||
				!e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		GenericFunc: func(_ event.GenericEvent) bool { return true },
	}
}

// NotPaused returns a predicate that filters out any object that carries the
// oper8.org/pause-reconciliation annotation.
//
// This is equivalent to Python oper8's PauseFilter — the object is never
// even enqueued when it is paused, so the reconcile loop never runs.
func NotPaused() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(o client.Object) bool {
		annotations := o.GetAnnotations()
		if annotations == nil {
			return true
		}
		val, exists := annotations[pauseAnnotation]
		return !exists || val == "" || val == "false"
	})
}

// compile-time assertion: *Adapter satisfies reconcile.Reconciler.
var _ reconcile.Reconciler = (*Adapter)(nil)

// GVKFromString parses a "group/version/Kind" string into a GroupVersionKind.
// Convenience helper for operator main() functions.
//
// Accepted formats:
//   - "group/version/Kind"  e.g. "apps/v1/Deployment"
//   - "version/Kind"        e.g. "v1/ConfigMap"  (core group, empty group)
func GVKFromString(s string) (schema.GroupVersionKind, error) {
	parts := strings.SplitN(s, "/", 3)
	switch len(parts) {
	case 3:
		return schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: parts[2]}, nil
	case 2:
		return schema.GroupVersionKind{Group: "", Version: parts[0], Kind: parts[1]}, nil
	default:
		return schema.GroupVersionKind{}, fmt.Errorf("watchmanager: GVKFromString: expected group/version/Kind or version/Kind, got %q", s)
	}
}
