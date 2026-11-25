// Package cmd provides the RunOperator entrypoint for oper8-go operators.
//
// Ported from oper8 Python (cmd/run_operator_cmd.py).
//
// Usage:
//
//	func main() {
//	    if err := cmd.RunOperator(cmd.Options{
//	        Controller: &MyController{},
//	        GVK: schema.GroupVersionKind{
//	            Group: "example.com", Version: "v1alpha1", Kind: "MyCR",
//	        },
//	    }); err != nil {
//	        fmt.Fprintln(os.Stderr, err)
//	        os.Exit(1)
//	    }
//	}
package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/reconcilemanager"
	"github.com/example/oper8-go/watchmanager"
)

// Options configures the operator entrypoint.
type Options struct {
	// Controller is the operator's Controller implementation. Required.
	Controller controller.Controller

	// GVK is the GroupVersionKind of the CR to watch. Required.
	GVK schema.GroupVersionKind

	// Scheme is the runtime scheme. When nil, a default scheme with core k8s
	// types is used. Add your CR types to this scheme before passing it.
	Scheme *runtime.Scheme

	// ReconcileOptions are forwarded to the ReconcileManager.
	ReconcileOptions reconcilemanager.Options

	// LeaderElection enables Kubernetes Lease-based leader election.
	// Recommended for production deployments. Default: false (dev mode).
	LeaderElection bool

	// LeaderElectionNamespace is the namespace for the leader election Lease.
	// Defaults to the value of the POD_NAMESPACE env variable, then "default".
	LeaderElectionNamespace string

	// LeaderElectionID is the name of the Lease resource. Defaults to
	// "oper8-go-leader-election".
	LeaderElectionID string

	// MetricsAddr is the address for the Prometheus metrics endpoint.
	// Set to "0" to disable. Default: ":8080".
	MetricsAddr string

	// HealthProbeAddr is the address for /healthz and /readyz endpoints.
	// Default: ":8081".
	HealthProbeAddr string

	// Namespaces restricts the watch to the given namespaces. Empty means
	// cluster-wide (all namespaces). Mirrors Python's namespace_list.
	Namespaces []string
}

// RunOperator starts the operator and blocks until a SIGTERM or SIGINT is
// received. It sets up the controller-runtime manager, registers the adapter,
// starts the health/metrics endpoints, acquires leader election (if enabled),
// and calls mgr.Start.
func RunOperator(opts Options) error {
	var (
		metricsAddr     = flag.String("metrics-addr", coalesce(opts.MetricsAddr, ":8080"), "Prometheus metrics address")
		healthProbeAddr = flag.String("health-probe-addr", coalesce(opts.HealthProbeAddr, ":8081"), "Health probe address")
		leaderElect     = flag.Bool("leader-elect", opts.LeaderElection, "Enable leader election")
	)
	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("cmd")

	// ── Scheme ───────────────────────────────────────────────────────────────
	scheme := opts.Scheme
	if scheme == nil {
		scheme = runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
	}

	// ── Leader election config ────────────────────────────────────────────────
	leaderNS := opts.LeaderElectionNamespace
	if leaderNS == "" {
		leaderNS = os.Getenv("POD_NAMESPACE")
	}
	if leaderNS == "" {
		leaderNS = "default"
	}
	leaderID := opts.LeaderElectionID
	if leaderID == "" {
		leaderID = "oper8-go-leader-election"
	}

	// ── Manager ──────────────────────────────────────────────────────────────
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: *metricsAddr,
		},
		HealthProbeBindAddress:  *healthProbeAddr,
		LeaderElection:          *leaderElect,
		LeaderElectionID:        leaderID,
		LeaderElectionNamespace: leaderNS,
	})
	if err != nil {
		return fmt.Errorf("cmd: failed to create manager: %w", err)
	}

	// ── Health checks ─────────────────────────────────────────────────────────
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("cmd: healthz check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("cmd: readyz check: %w", err)
	}

	// ── Adapter ──────────────────────────────────────────────────────────────
	adapter := watchmanager.New(
		opts.Controller,
		mgr.GetClient(),
		opts.GVK,
		opts.ReconcileOptions,
	)
	if err := adapter.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("cmd: SetupWithManager: %w", err)
	}

	// ── Start ─────────────────────────────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Info("starting operator",
		"gvk", opts.GVK.String(),
		"leaderElection", *leaderElect,
		"namespaces", opts.Namespaces,
	)

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("cmd: manager exited with error: %w", err)
	}
	return nil
}

// coalesce returns the first non-empty string.
func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
