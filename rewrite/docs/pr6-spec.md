# PR-6 Spec: Watch Manager, CRD Codegen, OLM Bundle

**Branch:** `rewrite/Watch_Manager`  
**Depends on:** PR-5 (`rewrite/Reconcile_Manager`) merged  
**Module:** `github.com/example/oper8-go` (`go 1.23`)

---

## Overview

PR-6 delivers three distinct sub-features that together make oper8-go a complete, production-deployable operator framework:

| Sub-feature            | Package                      | What it adds                                                                         |
| ---------------------- | ---------------------------- | ------------------------------------------------------------------------------------ |
| **6a — Watch Manager** | `watchmanager/`              | controller-runtime adapter; wires ReconcileManager into a real Kubernetes watch loop |
| **6b — CRD Codegen**   | `api/v1alpha1/` + `Makefile` | Generate CRD YAML from Go struct annotations via `controller-gen`                    |
| **6c — OLM Bundle**    | `bundle/` + `Makefile`       | Generate OperatorHub-ready bundle via `operator-sdk generate bundle`                 |

---

## 6a — Watch Manager

### Background: what the Python version does

The Python `PythonWatchManager` runs a multi-process architecture:

```
K8s watch stream → WatchThread (per GVK/namespace)
  → filter evaluation (GenerationFilter, PauseFilter, etc.)
  → ReconcileRequest pushed to queue
  → ReconcileThread spawns a subprocess per reconcile
  → ReconcileProcessEntrypoint runs ReconcileManager.safe_reconcile()
  → result + dependent watch requests returned via pipe
  → timer thread schedules requeue / periodic events
```

Key complexity points: subprocess isolation, multiprocessing IPC pipes, per-resource annotation-based leader election, 10+ filter types, and dynamic dependent-resource watch requests during reconcile.

### Go design: use controller-runtime, not a port

**Do not port the Python WatchManager directly.** The Python implementation reimplements what `controller-runtime` already provides in Go — watch streams, work queues, leader election, requeue, and periodic reconciliation. Porting it would reproduce a large, complex subsystem for zero gain.

Instead, provide a **thin adapter** in `watchmanager/` that bridges oper8-go's `ReconcileManager` into controller-runtime's `Reconciler` interface. All watch/queue/leader-election machinery comes from controller-runtime for free.

### Architecture

```
controller-runtime Manager
  └─ controller-runtime Controller (watches CR GVK)
       └─ Reconciler.Reconcile(ctx, req)  ← our adapter
            ├─ fetch current CR from API server
            ├─ determine isFinalizer (DeletionTimestamp set + our finalizer present)
            └─ reconcilemanager.ReconcileManager.Reconcile(ctx, ctrl, cr, dm, isFinalizer)
                 └─ returns ReconcileResult{Requeue, RequeueAfter, Err}
                      └─ mapped to ctrl.Result / ctrl.Result{RequeueAfter} / error
```

### New package: `watchmanager/`

#### `watchmanager.go` — exports

```go
// Adapter bridges a controller.Controller into controller-runtime.
type Adapter struct {
    ctrl   controller.Controller
    rm     *reconcilemanager.ReconcileManager
    client client.Client  // controller-runtime client
    scheme *runtime.Scheme
}

// New creates an Adapter. opts are passed to ReconcileManager.
func New(
    ctrl controller.Controller,
    client client.Client,
    scheme *runtime.Scheme,
    opts reconcilemanager.Options,
) *Adapter

// Reconcile implements sigs.k8s.io/controller-runtime/pkg/reconcile.Reconciler.
// Called by controller-runtime on every watch event.
func (a *Adapter) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error)

// SetupWithManager registers the adapter with a controller-runtime Manager,
// sets up the watch on the CR GVK, and wires the RBAC markers.
func (a *Adapter) SetupWithManager(mgr ctrl.Manager) error
```

#### `Reconcile` implementation detail

```go
func (a *Adapter) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
    // 1. Fetch the current CR from the API server.
    var obj unstructured.Unstructured
    obj.SetGroupVersionKind(schema.GroupVersionKind{...}) // from a.ctrl.GVK()
    if err := a.client.Get(ctx, req.NamespacedName, &obj); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    // 2. Build a live DeployManager backed by the controller-runtime client.
    dm := k8sdeploymanager.New(a.client, a.scheme)

    // 3. Determine if this is a finalizer pass.
    isFinalizer := !obj.GetDeletionTimestamp().IsZero() &&
        controllerutil.ContainsFinalizer(&obj, a.ctrl.Finalizer())

    // 4. Run oper8 reconcile loop.
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
```

#### New package: `deploymanager/k8s/` — live cluster DeployManager

The existing `DryRunDeployManager` works against an in-memory store. For production we need one that talks to the real API server via the controller-runtime `client.Client`.

```go
// Package k8s provides a DeployManager backed by a controller-runtime client.
package k8s

type Client struct {
    client client.Client
    scheme *runtime.Scheme
}

func New(c client.Client, s *runtime.Scheme) deploymanager.DeployManager

// Implements: Get, Deploy (create-or-patch), SetStatus, Delete
```

All four methods map directly onto `client.Get`, `client.Patch` (server-side apply), `client.Status().Update()`, and `client.Delete`.

### Event filtering

Python oper8 has 10+ filter types (`GenerationFilter`, `PauseFilter`, etc.). In controller-runtime, this is handled by **predicates** on the `Watches` call. We implement the two that matter:

```go
// GenerationChangedOrDeleted passes events where metadata.generation changed,
// or where the object is being deleted. Equivalent to Python's GenerationFilter
// + CreationDeletionFilter combined.
func GenerationChangedOrDeleted() predicate.Predicate

// NotPaused filters out events where oper8.org/pause-reconciliation is set.
// Equivalent to Python's PauseFilter — avoids even enqueuing paused CRs.
func NotPaused() predicate.Predicate
```

These are passed to `SetupWithManager` and require zero controller-runtime boilerplate from the operator author.

### Dependent resource watches

Python's `ReconcileProcessDeployManager` dynamically registered new watches during a reconcile when it detected a dependent resource being applied. In Go, this is handled by `ctrl.Watches()` at startup with an `EnqueueRequestForOwner` handler — no runtime watch registration needed.

Operator authors that need dynamic dependent watches call `SetupWithManager` with additional `Watches` options. This is idiomatic controller-runtime and requires no oper8-specific machinery.

### Leader election

Python had three leader election backends (Lease, leader-for-life, annotation). controller-runtime provides Kubernetes Lease-based leader election out of the box via `ctrl.NewManager(..., ctrl.Options{LeaderElection: true})`. No oper8-specific leader election code needed.

### Packages to add to `go.mod`

```
sigs.k8s.io/controller-runtime v0.19.x
k8s.io/apimachinery v0.31.x
k8s.io/client-go v0.31.x
k8s.io/api v0.31.x
```

These are the standard controller-runtime dependencies. They are **runtime dependencies** — they must appear in `go.mod require`.

### Tests

| Test                                 | What it covers                                                      |
| ------------------------------------ | ------------------------------------------------------------------- |
| `TestAdapter_Reconcile_HappyPath`    | CR fetched, ReconcileManager called, Result mapped correctly        |
| `TestAdapter_Reconcile_NotFound`     | 404 on Get returns no-error (object deleted)                        |
| `TestAdapter_Reconcile_IsFinalizer`  | DeletionTimestamp set → isFinalizer=true passed to ReconcileManager |
| `TestAdapter_Reconcile_Paused`       | PauseAnnotation set → filter blocks enqueue, Reconcile never called |
| `TestAdapter_Reconcile_RequeueAfter` | ReconcileResult.RequeueAfter mapped to ctrl.Result.RequeueAfter     |
| `TestAdapter_Reconcile_Error`        | ReconcileResult.Err → returned as error to controller-runtime       |
| `TestK8sDeployManager_Get`           | Live DM Get maps to client.Get                                      |
| `TestK8sDeployManager_Deploy_Create` | Object absent → client.Create                                       |
| `TestK8sDeployManager_Deploy_Patch`  | Object present → server-side apply patch                            |
| `TestK8sDeployManager_SetStatus`     | client.Status().Update() called                                     |

Use `sigs.k8s.io/controller-runtime/pkg/envtest` (or `fake.NewClientBuilder()`) for all tests — no live cluster required.

---

## 6b — CRD Codegen

### Goal

Operator authors define their CR as a Go struct with `+kubebuilder:` marker comments. Running `make manifests` generates the CRD YAML automatically. No hand-written YAML.

### What to add

#### `api/v1alpha1/` — sample CR type

```go
// rewrite/api/v1alpha1/foocr_types.go

// +groupName=example.com

// FooCRSpec defines the desired state.
type FooCRSpec struct {
    // +kubebuilder:validation:MinLength=1
    Version string `json:"version"`

    // +kubebuilder:validation:Minimum=1
    // +kubebuilder:validation:Maximum=10
    Replicas int32 `json:"replicas,omitempty"`
}

// FooCRStatus defines the observed state.
type FooCRStatus struct {
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=foo
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
type FooCR struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   FooCRSpec   `json:"spec,omitempty"`
    Status FooCRStatus `json:"status,omitempty"`
}
```

#### `api/v1alpha1/groupversion_info.go`

```go
// +kubebuilder:object:generate=true
package v1alpha1

var (
    GroupVersion  = schema.GroupVersion{Group: "example.com", Version: "v1alpha1"}
    SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
    AddToScheme   = SchemeBuilder.AddToScheme
)
```

#### `api/v1alpha1/zz_generated.deepcopy.go`

Generated by `controller-gen object`. Checked into the repo. Never hand-edited.

#### `config/crd/` — generated CRD YAML

Generated by `controller-gen crd`. Checked in. Updated by running `make manifests`.

```
rewrite/config/crd/bases/example.com_foocrs.yaml
```

#### `Makefile` targets

```makefile
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.0

.PHONY: generate
generate: ## Generate DeepCopy methods (zz_generated.deepcopy.go)
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: manifests
manifests: ## Generate CRD YAML into config/crd/
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases

.PHONY: manifests-verify
manifests-verify: manifests ## Verify CRDs are up to date (used in CI)
	git diff --exit-code config/crd/
```

`controller-gen` is invoked via `go run` — no binary installation required, no vendoring. The version is pinned in the `go run` invocation.

#### CI addition (`.github/workflows/pr6.yml`)

Add a `manifests-verify` step that runs `make manifests-verify` and fails if the generated YAML differs from what is checked in. This prevents drift between the Go types and the CRD YAML.

### `go.mod` addition (build-time only)

```
// tools.go — blank import to pin controller-gen version in go.sum
//go:build tools
package tools
import _ "sigs.k8s.io/controller-tools/cmd/controller-gen"
```

`controller-tools` is a **build/codegen tool**, not a runtime dependency. It lives in `go.sum` via the `tools.go` convention and is never imported by production code.

---

## 6c — OLM Bundle

### Goal

Running `make bundle` produces an OLM-compatible operator bundle under `rewrite/bundle/`. The bundle can be pushed to a registry and submitted to OperatorHub. This makes oper8-go the first Go operator framework at IBM with built-in OLM publishing support.

### Bundle structure (output of `make bundle`)

```
rewrite/bundle/
├── manifests/
│   ├── example.com_foocrs.yaml              ← CRD (copied from config/crd/)
│   ├── foocr-operator.clusterserviceversion.yaml  ← CSV (generated)
│   └── foocr-operator_rbac.yaml             ← RBAC (generated from markers)
├── metadata/
│   └── annotations.yaml                     ← OLM channel/media-type metadata
└── Dockerfile                               ← bundle image Dockerfile
```

### `Makefile` targets

```makefile
VERSION        ?= 0.0.1
IMG            ?= controller:latest
BUNDLE_IMG     ?= bundle:$(VERSION)
OPERATOR_SDK   ?= operator-sdk

.PHONY: bundle
bundle: manifests ## Generate OLM bundle in bundle/
	$(OPERATOR_SDK) generate kustomize manifests -q
	cd config/manager && kustomize edit set image controller=$(IMG)
	kustomize build config/manifests | \
		$(OPERATOR_SDK) generate bundle \
			--overwrite \
			--version $(VERSION) \
			--channels alpha \
			--default-channel alpha
	$(OPERATOR_SDK) bundle validate ./bundle

.PHONY: bundle-build
bundle-build: ## Build the bundle image
	docker build -f bundle/Dockerfile -t $(BUNDLE_IMG) .

.PHONY: bundle-push
bundle-push: ## Push the bundle image
	docker push $(BUNDLE_IMG)
```

### RBAC markers (in `watchmanager/`)

controller-gen reads `+kubebuilder:rbac:` markers on the Reconciler to generate the RBAC manifests included in the bundle:

```go
// +kubebuilder:rbac:groups=example.com,resources=foocrs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=example.com,resources=foocrs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=example.com,resources=foocrs/finalizers,verbs=update
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
func (a *Adapter) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error)
```

### CSV fields to populate manually (post-generation)

The generated CSV has placeholder fields that must be filled in before OperatorHub submission:

| Field                 | Value                                       |
| --------------------- | ------------------------------------------- |
| `metadata.name`       | `oper8-go-operator.v0.0.1`                  |
| `spec.displayName`    | `Oper8 Go Operator`                         |
| `spec.description`    | From `README.md` description section        |
| `spec.icon`           | IBM logo SVG (base64)                       |
| `spec.provider.name`  | `IBM`                                       |
| `spec.links`          | GitHub repo URL                             |
| `spec.maintainers`    | Team contact                                |
| `spec.maturity`       | `alpha`                                     |
| `spec.keywords`       | `["operator", "ibm", "reconcile", "oper8"]` |
| `spec.minKubeVersion` | `1.28.0`                                    |

### Bundle format

Use **bundle format v1** (directory with `metadata/` subdirectory), not the deprecated package manifest format. This is the format required by OperatorHub and OLM ≥ 0.17.

---

## File layout after PR-6

```
rewrite/
├── api/
│   └── v1alpha1/
│       ├── foocr_types.go
│       ├── groupversion_info.go
│       └── zz_generated.deepcopy.go        ← generated, checked in
├── config/
│   ├── crd/
│   │   └── bases/
│   │       └── example.com_foocrs.yaml     ← generated, checked in
│   ├── manifests/                          ← kustomize bases for bundle gen
│   └── manager/
│       └── kustomization.yaml
├── bundle/                                 ← generated by make bundle
│   ├── manifests/
│   ├── metadata/
│   └── Dockerfile
├── deploymanager/
│   ├── deploymanager.go                    ← unchanged
│   ├── dryrun.go                           ← unchanged
│   ├── ownerref.go                         ← unchanged
│   └── k8s/
│       └── client.go                       ← NEW: live cluster DeployManager
├── watchmanager/
│   └── adapter.go                          ← NEW: controller-runtime Reconciler adapter
├── hack/
│   ├── boilerplate.go.txt                  ← copyright header for generated files
│   └── tools.go                            ← pins controller-gen + operator-sdk in go.sum
├── Makefile                                ← NEW
├── go.mod                                  ← add controller-runtime + k8s deps
└── .github/workflows/
    └── pr6.yml                             ← NEW: generate + build + bundle-validate CI
```

---

## Dependency additions to `go.mod`

| Dependency                       | Kind                  | Version   |
| -------------------------------- | --------------------- | --------- |
| `sigs.k8s.io/controller-runtime` | runtime               | `v0.19.x` |
| `k8s.io/apimachinery`            | runtime               | `v0.31.x` |
| `k8s.io/client-go`               | runtime               | `v0.31.x` |
| `k8s.io/api`                     | runtime               | `v0.31.x` |
| `sigs.k8s.io/controller-tools`   | build tool (tools.go) | `v0.17.x` |

---

## What is explicitly NOT in PR-6

| Item                                        | Reason                                                                                             |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Python filter system (10+ types)            | controller-runtime predicates handle this; porting adds no value                                   |
| Subprocess isolation per reconcile          | controller-runtime runs reconciles as goroutines; subprocesses are a Python workaround for the GIL |
| Annotation-based leader election            | controller-runtime Lease-based leader election is the standard; annotation style is deprecated     |
| Dynamic watch registration during reconcile | `ctrl.Watches()` with `EnqueueRequestForOwner` covers this declaratively at startup                |
| DryRunWatchManager                          | Not needed; `fake.NewClientBuilder()` from controller-runtime replaces it for tests                |
| TimerThread / HeartbeatThread               | controller-runtime's requeue mechanism (`ctrl.Result{RequeueAfter: d}`) replaces both              |

---

## Implementation order

1. **Add `go.mod` deps** — `go get sigs.k8s.io/controller-runtime@v0.19.x` etc. Verify `go build ./...` still passes.
2. **`deploymanager/k8s/client.go`** — live DeployManager; write tests with `fake.NewClientBuilder()`.
3. **`watchmanager/adapter.go`** — Reconciler adapter + predicates; write tests.
4. **`api/v1alpha1/`** — FooCR types; run `make generate` to produce deepcopy; run `make manifests` to produce CRD YAML; commit both generated files.
5. **`Makefile`** — `generate`, `manifests`, `manifests-verify`, `bundle`, `bundle-build`, `bundle-push`.
6. **`config/`** — kustomize bases for bundle generation.
7. **`make bundle`** — generate initial bundle; populate CSV fields; commit.
8. **CI** — add `pr6.yml`; `manifests-verify` + `go test -race ./...` + `bundle validate`.

---

## Open questions (resolve before implementation starts)

1. **Module path**: `go.mod` currently declares `github.com/example/oper8-go`. Should this be updated to the real IBM GitHub org path (e.g. `github.com/IBM/oper8-go`) before adding external deps, or stay as-is for the rewrite PRs?

2. **OLM channel**: Use `alpha` for the initial bundle, or set up `stable` and `alpha` channels from the start?

3. **CRD scope**: `FooCR` is defined as `Namespaced`. Does the framework need to demonstrate a `Cluster`-scoped CR as well for OperatorHub completeness?

4. **operator-sdk version**: Pin `v1.38.x` (latest stable) or use whatever is available in CI? Needs to be consistent with OLM bundle format version.
