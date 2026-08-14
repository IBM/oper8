# PR-6: Go rewrite — WatchManager, full operator lifecycle, and comprehensive test suite

## Summary

This PR completes the Go rewrite of oper8's core operator framework, replacing ~3,000 lines of Python with idiomatic, race-safe Go that delegates watch management, leader election, work-queuing, and health probes entirely to [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime). All 18 packages now compile; 306 tests pass under `go test -race ./...`.

---

## What was ported from Python

| Go package | Python source | Notes |
|---|---|---|
| `constants` | `constants.py` | All annotation keys, `PassthroughAnnotations`, misc constants |
| `errors` | `exceptions.py` | `ConfigError`, `ClusterError`, `RolloutError` (fatal); `PreconditionError`, `VerificationError` (transient); `IsFatal()` + assert helpers |
| `utils` | `utils.py` | `MergeConfigs`, `GetNested`, `SetNested`, `GetPassthroughAnnotations` |
| `status` | `status.py` | `MakeApplicationStatus`, `UpdateApplicationStatus`, `StatusChanged`, condition/version helpers |
| `dag` | `dag/node.py` + `dag/graph.py` | `Node`, `Graph`, `Runner` (concurrent + serial), `HaltError`, `CompletionState` |
| `session` | `session.py` | Per-reconcile context, CR field accessors, component DAG helpers, `ScopedName`/`TruncateName` |
| `component` | `component.py` | `Component` interface (Name, Disabled, Setup, Deploy, Verify) |
| `controller` | `controller.py` | `Controller` interface, `BaseController` (all hook no-ops), `GVK`, `HookResult` |
| `deploymanager` | `deploy_manager/base.py` | `DeployManager` interface, `DryRunDeployManager` (in-memory, thread-safe, watch events), `OwnerRef`/`ApplyOwnerRef` |
| `deploymanager/k8s` | — | Production `k8s.Client`: SSA default, Update, Replace, Delete, Get, List, SetStatus |
| `rolloutmanager` | `rollout_manager.py` | 4-phase rollout (deploy graph → after-deploy hooks → verify graph → after-verify hooks) |
| `reconcilemanager` | `reconcile.py` | Full reconcile lifecycle: ID gen, session init, finalizer mgmt, preconditions, setup/finalize, rollout, status update, requeue |
| `verify` | `verify_resources.py` | `VerifyResource`, `VerifyPod`, `VerifyJob`, `VerifyDeployment`, `VerifyStatefulSet`, `VerifySubsystem`, kind registry |
| `patch` | `patch.py` + `patch_strategic_merge.py` | `Apply()` with SMP (typed schema for Deployment/StatefulSet/etc.) + JSON-6902 (RFC 6902), component-name routing |
| `temporarypatch` | `temporary_patch/` | `Component` (add/remove patch annotation on target CR), `Controller` (finalizer, patchable-kinds allowlist) |
| `watchmanager` | `watch_manager.py` | **See §WatchManager Improvements below** |
| `cmd/run.go` | `cmd/run_operator_cmd.py` | `RunOperator(Options)`: wires manager, predicates, health/readyz probes, leader election, signal handling |

---

## WatchManager: from ~900 Python lines to ~200 Go lines

The Python `WatchManager` was the largest and most complex module — it implemented its own watch streams, work queue, rate limiting, leader election, subprocess-per-reconcile isolation, requeue backoff, and health probes, all from scratch.

In Go, **controller-runtime provides all of that natively**. The `watchmanager` package is now a thin adapter of ~200 lines:

| Concern | Python WatchManager | Go WatchManager |
|---|---|---|
| Watch streams | Custom (k8s-client-python) | controller-runtime informer cache |
| Work queue + rate limiting | `threading.Queue` + custom | controller-runtime workqueue |
| Leader election | Custom Lease implementation | `ctrl.Options{LeaderElection: true}` |
| Requeue / backoff | Custom sleep-poll loop | controller-runtime exponential backoff |
| Subprocess isolation | Fork per reconcile (GIL workaround) | Not needed — goroutines are race-safe |
| Health/readyz probes | Separate HTTP server | `mgr.AddHealthzCheck` / `AddReadyzCheck` |
| Pause filter | Checked inside reconcile (wastes queue slot) | `NotPaused` predicate — never enqueued |
| Generation filter | Partial (missed deletion edge case) | `GenerationChangedOrDeleted` predicate — gen change **or** `DeletionTimestamp` |
| Lines of code | ~900 | ~200 |

### New capabilities not in the Python version

- **Race-safe concurrent DAG runner** — goroutines + buffered results channel + `sync.Mutex`, verified clean under `go test -race`
- **Predicate-level filtering** — `NotPaused` and `GenerationChangedOrDeleted` prevent spurious objects from ever reaching the reconcile queue
- **Server-side apply (SSA) as the default deploy method** — field ownership tracking and conflict detection out of the box; test fallback for the controller-runtime fake client
- **Structured `GVK` routing** — `schema.GroupVersionKind` structs everywhere; `GVKFromString` validates format at startup
- **`DryRunDeployManager`** — fully thread-safe in-memory cluster simulator with watch event emission; used in all unit tests; no cluster needed

---

## Bugs fixed during this PR

1. **`errors.IsFatal()` always returned `false` for fatal errors** — type-asserted `err.(*Oper8Error)` but concrete types are `*ConfigError` / `*ClusterError` etc. (embedded structs). Fixed by asserting against a `fatalChecker` interface.

2. **`patch.resolvePatchPayload` routing was inverted** — patch entries without a matching component-name key were silently skipped instead of returning `nil`. Tests corrected to nest patch payloads under the `internalName` routing key, matching Python semantics.

3. **`TestApply_UnsupportedPatchType` never exercised the switch** — the test passed an empty map payload; the resolver found no key and returned `nil`, bypassing the type-switch entirely. Test fixed to use a resolvable payload.

---

## Test coverage

306 tests across 16 packages (all passing, `go test -race ./...`):

| Package | Tests | Highlights |
|---|---|---|
| `watchmanager` | 40 | Adapter (happy path, NotFound, finalizer, requeue, setup error, paused object, cross-namespace), GenerationChangedOrDeleted (8 cases), NotPaused (6 cases), GVKFromString (6 cases) |
| `reconcilemanager` | 19 | Empty graph, verified, setup error, deploy error, verify-not-ready, precondition blocking, ordered components, invalid CR, finalizer path, ShouldRequeue override, ManageStatus, multiple preconditions |
| `rolloutmanager` | 18 | Empty graph, happy path, setup/deploy errors, downstream blocking, independent branches, concurrent, hooks (all 4 variants), disabled component |
| `deploymanager/k8s` | 17 | Get, Deploy (SSA/Update/Replace), Delete, List, Watch, edge cases |
| `deploymanager` | 16 | DryRun: CRUD, watch events, label selector, deep-copy isolation, owner refs |
| `dag` | 25+ | Serial + concurrent runner, fatal/unverified halts, disabled nodes, execution order, edge funcs, context cancellation, race detector (20 goroutines), `HaltError.Unwrap`, `GetNode`, `Nodes()` root exclusion |
| `status` | 20 | MakeApplicationStatus (all reason combos), UpdateApplicationStatus (preserve/override), StatusChanged (timestamp-ignore), GetVersion, ComponentStatus sorting, IBM CloudPak kind field |
| `verify` | 28+ | Pod/Job/Deployment/StatefulSet/Subsystem verifiers, kind registry, latest-condition sort, custom condition type, custom timestamp key, per-call VerifyFunc overrides registry |
| `session` | 19 | Construction, validation (5 error paths), accessors, current version, component DAG helpers, ScopedName/TruncateName (determinism, collision-resistance, limit boundary) |
| `patch` | 18 | SMP + JSON-6902, routing (match/no-match/dotted/deeply-nested), multiple sequential patches, immutability, remove op |
| `errors` | 18 | IsFatal for all 5 types via error interface, nil, non-Oper8 error, formatted messages, assert helpers |
| `controller` | 13 | All BaseController defaults, all hook methods, GVK, HookResult |
| `utils` | 10 | MergeConfigs deep/shallow, GetNested/SetNested, GetPassthroughAnnotations |
| `constants` | 15 | Key format, distinctness, PassthroughAnnotations membership/prefix/deduplication, value assertions |
| `component` | 14 | Full interface contract, call counting, error propagation, full lifecycle |
| `temporarypatch` | 8 | Component (add/remove annotation), Controller (GVK/finalizer/missing-spec/unpatchable-kind) |

---

## What still needs to be done (follow-on PRs)

- **`make generate`** — run `controller-gen` to replace the hand-written `zz_generated.deepcopy.go` stub
- **`make manifests`** — populate `config/crd/bases/` with real CRD YAML (needed by OLM and integration tests)
- **`cmd/run.go` tests** — smoke test for `RunOperator()` option validation and manager wiring
- **`go.mod` stabilisation** — an external process keeps bumping to alpha k8s deps; pin to `v0.31.0` stable once resolved
- **Namespace-scoped watch** — pass `Namespaces []string` through `watchmanager.Options` into `ctrl.Options{Cache: ...}` for namespace-restricted operators

---

## How to test

```bash
cd oper8/rewrite
go mod tidy
go test -race ./...   # 306 tests, all green
```
