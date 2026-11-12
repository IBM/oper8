// Package temporarypatch provides a framework-level Component and Controller
// for the oper8 TemporaryPatch system.
//
// Ported from oper8 Python (temporary_patch/).
//
// The TemporaryPatch system allows operator users to annotate a target CR with
// a JSON map of named patches. At deploy time, TemporaryPatchComponent reads
// those patches and applies them (SMP or JSON-6902) to the targeted resource
// manifests before they reach the cluster.
//
// Usage:
//
//  1. Deploy a TemporaryPatch CR (group: oper8.org, kind: TemporaryPatch) that
//     references a target CR and carries the patch payload.
//  2. Register TemporaryPatchController (or a derived type) against the
//     TemporaryPatch GVK in your operator manager.
//  3. TemporaryPatchController's SetupComponents writes a patch annotation onto
//     the target CR; the target CR's own controller picks up the annotation on
//     the next reconcile and runs TemporaryPatchComponent.
package temporarypatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/example/oper8-go/constants"
	"github.com/example/oper8-go/controller"
	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	oper8err "github.com/example/oper8-go/errors"
	"github.com/example/oper8-go/session"
)

// ── TemporaryPatchComponent ───────────────────────────────────────────────────

// Component is responsible for adding or removing a named patch entry from
// the oper8.org/temporary-patches annotation on a target CR.
//
// It is typically registered by Controller.SetupComponents or
// Controller.FinalizeComponents. Mirrors Python's TemporaryPatchComponent.
type Component struct {
	name             string
	disabled         bool
	patchName        string
	targetAPIVersion string
	targetKind       string
	targetName       string
	targetNamespace  string
	targetState      map[string]any // fetched at construction time
}

// NewComponent creates a TemporaryPatchComponent. If disabled is true, the
// component removes the patch entry (finalizer path); when false it adds it.
//
// targetState is fetched immediately so that missing targets are caught at
// setup time rather than at deploy time.
func NewComponent(
	ctx context.Context,
	dm deploymanager.DeployManager,
	patchName string,
	targetAPIVersion, targetKind, targetName, targetNamespace string,
	disabled bool,
) (*Component, error) {
	state, err := dm.Get(ctx, targetAPIVersion, targetKind, targetName, targetNamespace)
	if err != nil {
		return nil, oper8err.NewClusterError(
			"temporarypatch: failed to fetch target %s/%s/%s: %v",
			targetNamespace, targetKind, targetName, err,
		)
	}
	if !disabled && state == nil {
		return nil, oper8err.NewPreconditionError(
			"temporarypatch: target resource %s/%s/%s not found",
			targetNamespace, targetKind, targetName,
		)
	}
	return &Component{
		name:             "temporarypatch-" + patchName,
		disabled:         disabled,
		patchName:        patchName,
		targetAPIVersion: targetAPIVersion,
		targetKind:       targetKind,
		targetName:       targetName,
		targetNamespace:  targetNamespace,
		targetState:      state,
	}, nil
}

func (c *Component) Name() string   { return c.name }
func (c *Component) Disabled() bool { return false }

func (c *Component) Setup(_ context.Context, _ *session.Session) error { return nil }

func (c *Component) Deploy(ctx context.Context, sess *session.Session) error {
	return c.managePatch(ctx, sess.DeployManager, c.disabled)
}

func (c *Component) Verify(_ context.Context, _ *session.Session) bool { return true }

// managePatch adds or removes the named patch entry from the target's annotation.
func (c *Component) managePatch(ctx context.Context, dm deploymanager.DeployManager, remove bool) error {
	if c.targetState == nil {
		return nil // target absent on finalizer path — nothing to do
	}

	// Read existing annotation.
	meta, _ := c.targetState["metadata"].(map[string]any)
	if meta == nil {
		meta = make(map[string]any)
		c.targetState["metadata"] = meta
	}
	annotations, _ := meta["annotations"].(map[string]any)
	if annotations == nil {
		annotations = make(map[string]any)
		meta["annotations"] = annotations
	}
	annoStr, _ := annotations[constants.TemporaryPatchesAnnotation].(string)
	if annoStr == "" {
		annoStr = "{}"
	}
	existing := make(map[string]any)
	if err := json.Unmarshal([]byte(annoStr), &existing); err != nil {
		return oper8err.NewConfigError("temporarypatch: annotation is not valid JSON: %v", err)
	}

	// Add or remove.
	updated := shallowCopyMap(existing)
	if remove {
		delete(updated, c.patchName)
	} else if _, exists := updated[c.patchName]; !exists {
		updated[c.patchName] = map[string]any{
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
			"api_version": c.targetAPIVersion,
			"kind":        c.targetKind,
		}
	}

	// Only write if something changed.
	if mapsEqual(existing, updated) {
		return nil
	}
	newAnno, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("temporarypatch: marshal annotation: %w", err)
	}
	annotations[constants.TemporaryPatchesAnnotation] = string(newAnno)

	_, err = dm.Deploy(ctx, []map[string]any{c.targetState}, deploymanager.DeployMethodDefault, false)
	if err != nil {
		return oper8err.NewClusterError("temporarypatch: re-apply target: %v", err)
	}
	return nil
}

// ── TemporaryPatchController ──────────────────────────────────────────────────

// Controller manages TemporaryPatch CRs. It reads the target info from the
// TemporaryPatch CR's spec and registers a TemporaryPatchComponent that stamps
// (or removes on finalize) the patch annotation on the target.
//
// Operator authors that need a custom GVK for the TemporaryPatch CR (required
// for OLM namespace-install isolation) should embed this type and override GVK:
//
//	type MyPatchController struct {
//	    temporarypatch.Controller
//	}
//	func (c *MyPatchController) GVK() controller.GVK {
//	    return controller.GVK{Group: "my.group", Version: "v1", Kind: "MyPatch"}
//	}
type Controller struct {
	controller.BaseController
	patchableKinds []string // "kind" or "apiVersion/kind"
}

// NewController creates a Controller that only patches the listed kinds.
// Entries can be plain "Kind" strings or "apiVersion/Kind" to scope to a
// specific API group.
func NewController(patchableKinds []string) *Controller {
	return &Controller{patchableKinds: patchableKinds}
}

// GVK returns the default TemporaryPatch GVK. Override in derived types.
func (c *Controller) GVK() controller.GVK {
	return controller.GVK{Group: "oper8.org", Version: "v1", Kind: "TemporaryPatch"}
}

// SetupComponents registers a TemporaryPatchComponent that adds the patch
// annotation to the target CR.
func (c *Controller) SetupComponents(ctx context.Context, sess *session.Session) error {
	return c.doSetup(ctx, sess, false)
}

// FinalizeComponents registers a TemporaryPatchComponent that removes the
// patch annotation from the target CR.
func (c *Controller) FinalizeComponents(ctx context.Context, sess *session.Session) error {
	return c.doSetup(ctx, sess, true)
}

// HasFinalizer — TemporaryPatchController always registers a finalizer so it
// can clean up the annotation when the patch CR is deleted.
func (c *Controller) HasFinalizer() bool { return true }

// Finalizer returns the finalizer string used by this controller.
func (c *Controller) Finalizer() string { return "oper8.org/temporarypatch-finalizer" }

func (c *Controller) doSetup(ctx context.Context, sess *session.Session, isFinalize bool) error {
	spec, _ := sess.CRManifest["spec"].(map[string]any)
	if spec == nil {
		return oper8err.NewConfigError("temporarypatch: CR missing spec")
	}
	targetAPIVersion, _ := spec["apiVersion"].(string)
	targetKind, _ := spec["kind"].(string)
	targetName, _ := spec["name"].(string)
	if targetAPIVersion == "" || targetKind == "" || targetName == "" {
		return oper8err.NewConfigError(
			"temporarypatch: spec must contain non-empty apiVersion, kind, and name",
		)
	}

	// Only patch if this kind (or apiVersion/kind) is in the allowed list.
	if !c.isPatchable(targetAPIVersion, targetKind) {
		return nil
	}

	comp, err := NewComponent(
		ctx, sess.DeployManager,
		sess.Name(),
		targetAPIVersion, targetKind, targetName, sess.Namespace(),
		isFinalize,
	)
	if err != nil {
		return err
	}

	node := dag.NewFuncNode(comp.Name(), nil)
	node.SetData(comp)
	return sess.AddComponent(node)
}

func (c *Controller) isPatchable(apiVersion, kind string) bool {
	if len(c.patchableKinds) == 0 {
		return true // no restriction — patch anything
	}
	avKind := apiVersion + "/" + kind
	for _, entry := range c.patchableKinds {
		if entry == kind || entry == avKind {
			return true
		}
	}
	return false
}

// ── helpers ───────────────────────────────────────────────────────────────────

func shallowCopyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func mapsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		ja, _ := json.Marshal(va)
		jb, _ := json.Marshal(vb)
		if string(ja) != string(jb) {
			return false
		}
	}
	return true
}
