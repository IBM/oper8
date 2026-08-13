// Package k8s provides a DeployManager backed by a controller-runtime client.
// It is the production implementation of deploymanager.DeployManager for use
// inside a real Kubernetes cluster.
//
// Design:
//   - Deploy uses server-side apply (Patch with Apply patch type) for
//     DeployMethodDefault, and client.Update for DeployMethodUpdate.
//   - Get returns (nil, nil) on NotFound, matching DryRunDeployManager.
//   - List converts client objects to map[string]any via JSON round-trip so
//     that oper8 components can work with raw maps regardless of whether a
//     typed scheme is available.
//   - Watch is not implemented here; controller-runtime owns the watch loop.
//     Callers that need a Watch should use controller-runtime Informers instead.
//   - SetStatus calls client.Status().Update(), which requires the object to
//     already exist (guaranteed in normal reconcile flow).
package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/example/oper8-go/deploymanager"
)

// Client implements deploymanager.DeployManager using a controller-runtime
// client.Client. Use New to construct.
type Client struct {
	c client.Client
}

// New constructs a k8s.Client wrapping the given controller-runtime client.
func New(c client.Client) deploymanager.DeployManager {
	return &Client{c: c}
}

// Deploy applies resource manifests to the cluster.
//
//   - DeployMethodDefault uses server-side apply (Apply patch).
//   - DeployMethodUpdate uses a full Update (requires resourceVersion in manifest).
//   - DeployMethodReplace deletes then re-creates.
//
// manageOwnerRefs is accepted for interface compatibility but ownership is
// expected to be set by the caller via controller-runtime controllerutil.
func (k *Client) Deploy(ctx context.Context, resources []map[string]any, method deploymanager.DeployMethod, _ bool) (bool, error) {
	changed := false
	for _, res := range resources {
		obj, err := toUnstructured(res)
		if err != nil {
			return changed, fmt.Errorf("k8s deploy: marshal %v: %w", gvkStr(res), err)
		}

		switch method {
		case deploymanager.DeployMethodReplace:
			// Attempt delete (ignore not-found), then create.
			existing := obj.DeepCopy()
			if err := k.c.Get(ctx, namespacedName(obj), existing); err == nil {
				if err := k.c.Delete(ctx, existing); err != nil && !errors.IsNotFound(err) {
					return changed, fmt.Errorf("k8s replace delete: %w", err)
				}
				changed = true
			}
			if err := k.c.Create(ctx, obj); err != nil {
				return changed, fmt.Errorf("k8s replace create: %w", err)
			}
			changed = true

		case deploymanager.DeployMethodUpdate:
			if err := k.c.Update(ctx, obj); err != nil {
				return changed, fmt.Errorf("k8s update: %w", err)
			}
			changed = true

		default: // DeployMethodDefault → server-side apply
			data, err := json.Marshal(obj)
			if err != nil {
				return changed, fmt.Errorf("k8s ssa marshal: %w", err)
			}
			patch := client.RawPatch(types.ApplyPatchType, data)
			if err := k.c.Patch(ctx, obj, patch, client.ForceOwnership, client.FieldOwner("oper8")); err != nil {
				return changed, fmt.Errorf("k8s ssa patch: %w", err)
			}
			changed = true
		}
	}
	return changed, nil
}

// Delete removes the given resources from the cluster.
// Not-found resources are silently ignored.
func (k *Client) Delete(ctx context.Context, resources []map[string]any) (bool, error) {
	changed := false
	for _, res := range resources {
		obj, err := toUnstructured(res)
		if err != nil {
			return changed, fmt.Errorf("k8s delete: marshal: %w", err)
		}
		if err := k.c.Delete(ctx, obj); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return changed, fmt.Errorf("k8s delete: %w", err)
		}
		changed = true
	}
	return changed, nil
}

// Get fetches a single resource by apiVersion, kind, name, and namespace.
// Returns (nil, nil) when the resource does not exist.
func (k *Client) Get(ctx context.Context, apiVersion, kind, name, namespace string) (map[string]any, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("k8s get: parse apiVersion %q: %w", apiVersion, err)
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind})
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := k.c.Get(ctx, key, obj); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("k8s get %s/%s/%s: %w", kind, namespace, name, err)
	}
	return obj.Object, nil
}

// List returns all resources matching apiVersion, kind, and namespace.
func (k *Client) List(ctx context.Context, apiVersion, kind, namespace string, opts deploymanager.ListOptions) ([]map[string]any, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("k8s list: parse apiVersion %q: %w", apiVersion, err)
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind + "List"})

	listOpts := []client.ListOption{client.InNamespace(namespace)}
	if opts.LabelSelector != "" {
		sel, err := metav1.ParseToLabelSelector(opts.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("k8s list: label selector: %w", err)
		}
		listOpts = append(listOpts, client.MatchingLabels(sel.MatchLabels))
	}

	if err := k.c.List(ctx, list, listOpts...); err != nil {
		return nil, fmt.Errorf("k8s list %s/%s: %w", kind, namespace, err)
	}
	out := make([]map[string]any, len(list.Items))
	for i := range list.Items {
		out[i] = list.Items[i].Object
	}
	return out, nil
}

// Watch is not implemented by this client. controller-runtime manages the
// watch loop via its internal informer cache. This method returns an error
// to surface misuse clearly.
func (k *Client) Watch(_ context.Context, _, _, _ string, _ deploymanager.ListOptions) (<-chan deploymanager.WatchEvent, error) {
	return nil, fmt.Errorf("k8s.Client.Watch: use controller-runtime informer cache for watches; Watch() is not implemented")
}

// SetStatus patches the status sub-resource of a named object.
func (k *Client) SetStatus(ctx context.Context, apiVersion, kind, name, namespace string, statusVal map[string]any) (bool, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return false, fmt.Errorf("k8s setstatus: parse apiVersion %q: %w", apiVersion, err)
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind})
	obj.SetName(name)
	obj.SetNamespace(namespace)

	// Fetch current to obtain resourceVersion (required for Update).
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := k.c.Get(ctx, key, obj); err != nil {
		return false, fmt.Errorf("k8s setstatus get: %w", err)
	}
	if err := unstructured.SetNestedMap(obj.Object, statusVal, "status"); err != nil {
		return false, fmt.Errorf("k8s setstatus set: %w", err)
	}
	if err := k.c.Status().Update(ctx, obj); err != nil {
		return false, fmt.Errorf("k8s setstatus update: %w", err)
	}
	return true, nil
}

// ── helpers ────────────────────────────────────────────────────────────────────

func toUnstructured(m map[string]any) (*unstructured.Unstructured, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(b, &obj.Object); err != nil {
		return nil, err
	}
	return obj, nil
}

func namespacedName(obj *unstructured.Unstructured) types.NamespacedName {
	return types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
}

// gvkStr returns a human-readable GVK string for error messages.
func gvkStr(m map[string]any) string {
	av, _ := m["apiVersion"].(string)
	kind, _ := m["kind"].(string)
	meta, _ := m["metadata"].(map[string]any)
	name := ""
	if meta != nil {
		name, _ = meta["name"].(string)
	}
	return strings.Join([]string{av, kind, name}, "/")
}

// compile-time assertion that *Client satisfies DeployManager.
var _ deploymanager.DeployManager = (*Client)(nil)

// ensure time import is used (WatchEvent has a Timestamp field).
var _ = time.Time{}
