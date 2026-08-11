package deploymanager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// DryRunDeployManager implements [DeployManager] entirely in-memory.
// It is the primary tool for unit-testing controllers and components
// without a live cluster.
//
// Ported from deploy_manager/dry_run_deploy_manager.py.
//
// Thread safety: all cluster-state mutations are protected by a single RWMutex.
// Watch channels are individually buffered; slow consumers cause no blocking.
type DryRunDeployManager struct {
	mu      sync.RWMutex
	store   clusterStore        // namespace→kind→apiVersion→name→object
	watches []watchRegistration // active watch subscriptions
	ownerCR map[string]any      // optional; stamped onto every deployed object
}

// clusterStore is the nested map that simulates the cluster etcd store.
// Key path: namespace / kind / apiVersion / name → object dict.
type clusterStore map[string]map[string]map[string]map[string]map[string]any

// watchRegistration holds a subscriber channel and the filters it cares about.
type watchRegistration struct {
	apiVersion string
	kind       string
	namespace  string // "" = any namespace
	name       string // "" = any name
	ch         chan WatchEvent
}

// NewDryRunDeployManager creates an empty in-memory deploy manager.
// Pass initial resources to pre-populate the cluster state.
func NewDryRunDeployManager(ownerCR map[string]any, initial ...map[string]any) *DryRunDeployManager {
	dm := &DryRunDeployManager{
		store:   make(clusterStore),
		ownerCR: ownerCR,
	}
	// Pre-populate without triggering watches (call_watches=false equivalent).
	for _, r := range initial {
		_, _ = dm.storeObject(r, DeployMethodDefault, false, false)
	}
	return dm
}

// ── DeployManager interface ───────────────────────────────────────────────────

func (dm *DryRunDeployManager) Deploy(
	_ context.Context,
	resources []map[string]any,
	method DeployMethod,
	manageOwnerRefs bool,
) (bool, error) {
	var anyChanged bool
	for _, r := range resources {
		if manageOwnerRefs && dm.ownerCR != nil {
			if err := ApplyOwnerRef(dm.ownerCR, r); err != nil {
				return false, fmt.Errorf("deploy: owner ref: %w", err)
			}
		}
		changed, err := dm.storeObject(r, method, true, true)
		if err != nil {
			return false, err
		}
		if changed {
			anyChanged = true
		}
	}
	return anyChanged, nil
}

func (dm *DryRunDeployManager) Delete(_ context.Context, resources []map[string]any) (bool, error) {
	var anyChanged bool
	for _, r := range resources {
		changed := dm.deleteObject(r)
		if changed {
			anyChanged = true
		}
	}
	return anyChanged, nil
}

func (dm *DryRunDeployManager) Get(_ context.Context, apiVersion, kind, name, namespace string) (map[string]any, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	obj := dm.store.get(namespace, kind, apiVersion, name)
	if obj == nil {
		return nil, nil
	}
	return deepCopy(obj), nil
}

func (dm *DryRunDeployManager) List(_ context.Context, apiVersion, kind, namespace string, opts ListOptions) ([]map[string]any, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	ns := dm.store[namespace]
	if ns == nil {
		return nil, nil
	}
	kindMap := ns[kind]
	if kindMap == nil {
		return nil, nil
	}

	var results []map[string]any
	for av, names := range kindMap {
		if apiVersion != "" && av != apiVersion {
			continue
		}
		for _, obj := range names {
			if opts.LabelSelector != "" {
				labels, _ := nestedMap(obj, "metadata", "labels")
				if !matchSelector(labels, opts.LabelSelector) {
					continue
				}
			}
			results = append(results, deepCopy(obj))
		}
	}
	return results, nil
}

func (dm *DryRunDeployManager) Watch(ctx context.Context, apiVersion, kind, namespace string, _ ListOptions) (<-chan WatchEvent, error) {
	ch := make(chan WatchEvent, 64)

	dm.mu.Lock()
	dm.watches = append(dm.watches, watchRegistration{
		apiVersion: apiVersion,
		kind:       kind,
		namespace:  namespace,
		ch:         ch,
	})
	dm.mu.Unlock()

	// Seed the channel with ADDED events for already-existing objects.
	existing, _ := dm.List(ctx, apiVersion, kind, namespace, ListOptions{})
	for _, obj := range existing {
		select {
		case ch <- WatchEvent{Type: EventAdded, Object: obj, Timestamp: time.Now()}:
		default:
		}
	}

	// Close the channel when ctx is done.
	go func() {
		<-ctx.Done()
		dm.mu.Lock()
		for i, w := range dm.watches {
			if w.ch == ch {
				dm.watches = append(dm.watches[:i], dm.watches[i+1:]...)
				break
			}
		}
		dm.mu.Unlock()
		close(ch)
	}()

	return ch, nil
}

func (dm *DryRunDeployManager) SetStatus(_ context.Context, apiVersion, kind, name, namespace string, status map[string]any) (bool, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	obj := dm.store.get(namespace, kind, apiVersion, name)
	if obj == nil {
		return false, fmt.Errorf("SetStatus: object %s/%s/%s/%s not found", namespace, kind, apiVersion, name)
	}
	prev, _ := json.Marshal(obj["status"])
	obj["status"] = deepCopy(status)
	next, _ := json.Marshal(obj["status"])
	return string(prev) != string(next), nil
}

// ── Test helpers ──────────────────────────────────────────────────────────────

// GetStored returns the raw stored object for assertions in tests.
// Returns nil if not found.
func (dm *DryRunDeployManager) GetStored(namespace, kind, apiVersion, name string) map[string]any {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	obj := dm.store.get(namespace, kind, apiVersion, name)
	if obj == nil {
		return nil
	}
	return deepCopy(obj)
}

// ObjectCount returns the total number of objects in the in-memory store.
func (dm *DryRunDeployManager) ObjectCount() int {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	n := 0
	for _, kinds := range dm.store {
		for _, avs := range kinds {
			for _, names := range avs {
				n += len(names)
			}
		}
	}
	return n
}

// ── Internal helpers ─────────────────────────────────────────────────────────

// storeObject writes obj into the store. If notifyWatches, subscribers
// are sent ADDED or MODIFIED events. Returns whether the object changed.
func (dm *DryRunDeployManager) storeObject(obj map[string]any, method DeployMethod, setMeta, notifyWatches bool) (bool, error) {
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	meta, _ := obj["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	namespace, _ := meta["namespace"].(string)

	if apiVersion == "" || kind == "" || name == "" {
		return false, fmt.Errorf("storeObject: object missing apiVersion, kind, or metadata.name")
	}

	dm.mu.Lock()
	existing := dm.store.get(namespace, kind, apiVersion, name)

	// Determine event type before mutation.
	evType := EventAdded
	if existing != nil {
		evType = EventModified
	}

	// Apply deploy method.
	var stored map[string]any
	switch method {
	case DeployMethodUpdate:
		if existing != nil {
			stored = mergeMaps(deepCopy(existing), deepCopy(obj))
		} else {
			stored = deepCopy(obj)
		}
	default: // Default and Replace both do full replacement.
		stored = deepCopy(obj)
	}

	// Preserve creationTimestamp and uid from existing object.
	if setMeta {
		storedMeta, _ := stored["metadata"].(map[string]any)
		if storedMeta == nil {
			storedMeta = make(map[string]any)
			stored["metadata"] = storedMeta
		}
		if existing != nil {
			if exMeta, ok := existing["metadata"].(map[string]any); ok {
				if ts := exMeta["creationTimestamp"]; ts != nil {
					storedMeta["creationTimestamp"] = ts
				}
				if uid := exMeta["uid"]; uid != nil {
					storedMeta["uid"] = uid
				}
			}
		}
		if storedMeta["creationTimestamp"] == nil {
			storedMeta["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
		}
	}

	changed := !mapsEqual(existing, stored)
	dm.store.set(namespace, kind, apiVersion, name, stored)
	dm.mu.Unlock()

	if notifyWatches && changed {
		dm.notifyWatches(apiVersion, kind, namespace, name, evType, stored)
	}
	return changed, nil
}

func (dm *DryRunDeployManager) deleteObject(obj map[string]any) bool {
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	meta, _ := obj["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	namespace, _ := meta["namespace"].(string)

	dm.mu.Lock()
	existing := dm.store.get(namespace, kind, apiVersion, name)
	if existing == nil {
		dm.mu.Unlock()
		return false
	}
	dm.store.delete(namespace, kind, apiVersion, name)
	dm.mu.Unlock()

	dm.notifyWatches(apiVersion, kind, namespace, name, EventDeleted, deepCopy(existing))
	return true
}

func (dm *DryRunDeployManager) notifyWatches(apiVersion, kind, namespace, name string, evType EventType, obj map[string]any) {
	evt := WatchEvent{Type: evType, Object: obj, Timestamp: time.Now()}
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	for _, w := range dm.watches {
		if w.apiVersion != "" && w.apiVersion != apiVersion {
			continue
		}
		if w.kind != "" && w.kind != kind {
			continue
		}
		if w.namespace != "" && w.namespace != namespace {
			continue
		}
		if w.name != "" && w.name != name {
			continue
		}
		select {
		case w.ch <- evt:
		default: // drop if consumer is slow — watch channels are best-effort
		}
	}
}

// ── clusterStore helpers ─────────────────────────────────────────────────────

func (s clusterStore) get(ns, kind, av, name string) map[string]any {
	return s[ns][kind][av][name]
}

func (s clusterStore) set(ns, kind, av, name string, obj map[string]any) {
	if s[ns] == nil {
		s[ns] = make(map[string]map[string]map[string]map[string]any)
	}
	if s[ns][kind] == nil {
		s[ns][kind] = make(map[string]map[string]map[string]any)
	}
	if s[ns][kind][av] == nil {
		s[ns][kind][av] = make(map[string]map[string]any)
	}
	s[ns][kind][av][name] = obj
}

func (s clusterStore) delete(ns, kind, av, name string) {
	delete(s[ns][kind][av], name)
}

// ── Pure helpers ─────────────────────────────────────────────────────────────

// deepCopy marshals and unmarshals via JSON — simple, correct for map[string]any.
func deepCopy(v map[string]any) map[string]any {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

// mergeMaps shallowly merges src into dst (dst wins on conflict).
func mergeMaps(dst, src map[string]any) map[string]any {
	for k, v := range src {
		if _, exists := dst[k]; !exists {
			dst[k] = v
		}
	}
	return dst
}

// mapsEqual compares two maps by JSON serialisation.
func mapsEqual(a, b map[string]any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

// nestedMap digs into a map[string]any by successive keys.
func nestedMap(obj map[string]any, keys ...string) (map[string]any, bool) {
	cur := obj
	for _, k := range keys {
		next, ok := cur[k].(map[string]any)
		if !ok {
			return nil, false
		}
		cur = next
	}
	return cur, true
}

// matchSelector does very basic key=value,key=value label matching.
// It supports = == != operators only (the subset needed for dry-run tests).
// A full implementation for production would handle set-based selectors.
func matchSelector(labels map[string]any, selector string) bool {
	for _, part := range strings.Split(selector, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		switch {
		case strings.Contains(part, "!="):
			kv := strings.SplitN(part, "!=", 2)
			val, _ := labels[strings.TrimSpace(kv[0])].(string)
			if val == strings.TrimSpace(kv[1]) {
				return false
			}
		case strings.Contains(part, "="):
			kv := strings.SplitN(part, "=", 2)
			key := strings.TrimSuffix(strings.TrimSpace(kv[0]), "=")
			val, _ := labels[key].(string)
			if val != strings.TrimSpace(kv[1]) {
				return false
			}
		default:
			// existence check
			if _, ok := labels[part]; !ok {
				return false
			}
		}
	}
	return true
}
