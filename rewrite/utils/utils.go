// Package utils provides shared utility functions used across oper8-go
// components and controllers.
//
// Ported from oper8 Python (utils.py).
package utils

import (
	"fmt"
	"strings"
)

// MergeConfigs performs a deep merge of overrides into base.
// Rules (mirrors Python merge_configs):
//   - If a key exists in both maps and both values are map[string]any,
//     recurse and merge.
//   - Otherwise the override value wins.
//
// base is modified in place and also returned for convenience.
func MergeConfigs(base, overrides map[string]any) map[string]any {
	for k, v := range overrides {
		baseVal, exists := base[k]
		if exists {
			baseMap, baseIsMap := baseVal.(map[string]any)
			overMap, overIsMap := v.(map[string]any)
			if baseIsMap && overIsMap {
				base[k] = MergeConfigs(baseMap, overMap)
				continue
			}
		}
		base[k] = v
	}
	return base
}

// GetNested retrieves a value from a nested map[string]any using dot-separated
// key notation (e.g. "metadata.name").
//
// Returns dflt when any intermediate key is missing or a non-map intermediate
// value is encountered. Mirrors Python's nested_get.
func GetNested(m map[string]any, key string, dflt any) (any, error) {
	parts := strings.SplitN(key, ".", 2)
	val, ok := m[parts[0]]
	if !ok {
		return dflt, nil
	}
	if len(parts) == 1 {
		return val, nil
	}
	child, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("utils: key %q: intermediate value is not a map", parts[0])
	}
	return GetNested(child, parts[1], dflt)
}

// GetNestedString is a convenience wrapper around GetNested that asserts the
// returned value is a string. Returns dflt when the key is absent or the value
// is not a string.
func GetNestedString(m map[string]any, key, dflt string) string {
	v, err := GetNested(m, key, nil)
	if err != nil || v == nil {
		return dflt
	}
	s, ok := v.(string)
	if !ok {
		return dflt
	}
	return s
}

// SetNested sets a value in a nested map[string]any using dot-separated key
// notation (e.g. "metadata.labels.app"). Intermediate maps are created
// automatically. Returns an error if an intermediate key exists but is not a
// map. Mirrors Python's nested_set.
func SetNested(m map[string]any, key string, val any) error {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) == 1 {
		m[parts[0]] = val
		return nil
	}
	child, exists := m[parts[0]]
	if !exists {
		child = make(map[string]any)
		m[parts[0]] = child
	}
	childMap, ok := child.(map[string]any)
	if !ok {
		return fmt.Errorf("utils: key %q: intermediate value is not a map", parts[0])
	}
	return SetNested(childMap, parts[1], val)
}

// GetPassthroughAnnotations returns the subset of annotations from src
// that should be forwarded to child CRs. It copies all keys present in
// allowed. Mirrors Python's get_passthrough_annotations.
func GetPassthroughAnnotations(src map[string]string, allowed []string) map[string]string {
	out := make(map[string]string, len(allowed))
	for _, k := range allowed {
		if v, ok := src[k]; ok {
			out[k] = v
		}
	}
	return out
}
