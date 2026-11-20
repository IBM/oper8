package deploymanager

import "fmt"

// OwnerRef builds an ownerReference entry for childObj that points to ownerCR.
// Both must have apiVersion, kind, and metadata.{name,namespace,uid}.
//
// Ported from deploy_manager/owner_references.py.
//
// NOTE: blockOwnerDeletion is set to true (parent not deleted until child
// finishes). controller is intentionally omitted — only one ownerRef may set
// controller:true, and oper8 does not need adoption behaviour.
func OwnerRef(ownerCR map[string]any) (map[string]any, error) {
	meta, ok := ownerCR["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("ownerRef: missing metadata in owner CR")
	}
	uid, _ := meta["uid"].(string)
	name, _ := meta["name"].(string)
	if uid == "" || name == "" {
		return nil, fmt.Errorf("ownerRef: owner CR metadata.uid and metadata.name are required")
	}
	return map[string]any{
		"apiVersion":         ownerCR["apiVersion"],
		"kind":               ownerCR["kind"],
		"name":               name,
		"uid":                uid,
		"blockOwnerDeletion": true,
	}, nil
}

// ApplyOwnerRef stamps ownerCR's ownerReference onto childObj if they share
// the same namespace and the reference is not already present.
// It is a no-op when ownerCR and childObj have the same UID (owner == child).
func ApplyOwnerRef(ownerCR, childObj map[string]any) error {
	if err := validateObj("owner", ownerCR); err != nil {
		return err
	}
	if err := validateObj("child", childObj); err != nil {
		return err
	}

	ownerMeta := ownerCR["metadata"].(map[string]any)
	childMeta := childObj["metadata"].(map[string]any)

	ownerUID, _ := ownerMeta["uid"].(string)
	childUID, _ := childMeta["uid"].(string)
	if ownerUID != "" && ownerUID == childUID {
		return nil // owner is same object as child — skip
	}

	ownerNS, _ := ownerMeta["namespace"].(string)
	childNS, _ := childMeta["namespace"].(string)
	if ownerNS != childNS {
		return nil // cross-namespace owner references are not supported in K8s
	}

	// Read existing ownerReferences.
	existing, _ := childMeta["ownerReferences"].([]any)
	for _, ref := range existing {
		r, _ := ref.(map[string]any)
		if uid, _ := r["uid"].(string); uid == ownerUID {
			return nil // already present
		}
	}

	ref, err := OwnerRef(ownerCR)
	if err != nil {
		return err
	}
	childMeta["ownerReferences"] = append(existing, ref)
	return nil
}

func validateObj(label string, obj map[string]any) error {
	if obj["kind"] == nil {
		return fmt.Errorf("ApplyOwnerRef: %s object missing 'kind'", label)
	}
	if obj["apiVersion"] == nil {
		return fmt.Errorf("ApplyOwnerRef: %s object missing 'apiVersion'", label)
	}
	meta, ok := obj["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("ApplyOwnerRef: %s object missing 'metadata'", label)
	}
	if meta["name"] == nil {
		return fmt.Errorf("ApplyOwnerRef: %s object missing metadata.name", label)
	}
	if meta["namespace"] == nil {
		return fmt.Errorf("ApplyOwnerRef: %s object missing metadata.namespace", label)
	}
	return nil
}
