package verify

import (
	"github.com/example/oper8-go/status"
)

// ── Built-in resource verifiers ───────────────────────────────────────────────
//
// Ported from oper8 Python (verify_resources.py): verify_pod, verify_job,
// verify_deployment, verify_statefulset, verify_subsystem.

const (
	availableConditionKey   = "Available"
	completeConditionKey    = "Complete"
	progressingConditionKey = "Progressing"
	newRSAvailableReason    = "NewReplicaSetAvailable"
)

// VerifyPod returns true when the Pod has a Ready=True condition.
func VerifyPod(objectState map[string]any) bool {
	return verifyCondition(objectState, "Ready", true, DefaultTimestampKey, "")
}

// VerifyJob returns true when the Job has a Complete=True condition.
func VerifyJob(objectState map[string]any) bool {
	return verifyCondition(objectState, completeConditionKey, true, DefaultTimestampKey, "")
}

// VerifyDeployment returns true when the Deployment has:
//   - Available=True
//   - Progressing=True with reason=NewReplicaSetAvailable
//
// This ensures all replicas are up AND the rollout has fully converged.
func VerifyDeployment(objectState map[string]any) bool {
	return verifyCondition(objectState, availableConditionKey, true, DefaultTimestampKey, "") &&
		verifyCondition(objectState, progressingConditionKey, true, DefaultTimestampKey, newRSAvailableReason)
}

// VerifyStatefulSet returns true when all desired replicas are ready.
func VerifyStatefulSet(objectState map[string]any) bool {
	objStatus, _ := objectState["status"].(map[string]any)
	if objStatus == nil {
		return false
	}
	// replicas may be absent when spec.replicas is not set; treat as not ready.
	rawReplicas, ok := objStatus["replicas"]
	if !ok || rawReplicas == nil {
		return false
	}
	expected := toInt(rawReplicas)
	ready := toInt(objStatus["readyReplicas"])
	return ready == expected
}

// VerifySubsystem returns true when an oper8-managed subsystem is fully ready:
// Ready=True, Updating=False, and versions.reconciled == desiredVersion.
func VerifySubsystem(objectState map[string]any, desiredVersion string) bool {
	objStatus, _ := objectState["status"].(map[string]any)
	current := status.GetVersion(objStatus)
	if desiredVersion != "" && current == "" {
		return false
	}
	return verifyCondition(objectState, status.ConditionReady, true, status.TimestampKey, "") &&
		verifyCondition(objectState, status.ConditionUpdating, false, status.TimestampKey, "") &&
		current == desiredVersion
}

// toInt converts common JSON number types to int; returns 0 on nil/unknown.
func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func init() {
	Register("Pod", VerifyPod)
	Register("Job", VerifyJob)
	Register("Deployment", VerifyDeployment)
	Register("StatefulSet", VerifyStatefulSet)
}
