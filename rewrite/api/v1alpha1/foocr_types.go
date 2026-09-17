// Package v1alpha1 contains API types for the example.com operator group.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FooCRSpec defines the desired state of FooCR.
type FooCRSpec struct {
	// Version is the application version to deploy.
	// +kubebuilder:validation:MinLength=1
	Version string `json:"version"`

	// Replicas is the desired number of instances (1–10).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`
}

// FooCRStatus defines the observed state of FooCR.
type FooCRStatus struct {
	// Conditions are the standard status conditions reported by oper8.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=foo
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FooCR is the Schema for the foocrs API.
type FooCR struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FooCRSpec   `json:"spec,omitempty"`
	Status FooCRStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// FooCRList contains a list of FooCR objects.
type FooCRList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FooCR `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FooCR{}, &FooCRList{})
}
