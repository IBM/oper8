// Package constants holds shared annotation keys and other string constants
// used across oper8-go packages.
//
// Ported from oper8 Python (constants.py).
package constants

// Annotation keys used by oper8-go controllers and predicates.
const (
	// PauseAnnotation suppresses reconciliation when set to "true".
	// Mirrors Python's PAUSE_ANNOTATION_NAME.
	PauseAnnotation = "oper8.org/pause-execution"

	// ConfigDefaultsAnnotation carries JSON-encoded config defaults
	// that override the operator's built-in defaults.
	ConfigDefaultsAnnotation = "oper8.org/config-defaults"

	// TemporaryPatchesAnnotation holds a JSON-encoded map of active
	// temporary patches on a target CR. Managed by TemporaryPatchComponent.
	TemporaryPatchesAnnotation = "oper8.org/temporary-patches"

	// InternalNameAnnotation records the operator-internal name of a
	// managed Kubernetes resource so the TemporaryPatch system can route
	// patches to the correct object.
	InternalNameAnnotation = "oper8.org/internal-name"

	// LeaseNameAnnotation identifies which Lease object is used for
	// per-resource leader election (legacy Python annotation, preserved
	// for compatibility).
	LeaseNameAnnotation = "oper8.org/lease-name"

	// LeaseTimeAnnotation records the last-acquired lease timestamp.
	LeaseTimeAnnotation = "oper8.org/lease-time"

	// LogDefaultLevelAnnotation overrides the default log level for a
	// single reconcile.
	LogDefaultLevelAnnotation = "oper8.org/log-default-level"

	// LogFiltersAnnotation provides per-reconcile log filter overrides.
	LogFiltersAnnotation = "oper8.org/log-filters"
)

// PassthroughAnnotations is the set of oper8 annotations that should be
// propagated from a parent CR to any child subsystem CRs it creates.
// Mirrors Python's PASSTHROUGH_ANNOTATIONS list.
var PassthroughAnnotations = []string{
	ConfigDefaultsAnnotation,
	LogDefaultLevelAnnotation,
	LogFiltersAnnotation,
	PauseAnnotation,
}

// Miscellaneous framework constants.
const (
	// NestedDictDelim is the separator for dotted-key access in nested maps.
	// Mirrors Python's NESTED_DICT_DELIM.
	NestedDictDelim = "."

	// DefaultNamespace is used when no namespace is specified.
	DefaultNamespace = "default"

	// ConfigOverridesKey is the spec key used to provide per-CR config overrides.
	ConfigOverridesKey = "configOverrides"
)
