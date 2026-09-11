package session_test

import (
	"context"
	"strings"
	"testing"

	"github.com/example/oper8-go/dag"
	"github.com/example/oper8-go/deploymanager"
	"github.com/example/oper8-go/session"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func cr(name, namespace, kind, av string) map[string]any {
	return map[string]any{
		"apiVersion": av,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"uid":       "uid-abc",
		},
	}
}

func newSession(t *testing.T, crManifest map[string]any, dm deploymanager.DeployManager) *session.Session {
	t.Helper()
	sess, err := session.New(context.Background(), "test-id", crManifest, dm)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	return sess
}

// ── construction ──────────────────────────────────────────────────────────────

func TestNew_ConstructedProperties(t *testing.T) {
	manifest := cr("myapp", "staging", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	if sess.ID != "test-id" {
		t.Errorf("ID = %q, want %q", sess.ID, "test-id")
	}
	if sess.Graph == nil {
		t.Error("Graph must not be nil")
	}
	if !sess.Graph.Empty() {
		t.Error("Graph should be empty at construction")
	}
}

func TestNew_CRFieldAccessors(t *testing.T) {
	manifest := cr("myapp", "staging", "Foo", "test.io/v1")
	manifest["spec"] = map[string]any{"version": "1.2.3"}
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	tests := []struct{ got, want string }{
		{sess.Name(), "myapp"},
		{sess.Namespace(), "staging"},
		{sess.Kind(), "Foo"},
		{sess.APIVersion(), "test.io/v1"},
		{sess.Version(), "1.2.3"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}

func TestNew_VersionAbsentInSpec(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)
	if sess.Version() != "" {
		t.Errorf("Version() = %q, want empty when spec.version absent", sess.Version())
	}
}

// ── current version from existing status ──────────────────────────────────────

func TestNew_CurrentVersionFromStatus(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	withStatus := map[string]any{
		"apiVersion": "test.io/v1",
		"kind":       "Foo",
		"metadata":   map[string]any{"name": "app", "namespace": "ns", "uid": "uid-abc"},
		"status":     map[string]any{"versions": map[string]any{"reconciled": "2.0.0"}},
	}
	dm := deploymanager.NewDryRunDeployManager(nil, withStatus)
	sess := newSession(t, manifest, dm)

	if sess.CurrentVersion != "2.0.0" {
		t.Errorf("CurrentVersion = %q, want %q", sess.CurrentVersion, "2.0.0")
	}
}

func TestNew_CurrentVersionEmptyWhenNeverReconciled(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)
	if sess.CurrentVersion != "" {
		t.Errorf("CurrentVersion = %q, want empty for fresh CR", sess.CurrentVersion)
	}
}

func TestNew_StatusEmptyWhenCRNotFound(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil) // no pre-seeded object
	sess := newSession(t, manifest, dm)
	if len(sess.Status) != 0 {
		t.Errorf("Status should be empty map when CR not found, got %v", sess.Status)
	}
}

// ── CR validation ─────────────────────────────────────────────────────────────

func TestNew_MissingKind(t *testing.T) {
	bad := map[string]any{
		"apiVersion": "test.io/v1",
		"metadata":   map[string]any{"name": "a", "namespace": "b"},
	}
	_, err := session.New(context.Background(), "id", bad, deploymanager.NewDryRunDeployManager(nil))
	if err == nil {
		t.Fatal("expected error for missing kind")
	}
}

func TestNew_MissingAPIVersion(t *testing.T) {
	bad := map[string]any{
		"kind":     "Foo",
		"metadata": map[string]any{"name": "a", "namespace": "b"},
	}
	_, err := session.New(context.Background(), "id", bad, deploymanager.NewDryRunDeployManager(nil))
	if err == nil {
		t.Fatal("expected error for missing apiVersion")
	}
}

func TestNew_MissingMetadata(t *testing.T) {
	bad := map[string]any{"kind": "Foo", "apiVersion": "test.io/v1"}
	_, err := session.New(context.Background(), "id", bad, deploymanager.NewDryRunDeployManager(nil))
	if err == nil {
		t.Fatal("expected error for missing metadata")
	}
}

func TestNew_MissingMetadataName(t *testing.T) {
	bad := map[string]any{
		"kind":       "Foo",
		"apiVersion": "test.io/v1",
		"metadata":   map[string]any{"namespace": "ns"},
	}
	_, err := session.New(context.Background(), "id", bad, deploymanager.NewDryRunDeployManager(nil))
	if err == nil {
		t.Fatal("expected error for missing metadata.name")
	}
}

func TestNew_MissingMetadataNamespace(t *testing.T) {
	bad := map[string]any{
		"kind":       "Foo",
		"apiVersion": "test.io/v1",
		"metadata":   map[string]any{"name": "a"},
	}
	_, err := session.New(context.Background(), "id", bad, deploymanager.NewDryRunDeployManager(nil))
	if err == nil {
		t.Fatal("expected error for missing metadata.namespace")
	}
}

// ── component DAG helpers ─────────────────────────────────────────────────────

func TestAddComponent_OK(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	n := dag.NewNode("widget")
	if err := sess.AddComponent(n); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	if _, ok := sess.Graph.GetNode("widget"); !ok {
		t.Error("node 'widget' not found in graph after AddComponent")
	}
}

func TestAddComponent_NoDuplicate(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	_ = sess.AddComponent(dag.NewNode("widget"))
	if err := sess.AddComponent(dag.NewNode("widget")); err == nil {
		t.Fatal("expected error for duplicate component name")
	}
}

func TestAddDependency_OK(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	nA := dag.NewNode("a")
	nB := dag.NewNode("b")
	_ = sess.AddComponent(nA)
	_ = sess.AddComponent(nB)
	if err := sess.AddDependency(nB, nA, nil); err != nil {
		t.Fatalf("AddDependency: %v", err)
	}
}

func TestAddDependency_UnknownNode(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	nA := dag.NewNode("a")
	nB := dag.NewNode("b") // not added to graph
	_ = sess.AddComponent(nA)
	if err := sess.AddDependency(nA, nB, nil); err == nil {
		t.Fatal("expected error when depending on unknown node")
	}
}

// ── name utilities ────────────────────────────────────────────────────────────

func TestScopedName_Short(t *testing.T) {
	manifest := cr("myapp", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	if got := sess.ScopedName("db"); got != "myapp-db" {
		t.Errorf("ScopedName = %q, want %q", got, "myapp-db")
	}
}

func TestScopedName_TruncatesLongName(t *testing.T) {
	manifest := cr("myapp", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	got := sess.ScopedName(strings.Repeat("x", 60))
	if len(got) > 63 {
		t.Errorf("ScopedName length = %d, must be ≤ 63", len(got))
	}
}

func TestTruncateName_ShortPassthrough(t *testing.T) {
	name := "short-name"
	if got := session.TruncateName(name); got != name {
		t.Errorf("TruncateName(%q) = %q, want passthrough", name, got)
	}
}

func TestTruncateName_ExactBoundary(t *testing.T) {
	name := strings.Repeat("a", 63)
	if got := session.TruncateName(name); got != name {
		t.Error("63-char name should not be truncated")
	}
}

func TestTruncateName_OverBoundary(t *testing.T) {
	name := strings.Repeat("a", 64)
	if got := session.TruncateName(name); len(got) != 63 {
		t.Errorf("TruncateName length = %d, want 63", len(got))
	}
}

func TestTruncateName_Uniqueness(t *testing.T) {
	base := strings.Repeat("a", 60)
	n1 := session.TruncateName(base + "XXXX")
	n2 := session.TruncateName(base + "YYYY")
	if n1 == n2 {
		t.Error("different long names produced identical truncated result")
	}
}

// ── Additional session tests ──────────────────────────────────────────────────

func TestNew_GraphStartsEmpty(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)
	if len(sess.Graph.Nodes()) != 0 {
		t.Errorf("Graph should have 0 nodes at construction, got %d", len(sess.Graph.Nodes()))
	}
}

func TestNew_IDIsPreserved(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess, err := session.New(context.Background(), "my-unique-id", manifest, dm)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	if sess.ID != "my-unique-id" {
		t.Errorf("ID = %q, want my-unique-id", sess.ID)
	}
}

func TestNew_DeployManagerPreserved(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)
	if sess.DeployManager == nil {
		t.Error("DeployManager must not be nil")
	}
}

func TestAddComponent_MultipleComponents(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	for _, name := range []string{"a", "b", "c"} {
		if err := sess.AddComponent(dag.NewNode(name)); err != nil {
			t.Fatalf("AddComponent(%q): %v", name, err)
		}
	}
	if len(sess.Graph.Nodes()) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(sess.Graph.Nodes()))
	}
}

func TestAddDependency_ChainOfThree(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	nA := dag.NewNode("a")
	nB := dag.NewNode("b")
	nC := dag.NewNode("c")
	_ = sess.AddComponent(nA)
	_ = sess.AddComponent(nB)
	_ = sess.AddComponent(nC)
	// b depends on a; c depends on b
	if err := sess.AddDependency(nB, nA, nil); err != nil {
		t.Fatalf("AddDependency(b, a): %v", err)
	}
	if err := sess.AddDependency(nC, nB, nil); err != nil {
		t.Fatalf("AddDependency(c, b): %v", err)
	}
}

func TestScopedName_AtExactLimit(t *testing.T) {
	// Build a CR whose name makes ScopedName exactly 63 chars — should NOT truncate.
	// "x" (1) + "-" (1) + "a"*61 = 63 chars
	suffix := strings.Repeat("a", 61)
	manifest := cr("x", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)
	name := sess.ScopedName(suffix)
	if len(name) > 63 {
		t.Errorf("ScopedName should be ≤63 chars, got %d: %s", len(name), name)
	}
}

func TestTruncateName_AlreadyShort(t *testing.T) {
	n := session.TruncateName("short")
	if n != "short" {
		t.Errorf("TruncateName of short name should be identity, got %q", n)
	}
}

func TestTruncateName_Long_IsDeterministic(t *testing.T) {
	long := strings.Repeat("abcdefgh", 10) // 80 chars
	a := session.TruncateName(long)
	b := session.TruncateName(long)
	if a != b {
		t.Error("TruncateName must be deterministic for the same input")
	}
	if len(a) > 63 {
		t.Errorf("truncated name must be ≤63 chars, got %d", len(a))
	}
}

func TestTruncateName_DifferentLongNames_DifferentResults(t *testing.T) {
	// Two different long names must produce different truncated names (hash suffix).
	long1 := strings.Repeat("a", 70)
	long2 := strings.Repeat("b", 70)
	if session.TruncateName(long1) == session.TruncateName(long2) {
		t.Error("different long names must not produce the same truncated name")
	}
}

func TestNew_StatusPopulatedFromExistingCR(t *testing.T) {
	// Seed the DryRunDeployManager with a CR that already has a status block.
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	withStatus := map[string]any{
		"apiVersion": "test.io/v1",
		"kind":       "Foo",
		"metadata":   map[string]any{"name": "app", "namespace": "ns", "uid": "uid-xyz"},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Ready", "status": "True"},
			},
			"versions": map[string]any{"reconciled": "3.0.0"},
		},
	}
	dm := deploymanager.NewDryRunDeployManager(nil, withStatus)
	sess := newSession(t, manifest, dm)

	if len(sess.Status) == 0 {
		t.Error("Session.Status should be populated from existing CR status")
	}
	if sess.CurrentVersion != "3.0.0" {
		t.Errorf("CurrentVersion = %q, want 3.0.0", sess.CurrentVersion)
	}
}
