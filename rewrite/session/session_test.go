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

func crWithStatus(name, namespace string, statusFields map[string]any) map[string]any {
	obj := cr(name, namespace, "Foo", "test.io/v1")
	obj["status"] = statusFields
	return obj
}

func newSession(t *testing.T, crManifest map[string]any, dm deploymanager.DeployManager) *session.Session {
	t.Helper()
	sess, err := session.New(context.Background(), "test-id", crManifest, dm)
	if err != nil {
		t.Fatalf("session.New: %v", err)
	}
	return sess
}

// ── construction ─────────────────────────────────────────────────────────────

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
	if sess.Graph.Empty() == false {
		// Empty graph at start.
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

// ── current version from existing status ─────────────────────────────────────

func TestNew_CurrentVersionFromStatus(t *testing.T) {
	manifest := cr("app", "ns", "Foo", "test.io/v1")
	// Pre-seed the cluster with a status that has a reconciled version.
	withStatus := crWithStatus("app", "ns", map[string]any{
		"versions": map[string]any{"reconciled": "2.0.0"},
	})
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
	// DM has no pre-seeded object — Get returns nil.
	dm := deploymanager.NewDryRunDeployManager(nil)
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

	n1 := dag.NewNode("widget")
	n2 := dag.NewNode("widget")
	_ = sess.AddComponent(n1)
	if err := sess.AddComponent(n2); err == nil {
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

	got := sess.ScopedName("db")
	want := "myapp-db"
	if got != want {
		t.Errorf("ScopedName = %q, want %q", got, want)
	}
}

func TestScopedName_TruncatesLongName(t *testing.T) {
	manifest := cr("myapp", "ns", "Foo", "test.io/v1")
	dm := deploymanager.NewDryRunDeployManager(nil, manifest)
	sess := newSession(t, manifest, dm)

	long := strings.Repeat("x", 60)
	got := sess.ScopedName(long)
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
		t.Errorf("63-char name should not be truncated")
	}
}

func TestTruncateName_OverBoundary(t *testing.T) {
	name := strings.Repeat("a", 64)
	got := session.TruncateName(name)
	if len(got) != 63 {
		t.Errorf("TruncateName length = %d, want 63", len(got))
	}
}

func TestTruncateName_Uniqueness(t *testing.T) {
	// Two different long names must not produce the same truncated result.
	base := strings.Repeat("a", 60)
	n1 := session.TruncateName(base + "XXXX")
	n2 := session.TruncateName(base + "YYYY")
	if n1 == n2 {
		t.Error("different long names produced identical truncated result")
	}
}
