package errors_test

import (
	"errors"
	"testing"

	oper8err "github.com/example/oper8-go/errors"
)

func TestConfigError_IsFatal(t *testing.T) {
	t.Parallel()
	err := oper8err.NewConfigError("bad spec: %s", "missing version")
	if !oper8err.IsFatal(err) {
		t.Error("ConfigError must be fatal")
	}
	if err.Error() == "" {
		t.Error("error message must not be empty")
	}
}

func TestClusterError_IsFatal(t *testing.T) {
	t.Parallel()
	if !oper8err.IsFatal(oper8err.NewClusterError("cluster down")) {
		t.Error("ClusterError must be fatal")
	}
}

func TestRolloutError_IsFatal(t *testing.T) {
	t.Parallel()
	if !oper8err.IsFatal(oper8err.NewRolloutError("dag failed")) {
		t.Error("RolloutError must be fatal")
	}
}

func TestPreconditionError_IsNotFatal(t *testing.T) {
	t.Parallel()
	err := oper8err.NewPreconditionError("db not ready")
	if oper8err.IsFatal(err) {
		t.Error("PreconditionError must NOT be fatal")
	}
}

func TestVerificationError_IsNotFatal(t *testing.T) {
	t.Parallel()
	if oper8err.IsFatal(oper8err.NewVerificationError("pod not ready")) {
		t.Error("VerificationError must NOT be fatal")
	}
}

func TestIsFatal_Nil(t *testing.T) {
	t.Parallel()
	if oper8err.IsFatal(nil) {
		t.Error("IsFatal(nil) must be false")
	}
}

func TestAssertConfig_Pass(t *testing.T) {
	t.Parallel()
	if err := oper8err.AssertConfig(true, "should not fail"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAssertConfig_Fail(t *testing.T) {
	t.Parallel()
	err := oper8err.AssertConfig(false, "config broken: %s", "field")
	if err == nil {
		t.Fatal("expected error")
	}
	if !oper8err.IsFatal(err) {
		t.Error("AssertConfig failure must be fatal")
	}
}

func TestAssertPrecondition_Fail_IsTransient(t *testing.T) {
	t.Parallel()
	err := oper8err.AssertPrecondition(false, "not ready")
	if err == nil {
		t.Fatal("expected error")
	}
	if oper8err.IsFatal(err) {
		t.Error("AssertPrecondition failure must NOT be fatal")
	}
}

func TestAssertVerified_Fail_IsTransient(t *testing.T) {
	t.Parallel()
	err := oper8err.AssertVerified(false, "pod not ready")
	if err == nil {
		t.Fatal("expected error")
	}
	if oper8err.IsFatal(err) {
		t.Error("AssertVerified failure must NOT be fatal")
	}
}

func TestAssertCluster_Pass(t *testing.T) {
	t.Parallel()
	if err := oper8err.AssertCluster(true, "ok"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// Additional expanded tests appended below the original suite.

func TestIsFatal_NonOper8Error_IsFalse(t *testing.T) {
	t.Parallel()
	// A plain stdlib error is not an Oper8 error — treat as transient (false).
	if oper8err.IsFatal(errors.New("plain error")) {
		t.Error("non-Oper8 errors should not be fatal")
	}
}

func TestNewConfigError_MessageFormatted(t *testing.T) {
	t.Parallel()
	err := oper8err.NewConfigError("key %q is required, got %d", "version", 0)
	if err.Error() == "" {
		t.Error("error message must not be empty")
	}
	if !oper8err.IsFatal(err) {
		t.Error("ConfigError must be fatal")
	}
}

func TestNewPreconditionError_MessageFormatted(t *testing.T) {
	t.Parallel()
	err := oper8err.NewPreconditionError("db %q not ready", "postgres")
	if err.Error() == "" {
		t.Error("error message must not be empty")
	}
}

func TestNewVerificationError_MessageFormatted(t *testing.T) {
	t.Parallel()
	err := oper8err.NewVerificationError("pod %s not ready", "mypod")
	if err.Error() == "" {
		t.Error("error message must not be empty")
	}
}

func TestNewRolloutError_MessageFormatted(t *testing.T) {
	t.Parallel()
	err := oper8err.NewRolloutError("dag failed: %v", "node a")
	if err.Error() == "" {
		t.Error("error message must not be empty")
	}
	if !oper8err.IsFatal(err) {
		t.Error("RolloutError must be fatal")
	}
}

func TestAssertCluster_Fail_IsFatal(t *testing.T) {
	t.Parallel()
	err := oper8err.AssertCluster(false, "cluster unreachable")
	if err == nil {
		t.Fatal("expected error")
	}
	if !oper8err.IsFatal(err) {
		t.Error("AssertCluster failure must be fatal")
	}
}

func TestIsFatal_ErrorInterface_ConfigError(t *testing.T) {
	t.Parallel()
	// Assign to error interface (as code typically passes errors around).
	var err error = oper8err.NewConfigError("bad")
	if !oper8err.IsFatal(err) {
		t.Error("ConfigError via error interface must still be fatal")
	}
}

func TestIsFatal_ErrorInterface_PreconditionError(t *testing.T) {
	t.Parallel()
	var err error = oper8err.NewPreconditionError("not ready")
	if oper8err.IsFatal(err) {
		t.Error("PreconditionError via error interface must not be fatal")
	}
}
