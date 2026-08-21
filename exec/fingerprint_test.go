package exec_test

import (
	"testing"

	"github.com/xraph/dispatch/exec"
)

func TestFingerprintOf_StableAcrossOrder(t *testing.T) {
	a := exec.FingerprintOf([]string{"b.job", "a.job", "c.job"}, "abc123")
	b := exec.FingerprintOf([]string{"a.job", "b.job", "c.job"}, "abc123")

	if a != b {
		t.Errorf("fingerprint depends on order: %q != %q", a, b)
	}
}

func TestFingerprintOf_ChangesWithNames(t *testing.T) {
	a := exec.FingerprintOf([]string{"a.job"}, "abc123")
	b := exec.FingerprintOf([]string{"a.job", "b.job"}, "abc123")

	if a == b {
		t.Error("fingerprint did not change when a handler was added")
	}
}

func TestFingerprintOf_ChangesWithRevision(t *testing.T) {
	a := exec.FingerprintOf([]string{"a.job"}, "abc123")
	b := exec.FingerprintOf([]string{"a.job"}, "def456")

	if a == b {
		t.Error("fingerprint did not change with the build revision")
	}
}

func TestFingerprintOf_DoesNotCollideOnSeparatorAmbiguity(t *testing.T) {
	// {"a", "b"} and {"a\nb"} must not hash the same, or a handler named
	// with an embedded separator could impersonate a two-handler set.
	a := exec.FingerprintOf([]string{"a", "b"}, "r")
	b := exec.FingerprintOf([]string{"a\nb"}, "r")

	if a == b {
		t.Error("separator ambiguity produced a collision")
	}
}

func TestFingerprintOf_Empty(t *testing.T) {
	if got := exec.FingerprintOf(nil, "r"); got == "" {
		t.Error("FingerprintOf(nil) = empty, want a hash")
	}
}
