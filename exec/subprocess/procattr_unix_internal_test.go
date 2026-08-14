//go:build unix

package subprocess

// This file is package subprocess (internal), not subprocess_test — same
// rationale as internal_test.go: sysProcAttr is unexported, and what it
// builds (Setpgid, Credential.Uid/Gid, and the NoSetGroups policy) cannot
// be observed from outside the package without actually spawning a
// process and inspecting its privileges, which for a genuinely different
// uid needs root and is exactly what the brief says not to write. Calling
// sysProcAttr directly with synthetic options pins its contract instead,
// the same way internal_test.go pins classify's.
//
// This exists because TestSameUserIsRefusedByDefault and
// TestSameUserAllowedExplicitly (limits_unix_test.go) exercise
// checkLaunch, a policy function with no dependency on sysProcAttr at
// all — replacing sysProcAttr's entire body with a bare `return
// &syscall.SysProcAttr{Setpgid: true}` (dropping Credential unconditionally)
// leaves both of those tests, and the rest of this package's suite,
// green. That is the regression this file exists to catch: a change
// that silently stops dropping privileges at all, the one thing this
// task exists to make happen.

import (
	"os"
	"testing"
)

func TestSysProcAttrSetsSetpgid(t *testing.T) {
	attr := sysProcAttr(options{})
	if attr == nil {
		t.Fatal("sysProcAttr(options{}) = nil")
	}
	if !attr.Setpgid {
		t.Error("Setpgid = false, want true — Task 4's whole-group kill depends on this")
	}
}

func TestSysProcAttrNoUserConfiguredSetsNoCredential(t *testing.T) {
	attr := sysProcAttr(options{})
	if attr.Credential != nil {
		t.Errorf("Credential = %+v, want nil when no user is configured", attr.Credential)
	}
}

// TestSysProcAttrCredential covers both branches of the NoSetGroups
// policy directly, without spawning a process: the same-uid case CI can
// actually exercise end to end (see TestSameUserAllowedExplicitly), and
// the differing-uid case, which needs root to launch for real and so is
// asserted here as a value, per the brief's own guidance not to write a
// root-only test.
func TestSysProcAttrCredential(t *testing.T) {
	self := os.Getuid()
	selfGid := os.Getgid()

	tests := []struct {
		name            string
		uid, gid        int
		wantNoSetGroups bool
	}{
		{
			name:            "uid and gid both match the caller's own",
			uid:             self,
			gid:             selfGid,
			wantNoSetGroups: true, // the WithAllowSameUser path: Credential is a no-op, setgroups needs privilege this process does not have
		},
		{
			name:            "uid differs",
			uid:             self + 1,
			gid:             selfGid,
			wantNoSetGroups: false, // a genuine drop: already needs CAP_SETUID/root, so setgroups actually runs and clears supplementary groups
		},
		{
			name:            "gid differs",
			uid:             self,
			gid:             selfGid + 1,
			wantNoSetGroups: false,
		},
		{
			name:            "both differ",
			uid:             self + 1,
			gid:             selfGid + 1,
			wantNoSetGroups: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attr := sysProcAttr(options{hasUser: true, uid: tt.uid, gid: tt.gid})

			if attr.Credential == nil {
				t.Fatal("Credential = nil, want non-nil when hasUser is true")
			}
			if attr.Credential.Uid != uint32(tt.uid) {
				t.Errorf("Credential.Uid = %d, want %d", attr.Credential.Uid, tt.uid)
			}
			if attr.Credential.Gid != uint32(tt.gid) {
				t.Errorf("Credential.Gid = %d, want %d", attr.Credential.Gid, tt.gid)
			}
			if attr.Credential.NoSetGroups != tt.wantNoSetGroups {
				t.Errorf("Credential.NoSetGroups = %v, want %v", attr.Credential.NoSetGroups, tt.wantNoSetGroups)
			}
		})
	}
}

// TestSysProcAttrAlwaysSetsSetpgidWithUser guards against a regression
// where adding Credential handling accidentally drops Setpgid — the two
// are independent fields on the same struct, and Task 4's guarantee must
// survive Task 5's addition regardless of whether a user is configured.
func TestSysProcAttrAlwaysSetsSetpgidWithUser(t *testing.T) {
	attr := sysProcAttr(options{hasUser: true, uid: os.Getuid(), gid: os.Getgid()})
	if !attr.Setpgid {
		t.Error("Setpgid = false, want true even when a user is configured")
	}
}
