//go:build !unix

package subprocess

import (
	osexec "os/exec"
	"syscall"
)

// sysProcAttr is a no-op outside Unix: process groups are a POSIX concept
// this rung does not emulate anywhere else, and this package does not
// otherwise claim to support a non-Unix platform.
func sysProcAttr() *syscall.SysProcAttr { return nil }

// killGroup falls back to killing the process directly outside Unix,
// since there is no process group to address as a whole.
func killGroup(cmd *osexec.Cmd) error {
	return cmd.Process.Kill()
}
