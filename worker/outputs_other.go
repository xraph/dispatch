//go:build !unix

package worker

import "os"

// openRegularNoFollow opens path for reading.
//
// This platform has no portable, dependency-free equivalent of Unix's
// O_NOFOLLOW/O_NONBLOCK open flags in the standard library, so it
// cannot close the narrow TOCTOU window the unix build additionally
// closes (see outputs_unix.go's doc comment). That window requires a
// still-running process to swap a regular file for a symlink or FIFO
// between collectOutputEntries listing it and this function opening it
// — collectOutputEntries' own Lstat-based type filter, run moments
// earlier in the same synchronous walk, is what stops the case this
// package actually exists to prevent: a symlink or FIFO present in
// OutputDir all along is never opened at all, on any platform, because
// it never reaches this function to begin with.
func openRegularNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}

// processAlive reports whether pid names a process that is currently
// running. On this platform, unlike Unix, os.FindProcess itself opens
// a handle to the process and fails if none exists at pid, so the
// FindProcess call is the actual check here — there is no separate
// Signal(0) probe to make it accurate.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	_ = proc.Release() //nolint:errcheck // best-effort handle cleanup

	return true
}
