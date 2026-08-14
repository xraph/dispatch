//go:build !unix

package worker

import "os"

// openRegularNoFollow opens path for reading.
//
// This platform has no portable, dependency-free equivalent of Unix's
// O_NOFOLLOW open flag in the standard library, so it cannot close the
// narrow TOCTOU window the unix build additionally closes (see
// outputs_unix.go's doc comment). That window requires a still-running
// process to swap a regular file for a symlink between
// collectOutputEntries listing it and this function opening it —
// collectOutputEntries' own Lstat-based type filter, run moments
// earlier in the same synchronous walk, is what stops the case this
// package actually exists to prevent: a symlink present in OutputDir
// all along is never opened at all, on any platform, because it never
// reaches this function to begin with.
func openRegularNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}
