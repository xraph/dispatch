//go:build unix

package worker

import (
	"os"
	"syscall"
)

// openRegularNoFollow opens path for reading, refusing to follow a
// symlink at the final path component and refusing to block if it has
// instead become a FIFO with no writer.
//
// collectOutputEntries already filters out a symlink or FIFO dirent by
// its Lstat-reported type before any path derived from it ever reaches
// here, so this is defense in depth against the narrow window between
// that listing and this open: something that was a regular file when
// listed but has since been replaced (a still-running process the
// sandbox left behind, racing this walk) would otherwise be followed —
// or blocked on — anyway.
//
//   - O_NOFOLLOW makes the open fail with ELOOP if the final path
//     component is now a symlink, rather than silently opening whatever
//     it resolves to — which, unlike the file it replaced, could be
//     anything this worker process can read: its own config, cloud
//     credentials, a mounted service-account token.
//   - O_NONBLOCK stops the open call itself from blocking if the final
//     path component is now a FIFO with no writer on the other end.
//     Without it, open() on such a FIFO blocks before this function
//     even returns — well before the caller's later
//     f.Stat().Mode().IsRegular() check ever gets a chance to run and
//     reject it, so O_NOFOLLOW alone would not have closed this half of
//     the same race. It is deliberately never cleared afterward: POSIX
//     guarantees O_NONBLOCK has no effect on a regular file's
//     read/write behaviour, which is the only kind of file this ever
//     returns successfully — anything else is caught and rejected by
//     the caller's own fstat check.
func openRegularNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

// processAlive reports whether pid names a process that is currently
// running, using the standard POSIX existence probe: sending signal 0
// sends nothing but still fails with ESRCH if the process is gone.
//
// os.FindProcess itself cannot answer this on Unix — per its own
// documentation it "always succeeds and returns a Process for the
// given pid, regardless of whether the process exists" on this
// platform family — so the real check is the Signal(0) call, not the
// FindProcess call that precedes it.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	return proc.Signal(syscall.Signal(0)) == nil
}
