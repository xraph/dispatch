//go:build unix

package worker

import (
	"os"
	"syscall"
)

// openRegularNoFollow opens path for reading, refusing to follow a
// symlink at the final path component.
//
// collectOutputEntries already filters out a symlink dirent by its
// Lstat-reported type before any path derived from it ever reaches
// here, so this is defense in depth against the narrow window between
// that listing and this open: something that was a regular file when
// listed but has since been replaced with a symlink (a still-running
// process the sandbox left behind, racing this walk) would otherwise be
// followed anyway. O_NOFOLLOW makes the open itself fail with ELOOP in
// that case rather than silently opening whatever the symlink resolves
// to — which, unlike the file it replaced, could be anything this
// worker process can read: its own config, cloud credentials, a mounted
// service-account token.
func openRegularNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}
