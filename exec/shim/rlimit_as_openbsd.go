//go:build openbsd

package shim

// rlimitAS reports RLIMIT_AS as unusable on OpenBSD. Go's syscall
// package exports RLIMIT_CORE, RLIMIT_CPU, RLIMIT_DATA, RLIMIT_FSIZE,
// RLIMIT_NOFILE, and RLIMIT_STACK for this platform (confirmed against
// its generated zerrors_openbsd_*.go tables) but not RLIMIT_AS — the
// symbol simply is not there, so referencing syscall.RLIMIT_AS the way
// rlimit_as_unix.go does for every other unix platform would fail to
// compile here. applyRlimits treats ok=false as "skip this one, warn if
// the operator asked for it" rather than guessing a raw resource number
// this package has not verified for OpenBSD.
func rlimitAS() (resource int, ok bool) {
	return 0, false
}
