//go:build !unix

package shim

// applyRlimits is a no-op outside Unix: RLIMIT_* and syscall.Setrlimit
// are POSIX concepts this rung does not emulate anywhere else. It is
// never reached in practice — exec/subprocess's checkLaunch refuses to
// launch the child at all on a non-Unix platform (limits_other.go, in
// that package) — but this stub still needs to exist so this package
// compiles there: main.go's call to applyRlimits is unconditional, since
// mainExitCode itself has no reason to know which platform it is running
// on. Returning no failures means EnvRlimitStrict never trips here
// either, which is moot in practice for the same reason.
func applyRlimits() []rlimitFailure { return nil }
