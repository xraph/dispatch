package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sort"
)

// FingerprintOf derives a stable identifier for a handler set and the build
// that contains it.
//
// A sandbox verifies this before running anything. When the sandbox re-execs
// the worker's own binary the check always passes and costs one comparison;
// its purpose is the Policy.Image override, where a stale image would
// otherwise run an old handler and report success. Drift becomes an
// immediate, correctly-classified launch failure instead of a silent wrong
// answer.
func FingerprintOf(names []string, revision string) string {
	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)

	h := sha256.New()
	// Length-prefix every element. Joining on a separator would let a
	// handler named "a\nb" hash identically to the pair {"a", "b"}.
	fmt.Fprintf(h, "%d:%s\n", len(revision), revision)
	for _, n := range sorted {
		fmt.Fprintf(h, "%d:%s\n", len(n), n)
	}

	return hex.EncodeToString(h.Sum(nil))
}

// Fingerprint derives the identifier for a handler set using this binary's
// VCS revision. When the revision is unavailable — a build without VCS
// stamping — it falls back to the empty revision, so the fingerprint still
// covers the handler names.
func Fingerprint(names []string) string {
	return FingerprintOf(names, buildRevision())
}

// buildRevision returns the VCS revision this binary was built from.
func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}

	return ""
}
