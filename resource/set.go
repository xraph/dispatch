package resource

import (
	"math"
	"sort"
)

// Canonical resource keys. Any key outside this set is a custom
// resource: an integer quantity with user-defined semantics.
const (
	// CPU is measured in millicores. One core is 1000.
	CPU = "cpu"
	// Memory is measured in bytes.
	Memory = "memory"
	// Disk is measured in bytes. For a worker this is the staging cache
	// budget; for a job, the bytes its inputs and outputs need locally.
	Disk = "disk"
	// GPU is measured in milli-devices. One device is 1000, so a
	// fractional declaration is expressible. Kubernetes accepts only
	// whole devices, so track C rounds up at translation.
	GPU = "gpu"
)

// MilliScale is the multiplier for the milli-denominated keys.
const MilliScale = 1000

// Set is a resource vector. An absent key is zero, so a Set never needs
// to enumerate keys it does not constrain.
//
// Set is a map, so it is not safe for concurrent mutation. Every method
// here returns a new Set rather than mutating the receiver, which makes
// a Set stored on a job or a lease safe to read from many goroutines.
type Set map[string]int64

// CPUs builds a Set from a core count. CPUs(2.5) is 2500 millicores.
func CPUs(n float64) Set { return Set{CPU: milli(n)} }

// MemoryBytes builds a Set from a byte count.
func MemoryBytes(n int64) Set { return Set{Memory: n} }

// MemoryGB builds a Set from a gibibyte count.
func MemoryGB(n int64) Set { return Set{Memory: n << 30} }

// DiskBytes builds a Set from a byte count.
func DiskBytes(n int64) Set { return Set{Disk: n} }

// DiskGB builds a Set from a gibibyte count.
func DiskGB(n int64) Set { return Set{Disk: n << 30} }

// GPUs builds a Set from a device count. GPUs(0.5) is half a device.
func GPUs(n float64) Set { return Set{GPU: milli(n)} }

// Custom builds a Set for a single custom resource key.
func Custom(key string, n int64) Set { return Set{key: n} }

// milli converts a fractional count to its milli-denominated integer,
// rounding up so a request is never understated.
func milli(n float64) int64 {
	return int64(math.Ceil(n * MilliScale))
}

// Clone returns an independent copy.
func (s Set) Clone() Set {
	if s == nil {
		return nil
	}

	out := make(Set, len(s))
	for k, v := range s {
		out[k] = v
	}

	return out
}

// Add returns the per-key sum. Neither operand is modified.
func (s Set) Add(o Set) Set {
	out := s.Clone()
	if out == nil {
		out = make(Set, len(o))
	}

	for k, v := range o {
		out[k] += v
	}

	return out
}

// Sub returns the per-key difference, clamped at zero. A resource
// vector is never negative: "owing" capacity is not a state the
// accounting can represent, and clamping keeps a double release from
// corrupting the ledger.
func (s Set) Sub(o Set) Set {
	out := s.Clone()
	if out == nil {
		out = make(Set, len(o))
	}

	for k, v := range o {
		if out[k] -= v; out[k] < 0 {
			out[k] = 0
		}
	}

	return out
}

// Max returns the per-key maximum, used to merge a floor into a
// resolved requirement.
func (s Set) Max(o Set) Set {
	out := s.Clone()
	if out == nil {
		out = make(Set, len(o))
	}

	for k, v := range o {
		if v > out[k] {
			out[k] = v
		}
	}

	return out
}

// Scale multiplies every quantity by f, rounding up. Used for safety
// factors and OOM retry escalation, where rounding down would produce
// the same failure again.
func (s Set) Scale(f float64) Set {
	out := make(Set, len(s))
	for k, v := range s {
		out[k] = int64(math.Ceil(float64(v) * f))
	}

	return out
}

// Fits reports whether every quantity in s is within capacity. An
// absent capacity key is zero, so demanding a resource the capacity
// does not list never fits.
func (s Set) Fits(capacity Set) bool {
	for k, v := range s {
		if v > capacity[k] {
			return false
		}
	}

	return true
}

// Exceeds returns the keys on which s does not fit capacity, sorted.
// It is what turns a failed admission into an error naming the
// dimension that did not fit rather than a bare "does not fit".
func (s Set) Exceeds(capacity Set) []string {
	var over []string

	for k, v := range s {
		if v > capacity[k] {
			over = append(over, k)
		}
	}

	sort.Strings(over)

	return over
}

// Keys returns every key, sorted.
func (s Set) Keys() []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// CustomKeys returns the non-canonical keys carrying a nonzero
// quantity, sorted. This is the set persisted on the job row and
// matched by containment at dequeue.
func (s Set) CustomKeys() []string {
	var keys []string

	for k, v := range s {
		if v == 0 {
			continue
		}

		switch k {
		case CPU, Memory, Disk, GPU:
		default:
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	return keys
}

// IsZero reports whether every quantity is zero. A zero Set means "no
// declared requirement", which is how every job behaves before this
// feature is configured.
func (s Set) IsZero() bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}

	return true
}
