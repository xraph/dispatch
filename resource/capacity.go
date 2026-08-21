package resource

import (
	"math"
	"runtime"
)

// Default capacity tuning.
const (
	// DefaultCPUOvercommit is 1.0: no overcommit unless asked for.
	DefaultCPUOvercommit = 1.0
	// DefaultMemoryFraction leaves 20% for the Go runtime, the OS page
	// cache, and everything else sharing the box.
	DefaultMemoryFraction = 0.8
	// fallbackMemoryBytes is used when the host total cannot be read.
	// Deliberately small: under-advertising costs throughput, while
	// over-advertising costs the OOM cascade this package prevents.
	fallbackMemoryBytes = 2 << 30
)

// CapacityConfig controls how a worker's capacity is derived.
//
// There is no MemoryOvercommit. Overcommitting memory is how a box gets
// into the OOM cascade this package exists to prevent, and a knob whose
// only outcome is an incident should not exist.
type CapacityConfig struct {
	// CPUOvercommit multiplies detected cores. CPU is compressible:
	// exceeding it makes jobs slow, not dead, so overcommit is safe
	// where memory overcommit is not.
	CPUOvercommit float64
	// MemoryFraction is the share of the detected limit to advertise.
	MemoryFraction float64
	// DiskBytes is the staging cache budget. Zero omits the key.
	DiskBytes int64
	// Explicit overrides detection per key, and is the only way to
	// declare a custom resource.
	Explicit Set
}

// DefaultCapacityConfig returns the conservative defaults.
func DefaultCapacityConfig() CapacityConfig {
	return CapacityConfig{
		CPUOvercommit:  DefaultCPUOvercommit,
		MemoryFraction: DefaultMemoryFraction,
	}
}

// Detect derives a worker's capacity, with Explicit overriding any
// autodetected key.
func Detect(cfg CapacityConfig) Set {
	if cfg.CPUOvercommit <= 0 {
		cfg.CPUOvercommit = DefaultCPUOvercommit
	}

	if cfg.MemoryFraction <= 0 {
		cfg.MemoryFraction = DefaultMemoryFraction
	}

	out := Set{
		CPU:    int64(math.Floor(float64(detectCPUMillis()) * cfg.CPUOvercommit)),
		Memory: int64(math.Floor(float64(detectMemoryBytes()) * cfg.MemoryFraction)),
	}

	if cfg.DiskBytes > 0 {
		out[Disk] = cfg.DiskBytes
	}

	for k, v := range cfg.Explicit {
		out[k] = v
	}

	return out
}

// detectCPUMillis prefers the cgroup quota over the host core count.
//
// In a container with a two-core quota, runtime.NumCPU reports the
// host's 64, and every capacity derived from it is wrong by a factor of
// 32 — which on a resource-aware scheduler means admitting 32× the work
// the box can actually run.
func detectCPUMillis() int64 {
	if quota, ok := cgroupCPUMillis(); ok && quota > 0 {
		return quota
	}

	return int64(runtime.NumCPU()) * MilliScale
}

// detectMemoryBytes prefers the cgroup limit over host total, for the
// same reason.
func detectMemoryBytes() int64 {
	if limit, ok := cgroupMemoryBytes(); ok && limit > 0 {
		return limit
	}

	if total, ok := hostMemoryBytes(); ok && total > 0 {
		return total
	}

	return fallbackMemoryBytes
}
