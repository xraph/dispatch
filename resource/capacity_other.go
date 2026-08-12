//go:build !linux

package resource

// cgroupCPUMillis has no cgroup to read outside Linux.
func cgroupCPUMillis() (int64, bool) { return 0, false }

// cgroupMemoryBytes has no cgroup to read outside Linux.
func cgroupMemoryBytes() (int64, bool) { return 0, false }

// hostMemoryBytes is not implemented outside Linux, so Detect falls
// back to fallbackMemoryBytes. A non-Linux worker running heavy jobs
// should configure Explicit memory rather than rely on detection.
func hostMemoryBytes() (int64, bool) { return 0, false }
