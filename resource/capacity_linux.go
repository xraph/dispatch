//go:build linux

package resource

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
)

const (
	cgroupCPUMaxPath    = "/sys/fs/cgroup/cpu.max"
	cgroupMemoryMaxPath = "/sys/fs/cgroup/memory.max"
	procMemInfoPath     = "/proc/meminfo"
)

// cgroupCPUMillis reads cgroup v2 cpu.max, formatted "<quota> <period>"
// where quota may be the literal "max" for unlimited.
func cgroupCPUMillis() (int64, bool) {
	data, err := os.ReadFile(cgroupCPUMaxPath)
	if err != nil {
		return 0, false
	}

	fields := strings.Fields(string(data))
	if len(fields) != 2 || fields[0] == "max" {
		return 0, false
	}

	quota, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, false
	}

	period, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || period <= 0 {
		return 0, false
	}

	return int64(math.Floor(float64(quota) / float64(period) * MilliScale)), true
}

// cgroupMemoryBytes reads cgroup v2 memory.max, "max" when unlimited.
func cgroupMemoryBytes() (int64, bool) {
	data, err := os.ReadFile(cgroupMemoryMaxPath)
	if err != nil {
		return 0, false
	}

	text := strings.TrimSpace(string(data))
	if text == "max" {
		return 0, false
	}

	limit, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}

	return limit, true
}

// hostMemoryBytes reads MemTotal from /proc/meminfo, reported in kB.
func hostMemoryBytes() (int64, bool) {
	f, err := os.Open(procMemInfoPath)
	if err != nil {
		return 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "MemTotal:" {
			continue
		}

		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}

		return kb * 1024, true
	}

	return 0, false
}
