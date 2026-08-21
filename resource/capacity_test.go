package resource_test

import (
	"testing"

	"github.com/xraph/dispatch/resource"
)

func TestDetectExplicitWins(t *testing.T) {
	got := resource.Detect(resource.CapacityConfig{
		CPUOvercommit:  1.0,
		MemoryFraction: 0.8,
		Explicit:       resource.Set{resource.Memory: 42, "fpga": 2},
	})

	if got[resource.Memory] != 42 {
		t.Errorf("explicit memory = %d, want 42", got[resource.Memory])
	}
	if got["fpga"] != 2 {
		t.Errorf("custom key must be carried through, got %v", got)
	}
	if got[resource.CPU] == 0 {
		t.Error("cpu should still be autodetected when not explicit")
	}
}

func TestDetectAppliesOvercommitAndFraction(t *testing.T) {
	base := resource.Detect(resource.CapacityConfig{
		CPUOvercommit: 1.0, MemoryFraction: 1.0,
	})
	doubled := resource.Detect(resource.CapacityConfig{
		CPUOvercommit: 2.0, MemoryFraction: 0.5,
	})

	if doubled[resource.CPU] != base[resource.CPU]*2 {
		t.Errorf("cpu overcommit not applied: %d vs %d",
			doubled[resource.CPU], base[resource.CPU])
	}
	if doubled[resource.Memory] > base[resource.Memory]/2+1 {
		t.Errorf("memory fraction not applied: %d vs %d",
			doubled[resource.Memory], base[resource.Memory])
	}
}

func TestDefaultCapacityConfigIsConservative(t *testing.T) {
	cfg := resource.DefaultCapacityConfig()

	if cfg.CPUOvercommit != 1.0 {
		t.Errorf("CPUOvercommit = %v, want 1.0", cfg.CPUOvercommit)
	}
	if cfg.MemoryFraction >= 1.0 {
		t.Errorf("MemoryFraction = %v; must leave runtime and OS headroom",
			cfg.MemoryFraction)
	}
}

func TestDetectDiskFromConfig(t *testing.T) {
	got := resource.Detect(resource.CapacityConfig{
		CPUOvercommit: 1.0, MemoryFraction: 0.8, DiskBytes: 200 << 30,
	})
	if got[resource.Disk] != 200<<30 {
		t.Errorf("disk = %d, want %d", got[resource.Disk], int64(200)<<30)
	}
}

func TestDetectNeverReturnsZeroCPUOrMemory(t *testing.T) {
	got := resource.Detect(resource.CapacityConfig{})

	if got[resource.CPU] <= 0 {
		t.Errorf("cpu = %d; a zero-value config must still autodetect", got[resource.CPU])
	}
	if got[resource.Memory] <= 0 {
		t.Errorf("memory = %d; a zero-value config must still autodetect", got[resource.Memory])
	}
}
