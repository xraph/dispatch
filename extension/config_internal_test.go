package extension

import (
	"reflect"
	"testing"

	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

// TestResourceConfigYAMLShape pins the keys an operator writes. They are
// the published interface — a typo in a struct tag silently produces a
// worker running on detected defaults while the config file says
// otherwise, which is the failure mode this whole task is about.
func TestResourceConfigYAMLShape(t *testing.T) {
	want := map[string]string{
		"Enabled":        "enabled",
		"CPUOvercommit":  "cpu_overcommit",
		"MemoryFraction": "memory_fraction",
		"Explicit":       "explicit",
		"CustomKeys":     "custom_keys",
	}

	rt := reflect.TypeOf(ResourceConfig{})

	for i := range rt.NumField() {
		f := rt.Field(i)

		key, known := want[f.Name]
		if !known {
			t.Errorf("field %s has no expected config key; update this test", f.Name)

			continue
		}

		for _, tag := range []string{"yaml", "mapstructure", "json"} {
			if got := f.Tag.Get(tag); got != key {
				t.Errorf("%s: %s tag = %q, want %q", f.Name, tag, got, key)
			}
		}
	}

	if got := reflect.TypeOf(Config{}).Field(fieldIndex(t, Config{}, "Resources")).Tag.Get("yaml"); got != "resources" {
		t.Errorf("Config.Resources yaml tag = %q, want %q", got, "resources")
	}
}

func fieldIndex(t *testing.T, v any, name string) int {
	t.Helper()

	f, ok := reflect.TypeOf(v).FieldByName(name)
	if !ok {
		t.Fatalf("no field %q", name)
	}

	return f.Index[0]
}

// TestMergeResourceConfig covers the precedence rules: YAML wins where it
// spoke, programmatic options fill the gaps, and enabling is an OR so a
// binary built with WithResources cannot be silently switched off by a
// config file that simply does not mention it.
func TestMergeResourceConfig(t *testing.T) {
	t.Run("programmatic enable survives silent yaml", func(t *testing.T) {
		got := mergeResourceConfig(ResourceConfig{}, ResourceConfig{Enabled: true})
		if !got.Enabled {
			t.Error("WithResources was dropped by a config file that said nothing")
		}
	})

	t.Run("yaml wins on scalars", func(t *testing.T) {
		got := mergeResourceConfig(
			ResourceConfig{CPUOvercommit: 2, MemoryFraction: 0.5},
			ResourceConfig{CPUOvercommit: 4, MemoryFraction: 0.9},
		)

		if got.CPUOvercommit != 2 || got.MemoryFraction != 0.5 {
			t.Errorf("programmatic values overrode yaml: %+v", got)
		}
	})

	t.Run("explicit capacity merges per key", func(t *testing.T) {
		got := mergeResourceConfig(
			ResourceConfig{Explicit: resource.Set{resource.Memory: 1 << 30}},
			ResourceConfig{Explicit: resource.Set{"fpga": 2, resource.Memory: 8 << 30}},
		)

		// The binary knows how many FPGAs it was built to talk to; the
		// operator knows how much memory to hand this pod. Neither erases
		// the other, and on a key they both set, the file wins.
		if got.Explicit["fpga"] != 2 {
			t.Errorf("programmatic custom key lost: %v", got.Explicit)
		}

		if got.Explicit[resource.Memory] != 1<<30 {
			t.Errorf("yaml did not win on memory: %v", got.Explicit)
		}
	})
}

// TestWithWorkerCustomKeysMergesAndCopies keeps the option consistent
// with WithExplicitCapacity beside it and engine.WithWorkerCustomKeys
// below it: keys accumulate, duplicates collapse, and the caller's slice
// is not retained.
func TestWithWorkerCustomKeysMergesAndCopies(t *testing.T) {
	caller := []string{"fpga", "tpu"}

	e := New(
		WithWorkerCustomKeys(caller...),
		WithWorkerCustomKeys("fpga", "npu"),
	)

	want := []string{"fpga", "tpu", "npu"}

	got := e.config.Resources.CustomKeys
	if len(got) != len(want) {
		t.Fatalf("CustomKeys = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("CustomKeys = %v, want %v", got, want)
		}
	}

	// The caller keeps no handle on extension state.
	caller[0] = "mutated"

	if e.config.Resources.CustomKeys[0] != "fpga" {
		t.Errorf("the caller's slice is aliased: %v", e.config.Resources.CustomKeys)
	}
}

// TestStagingBudgetRouting pins where the cache budget ends up.
//
// cache.WithBudget is ignored once a manager is supplied, so if the
// configured number does not arrive here it does not arrive anywhere, and
// an operator who wrote a budget gets whatever Detect chose instead.
func TestStagingBudgetRouting(t *testing.T) {
	cases := []struct {
		name     string
		artifact ArtifactConfig
		noStore  bool
		want     int64
	}{
		{
			name: "artifacts off omits disk entirely",
			want: 0,
		},
		{
			// init only builds the plane when the dispatcher's store
			// implements artifact.Store. Configured-but-not-built has to
			// omit disk too, or the ledger advertises 20 GiB with no cache
			// behind it and no reclaimer registered for it.
			name:     "artifacts configured but no artifact store",
			artifact: ArtifactConfig{Enabled: true},
			noStore:  true,
			want:     0,
		},
		{
			name:     "configured budget",
			artifact: ArtifactConfig{Enabled: true, Cache: ArtifactCacheConfig{Budget: 200 << 30}},
			want:     200 << 30,
		},
		{
			name:     "unset budget falls back to the cache default",
			artifact: ArtifactConfig{Enabled: true},
			want:     cache.DefaultBudget,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Extension{config: Config{Artifacts: tc.artifact}}
			if !tc.noStore {
				e.artifactStore = memory.New()
			}

			if got := e.stagingBudget(); got != tc.want {
				t.Errorf("stagingBudget() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestDiskOverrideWarning pins when the operator gets told that two
// config keys are setting the same number.
//
// resources.explicit.disk is not just what the worker advertises: the
// cache reads its eviction ceiling off the ledger's disk capacity, so an
// explicit value is what the cache WRITES against. Above the volume that
// is ENOSPC, and before this it could only be reached by the cache budget
// key that names itself.
func TestDiskOverrideWarning(t *testing.T) {
	cases := []struct {
		name     string
		explicit resource.Set
		staging  int64
		want     bool
	}{
		{
			name:     "both set and disagreeing",
			explicit: resource.Set{resource.Disk: 500 << 30},
			staging:  200 << 30,
			want:     true,
		},
		{
			name:     "both set and agreeing",
			explicit: resource.Set{resource.Disk: 200 << 30},
			staging:  200 << 30,
		},
		{
			name:    "only the cache budget",
			staging: 200 << 30,
		},
		{
			// No staging cache to disagree with: explicit disk is then the
			// only source there is, which is not an override of anything.
			name:     "only explicit, no cache",
			explicit: resource.Set{resource.Disk: 500 << 30},
		},
		{
			name:     "an unrelated explicit key",
			explicit: resource.Set{resource.Memory: 8 << 30},
			staging:  200 << 30,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, conflict := diskOverride(tc.explicit, tc.staging)

			if conflict != tc.want {
				t.Fatalf("diskOverride() conflict = %v, want %v", conflict, tc.want)
			}

			if conflict && got != tc.explicit[resource.Disk] {
				t.Errorf("reported explicit disk = %d, want %d",
					got, tc.explicit[resource.Disk])
			}
		})
	}
}
