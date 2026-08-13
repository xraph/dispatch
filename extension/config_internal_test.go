package extension

import (
	"reflect"
	"testing"

	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/resource"
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

// TestStagingBudgetRouting pins where the cache budget ends up.
//
// cache.WithBudget is ignored once a manager is supplied, so if the
// configured number does not arrive here it does not arrive anywhere, and
// an operator who wrote a budget gets whatever Detect chose instead.
func TestStagingBudgetRouting(t *testing.T) {
	cases := []struct {
		name     string
		artifact ArtifactConfig
		want     int64
	}{
		{
			name: "no artifact plane omits disk entirely",
			want: 0,
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

			if got := e.stagingBudget(); got != tc.want {
				t.Errorf("stagingBudget() = %d, want %d", got, tc.want)
			}
		})
	}
}
