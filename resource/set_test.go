package resource_test

import (
	"testing"

	"github.com/xraph/dispatch/resource"
)

func TestSetArithmetic(t *testing.T) {
	tests := []struct {
		name string
		op   func() resource.Set
		want resource.Set
	}{
		{
			name: "add merges disjoint keys",
			op:   func() resource.Set { return resource.CPUs(2).Add(resource.MemoryGB(4)) },
			want: resource.Set{resource.CPU: 2000, resource.Memory: 4 << 30},
		},
		{
			name: "add sums shared keys",
			op:   func() resource.Set { return resource.CPUs(2).Add(resource.CPUs(1.5)) },
			want: resource.Set{resource.CPU: 3500},
		},
		{
			name: "sub clamps at zero",
			op:   func() resource.Set { return resource.CPUs(1).Sub(resource.CPUs(4)) },
			want: resource.Set{resource.CPU: 0},
		},
		{
			name: "max takes the larger per key",
			op: func() resource.Set {
				return resource.Set{resource.CPU: 1000, resource.Memory: 100}.
					Max(resource.Set{resource.CPU: 500, resource.Memory: 900})
			},
			want: resource.Set{resource.CPU: 1000, resource.Memory: 900},
		},
		{
			name: "scale rounds up so a request is never understated",
			op:   func() resource.Set { return resource.Set{resource.Memory: 3}.Scale(1.5) },
			want: resource.Set{resource.Memory: 5},
		},
		{
			name: "custom keys participate in arithmetic",
			op:   func() resource.Set { return resource.Custom("fpga", 1).Add(resource.Custom("fpga", 2)) },
			want: resource.Set{"fpga": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.op()
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("key %q: got %d, want %d", k, got[k], v)
				}
			}
		})
	}
}

func TestSetFits(t *testing.T) {
	tests := []struct {
		name     string
		want     resource.Set
		capacity resource.Set
		fits     bool
	}{
		{
			name:     "empty set fits anything",
			want:     resource.Set{},
			capacity: resource.Set{resource.CPU: 1},
			fits:     true,
		},
		{
			name:     "exact fit",
			want:     resource.Set{resource.Memory: 100},
			capacity: resource.Set{resource.Memory: 100},
			fits:     true,
		},
		{
			name:     "over on one key fails",
			want:     resource.Set{resource.CPU: 1, resource.Memory: 101},
			capacity: resource.Set{resource.CPU: 8, resource.Memory: 100},
			fits:     false,
		},
		{
			name:     "absent capacity key is zero, so any demand fails",
			want:     resource.Set{"fpga": 1},
			capacity: resource.Set{resource.CPU: 8},
			fits:     false,
		},
		{
			name:     "zero demand on an absent key still fits",
			want:     resource.Set{"fpga": 0},
			capacity: resource.Set{resource.CPU: 8},
			fits:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.want.Fits(tt.capacity); got != tt.fits {
				t.Errorf("Fits() = %v, want %v", got, tt.fits)
			}
		})
	}
}

func TestSetCustomKeysSorted(t *testing.T) {
	s := resource.Set{
		resource.CPU: 1000, "zebra": 1, resource.Memory: 2, "alpha": 3,
	}
	got := s.CustomKeys()
	want := []string{"alpha", "zebra"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestSetImmutability(t *testing.T) {
	a := resource.CPUs(1)
	b := a.Add(resource.CPUs(1))
	if a[resource.CPU] != 1000 {
		t.Errorf("Add mutated the receiver: %v", a)
	}
	if b[resource.CPU] != 2000 {
		t.Errorf("Add returned %v", b)
	}
}
