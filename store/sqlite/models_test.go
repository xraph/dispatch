package sqlite

import (
	"testing"

	"github.com/xraph/dispatch/resource"
)

// TestEncodeCustomKeys pins the leading/trailing separator encoding that a
// later dequeue predicate depends on. A bare join ("fpga" instead of
// ",fpga,") would let a containment match on "fpga" partially match
// "fpga-large" -- this is the case the first subtest exists to catch.
//
// This is store/postgres's TestEncodeCustomKeys, duplicated because the
// two packages duplicate the function under test; see the comment above
// CustomKeySep in models.go for why and how drift is meant to be caught.
func TestEncodeCustomKeys(t *testing.T) {
	tests := []struct {
		name string
		set  resource.Set
		want string
	}{
		{
			name: "prefix collision case: fpga must not partially match fpga-large",
			set:  resource.Set{"fpga": 2, "fpga-large": 1},
			want: ",fpga,fpga-large,",
		},
		{
			name: "canonical keys only encodes to empty string",
			set: resource.Set{
				resource.CPU:    4000,
				resource.Memory: 16 << 30,
				resource.Disk:   1 << 30,
				resource.GPU:    1000,
			},
			want: "",
		},
		{
			name: "zero-quantity custom key is excluded",
			set:  resource.Set{"fpga": 0, "gpu-slot": 3},
			want: ",gpu-slot,",
		},
		{
			name: "nil set encodes to empty string",
			set:  nil,
			want: "",
		},
		{
			name: "empty (zero) set encodes to empty string",
			set:  resource.Set{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeCustomKeys(tt.set)
			if got != tt.want {
				t.Errorf("encodeCustomKeys(%v) = %q, want %q", tt.set, got, tt.want)
			}
		})
	}
}

// TestEncodeDecodeSetRoundTrip exercises encodeSet/decodeSet directly,
// without going through the full store round-trip. The zero-Set-encodes-
// to-nil behavior is load-bearing: it is what makes an undeclared job
// indistinguishable from a row written before this migration (NULL, not
// "{}"). Unlike store/postgres's version, encodeSet/decodeSet here work on
// *string rather than []byte, since the column is TEXT, not JSONB.
func TestEncodeDecodeSetRoundTrip(t *testing.T) {
	t.Run("zero set encodes to nil, not {}", func(t *testing.T) {
		s, err := encodeSet(resource.Set{})
		if err != nil {
			t.Fatalf("encodeSet() error = %v", err)
		}
		if s != nil {
			t.Errorf("encodeSet(zero Set) = %v, want nil", *s)
		}
	})

	t.Run("nil set encodes to nil", func(t *testing.T) {
		s, err := encodeSet(nil)
		if err != nil {
			t.Fatalf("encodeSet() error = %v", err)
		}
		if s != nil {
			t.Errorf("encodeSet(nil) = %v, want nil", *s)
		}
	})

	t.Run("nonzero set survives encode then decode", func(t *testing.T) {
		want := resource.Set{
			resource.CPU:    4000,
			resource.Memory: 16 << 30,
			"fpga":          2,
		}

		s, err := encodeSet(want)
		if err != nil {
			t.Fatalf("encodeSet() error = %v", err)
		}
		if s == nil {
			t.Fatal("encodeSet(nonzero Set) = nil, want an encoded string")
		}

		got, err := decodeSet(s)
		if err != nil {
			t.Fatalf("decodeSet() error = %v", err)
		}

		if len(got) != len(want) {
			t.Fatalf("decodeSet() = %v, want %v", got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("decodeSet()[%q] = %d, want %d", k, got[k], v)
			}
		}
	})

	t.Run("decodeSet(nil) returns nil, not an error", func(t *testing.T) {
		got, err := decodeSet(nil)
		if err != nil {
			t.Fatalf("decodeSet(nil) error = %v", err)
		}
		if got != nil {
			t.Errorf("decodeSet(nil) = %v, want nil", got)
		}
	})

	t.Run("decodeSet(empty string) returns nil, not an error", func(t *testing.T) {
		empty := ""
		got, err := decodeSet(&empty)
		if err != nil {
			t.Fatalf("decodeSet(empty) error = %v", err)
		}
		if got != nil {
			t.Errorf("decodeSet(empty) = %v, want nil", got)
		}
	})

	t.Run("decodeSet rejects malformed JSON", func(t *testing.T) {
		bad := "not json"
		if _, err := decodeSet(&bad); err == nil {
			t.Error("decodeSet(malformed) error = nil, want non-nil")
		}
	})
}
