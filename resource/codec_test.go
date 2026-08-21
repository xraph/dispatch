package resource

import "testing"

// TestEncodeCustomKeys pins the leading/trailing separator encoding that
// the dequeue predicate depends on. A bare join ("fpga" instead of
// ",fpga,") would let a containment match on "fpga" partially match
// "fpga-large" — that's the case the first subtest exists to catch. This
// is the single shared copy: store/postgres and store/sqlite used to
// each pin their own duplicate of this behavior; now both call
// EncodeCustomKeys directly, so one suite covers every backend.
func TestEncodeCustomKeys(t *testing.T) {
	tests := []struct {
		name string
		set  Set
		want string
	}{
		{
			name: "prefix collision case: fpga must not partially match fpga-large",
			set:  Set{"fpga": 2, "fpga-large": 1},
			want: ",fpga,fpga-large,",
		},
		{
			name: "canonical keys only encodes to empty string",
			set: Set{
				CPU:    4000,
				Memory: 16 << 30,
				Disk:   1 << 30,
				GPU:    1000,
			},
			want: "",
		},
		{
			name: "zero-quantity custom key is excluded",
			set:  Set{"fpga": 0, "gpu-slot": 3},
			want: ",gpu-slot,",
		},
		{
			name: "nil set encodes to empty string",
			set:  nil,
			want: "",
		},
		{
			name: "empty (zero) set encodes to empty string",
			set:  Set{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodeCustomKeys(tt.set)
			if got != tt.want {
				t.Errorf("EncodeCustomKeys(%v) = %q, want %q", tt.set, got, tt.want)
			}
		})
	}
}

// TestEncodeDecodeSetRoundTrip exercises EncodeSet/DecodeSet directly,
// without going through any store. The zero-Set-encodes-to-nil behavior
// is load-bearing: it is what makes an undeclared job indistinguishable
// from a row written before this migration (NULL, not "{}").
func TestEncodeDecodeSetRoundTrip(t *testing.T) {
	t.Run("zero set encodes to nil, not {}", func(t *testing.T) {
		b, err := EncodeSet(Set{})
		if err != nil {
			t.Fatalf("EncodeSet() error = %v", err)
		}
		if b != nil {
			t.Errorf("EncodeSet(zero Set) = %q, want nil", b)
		}
	})

	t.Run("nil set encodes to nil", func(t *testing.T) {
		b, err := EncodeSet(nil)
		if err != nil {
			t.Fatalf("EncodeSet() error = %v", err)
		}
		if b != nil {
			t.Errorf("EncodeSet(nil) = %q, want nil", b)
		}
	})

	t.Run("nonzero set survives encode then decode", func(t *testing.T) {
		want := Set{
			CPU:    4000,
			Memory: 16 << 30,
			"fpga": 2,
		}

		b, err := EncodeSet(want)
		if err != nil {
			t.Fatalf("EncodeSet() error = %v", err)
		}
		if b == nil {
			t.Fatal("EncodeSet(nonzero Set) = nil, want encoded bytes")
		}

		got, err := DecodeSet(b)
		if err != nil {
			t.Fatalf("DecodeSet() error = %v", err)
		}

		if len(got) != len(want) {
			t.Fatalf("DecodeSet() = %v, want %v", got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("DecodeSet()[%q] = %d, want %d", k, got[k], v)
			}
		}
	})

	t.Run("DecodeSet(nil) returns nil, not an error", func(t *testing.T) {
		got, err := DecodeSet(nil)
		if err != nil {
			t.Fatalf("DecodeSet(nil) error = %v", err)
		}
		if got != nil {
			t.Errorf("DecodeSet(nil) = %v, want nil", got)
		}
	})

	t.Run("DecodeSet(empty slice) returns nil, not an error", func(t *testing.T) {
		got, err := DecodeSet([]byte{})
		if err != nil {
			t.Fatalf("DecodeSet(empty) error = %v", err)
		}
		if got != nil {
			t.Errorf("DecodeSet(empty) = %v, want nil", got)
		}
	})

	t.Run("DecodeSet rejects malformed JSON", func(t *testing.T) {
		if _, err := DecodeSet([]byte("not json")); err == nil {
			t.Error("DecodeSet(malformed) error = nil, want non-nil")
		}
	})
}

// TestEncodeDecodeSetStringRoundTrip covers the *string TEXT-column
// representation (SQLite has no JSONB type) with the same rules as
// EncodeSet/DecodeSet, including that a zero Set still produces a nil
// pointer (SQL NULL), not a pointer to "" or "{}".
func TestEncodeDecodeSetStringRoundTrip(t *testing.T) {
	t.Run("zero set encodes to nil, not a pointer to {}", func(t *testing.T) {
		s, err := EncodeSetString(Set{})
		if err != nil {
			t.Fatalf("EncodeSetString() error = %v", err)
		}
		if s != nil {
			t.Errorf("EncodeSetString(zero Set) = %q, want nil", *s)
		}
	})

	t.Run("nil set encodes to nil", func(t *testing.T) {
		s, err := EncodeSetString(nil)
		if err != nil {
			t.Fatalf("EncodeSetString() error = %v", err)
		}
		if s != nil {
			t.Errorf("EncodeSetString(nil) = %q, want nil", *s)
		}
	})

	t.Run("nonzero set survives encode then decode", func(t *testing.T) {
		want := Set{
			CPU:    4000,
			Memory: 16 << 30,
			"fpga": 2,
		}

		s, err := EncodeSetString(want)
		if err != nil {
			t.Fatalf("EncodeSetString() error = %v", err)
		}
		if s == nil {
			t.Fatal("EncodeSetString(nonzero Set) = nil, want an encoded string")
		}

		got, err := DecodeSetString(s)
		if err != nil {
			t.Fatalf("DecodeSetString() error = %v", err)
		}

		if len(got) != len(want) {
			t.Fatalf("DecodeSetString() = %v, want %v", got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("DecodeSetString()[%q] = %d, want %d", k, got[k], v)
			}
		}
	})

	t.Run("DecodeSetString(nil) returns nil, not an error", func(t *testing.T) {
		got, err := DecodeSetString(nil)
		if err != nil {
			t.Fatalf("DecodeSetString(nil) error = %v", err)
		}
		if got != nil {
			t.Errorf("DecodeSetString(nil) = %v, want nil", got)
		}
	})

	t.Run("DecodeSetString(empty string) returns nil, not an error", func(t *testing.T) {
		empty := ""
		got, err := DecodeSetString(&empty)
		if err != nil {
			t.Fatalf("DecodeSetString(empty) error = %v", err)
		}
		if got != nil {
			t.Errorf("DecodeSetString(empty) = %v, want nil", got)
		}
	})

	t.Run("DecodeSetString rejects malformed JSON", func(t *testing.T) {
		bad := "not json"
		if _, err := DecodeSetString(&bad); err == nil {
			t.Error("DecodeSetString(malformed) error = nil, want non-nil")
		}
	})

	t.Run("EncodeSet and EncodeSetString agree byte-for-byte", func(t *testing.T) {
		set := Set{CPU: 2000, "fpga": 1}

		b, err := EncodeSet(set)
		if err != nil {
			t.Fatalf("EncodeSet() error = %v", err)
		}

		s, err := EncodeSetString(set)
		if err != nil {
			t.Fatalf("EncodeSetString() error = %v", err)
		}
		if s == nil {
			t.Fatal("EncodeSetString() = nil, want an encoded string")
		}

		if string(b) != *s {
			t.Errorf("EncodeSet() = %q, EncodeSetString() = %q, want identical JSON", b, *s)
		}
	})
}
