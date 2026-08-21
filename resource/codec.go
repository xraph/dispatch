package resource

import (
	"encoding/json"
	"strings"
)

// CustomKeySep delimits the custom-resource key list produced by
// EncodeCustomKeys. The list is stored as a delimited string rather than
// an array so every store backend can express the containment test in
// its own idiom (a LIKE/GLOB pattern, a substring match, ...) without a
// schema translation.
const CustomKeySep = ","

// EncodeSet marshals a Set for a byte-oriented JSON column (e.g.
// Postgres JSONB). A zero Set — nil or every quantity zero — encodes to
// nil rather than "{}", so an undeclared job's row stays indistinguishable
// from one written before these columns existed: a genuine SQL NULL, not
// an empty JSON object.
func EncodeSet(s Set) ([]byte, error) {
	if s.IsZero() {
		return nil, nil
	}

	return json.Marshal(s)
}

// DecodeSet unmarshals a column produced by EncodeSet, treating both a
// NULL column (nil slice) and an empty one (zero-length slice) as
// "unset" rather than an error.
func DecodeSet(b []byte) (Set, error) {
	if len(b) == 0 {
		return nil, nil
	}

	var s Set
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}

	return s, nil
}

// EncodeSetString is EncodeSet for backends whose JSON column is a
// string-typed TEXT rather than a byte column — SQLite has no JSONB
// type, so its column is *string, where nil is what stores as SQL NULL.
// The encoding rules are identical to EncodeSet: a zero Set produces a
// nil pointer, never a pointer to "" or "{}".
func EncodeSetString(s Set) (*string, error) {
	b, err := EncodeSet(s)
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, nil
	}

	js := string(b)

	return &js, nil
}

// DecodeSetString is DecodeSet for the *string TEXT representation,
// treating both a NULL column (nil pointer) and an empty string as
// "unset".
func DecodeSetString(s *string) (Set, error) {
	if s == nil {
		return DecodeSet(nil)
	}

	return DecodeSet([]byte(*s))
}

// EncodeCustomKeys renders the non-canonical keys of s carrying a
// nonzero quantity as a delimited string with a leading and trailing
// separator, so a containment test can match on ",fpga," and never
// partially match ",fpga-large,". A Set with no custom keys — including
// a nil or zero Set — encodes to "".
func EncodeCustomKeys(s Set) string {
	keys := s.CustomKeys()
	if len(keys) == 0 {
		return ""
	}

	return CustomKeySep + strings.Join(keys, CustomKeySep) + CustomKeySep
}
