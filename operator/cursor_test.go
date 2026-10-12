package operator

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func TestCursorDerivedBudget(t *testing.T) {
	t.Logf("outer cursor bound: %d bytes, plaintext bound: %d bytes", maxCursorBytes, maxCursorJSON)
	version := strings.Repeat("<", durable.MaxDeliveryIdentifierBytes)
	codec, err := newCodec(CursorKeys{Active: version, Keys: map[string][]byte{version: make([]byte, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately exercise the conservative six-byte position budget as well
	// as the real inner base64 case. All 32 scope entries use full catalog IDs.
	state := cursorState{Position: strings.Repeat("<", durable.MaxReadCursorBytes), HighWater: math.MinInt64, Revision: math.MaxInt64}
	for i := 0; i < DiscoveryBudget; i++ {
		state.Scope = append(state.Scope, strings.Repeat("&", durable.MaxDeliveryIdentifierBytes))
	}
	bind := strings.Repeat("<", 64)
	token, err := codec.seal(state, bind)
	if err != nil || len(token) > maxCursorBytes {
		t.Fatal("seal", len(token), err)
	}
	opened, err := codec.open(token, bind)
	if err != nil || opened.Position != state.Position || !reflect.DeepEqual(opened.Scope, state.Scope) || opened.HighWater != state.HighWater || opened.Revision != state.Revision {
		t.Fatal("round trip", err)
	}
	if _, err = codec.open(token, "other"); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("binding lost", err)
	}
	if _, err = codec.open(strings.Repeat("A", maxCursorBytes+1), bind); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("oversized decoder input", err)
	}
	state.Position += "x"
	if token, err = codec.seal(state, bind); token != "" || !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("oversized position emitted", err)
	}
	state.Position = "position"
	state.Scope = append(state.Scope, "overflow")
	if token, err = codec.seal(state, bind); token != "" || !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("oversized scope emitted", err)
	}
	state.Scope = []string{strings.Repeat("x", durable.MaxDeliveryIdentifierBytes+1)}
	if token, err = codec.seal(state, bind); token != "" || !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("oversized namespace emitted", err)
	}
}
