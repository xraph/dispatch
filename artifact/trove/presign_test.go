package trove_test

import (
	"testing"

	"github.com/xraph/dispatch/artifact"
)

func TestTroveReportsCurrentPresignCapability(t *testing.T) {
	backend := newBackend(t)
	support, ok := backend.(interface{ SupportsPresign() bool })
	if !ok {
		t.Fatal("adapter does not expose the driver's signing capability")
	}
	if support.SupportsPresign() {
		t.Fatal("memory driver cannot sign URLs")
	}
	if _, ok := backend.(artifact.Presigner); !ok {
		t.Fatal("existing Presigner contract removed")
	}
}
