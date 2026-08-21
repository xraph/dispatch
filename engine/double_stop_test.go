package engine_test

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/engine"
)

// TestEngine_StopTwiceClosesExecutorsOnce hardens against a regression: Stop
// used to have no guard against being called twice, which was harmless only
// because closeExecutors had no caller at all. Now that Stop closes every
// registered executor, a second Stop call must not close them again — an
// out-of-process rung's Close releases real clients and child processes,
// and closing those twice is not something any rung is required to
// tolerate the way a no-op double-Stop is.
func TestEngine_StopTwiceClosesExecutorsOnce(t *testing.T) {
	rung := &countingExecutor{}
	eng, _ := startEngine(t, engine.WithExecutor(rung))

	if _, closed := rung.counts(); closed != 0 {
		t.Fatalf("closed count before any Stop = %d, want 0", closed)
	}

	if err := eng.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := eng.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	if _, closed := rung.counts(); closed != 1 {
		t.Errorf("Close called %d times across two Stop calls, want 1", closed)
	}
}
