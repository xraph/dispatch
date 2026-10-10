package sqlite

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestBusyRetrySurvivesLongerWriterHold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		busy := errors.New("SQLITE_BUSY: held writer")
		err := withBusyRetry(t.Context(), func() error {
			if time.Since(start) < 300*time.Millisecond {
				return busy
			}
			return nil
		})
		if err != nil {
			t.Fatalf("writer released within retry window: %v", err)
		}
	})
}
func TestBusyRetryBudgetReturnsOriginalError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		busy := errors.New("SQLITE_BUSY: held writer")
		err := withBusyRetry(t.Context(), func() error { return busy })
		if err != busy || time.Since(start) != 5*time.Second { //nolint:errorlint // The retry budget must return the original error unchanged.
			t.Fatalf("retry budget/error changed: elapsed=%v error=%v", time.Since(start), err)
		}
	})
}
func TestBusyRetryEarlierCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		time.AfterFunc(50*time.Millisecond, cancel)
		start := time.Now()
		err := withBusyRetry(ctx, func() error { return errors.New("SQLITE_BUSY") })
		if !errors.Is(err, context.Canceled) || time.Since(start) != 50*time.Millisecond {
			t.Fatalf("cancellation lost: elapsed=%v error=%v", time.Since(start), err)
		}
	})
}
func TestBusyRetryNonBusyAndAlreadyCanceled(t *testing.T) {
	unexpected := errors.New("storage failed")
	calls := 0
	if err := withBusyRetry(t.Context(), func() error { calls++; return unexpected }); err != unexpected || calls != 1 { //nolint:errorlint // Non-busy errors must propagate unchanged.
		t.Fatal("non-busy error retried or changed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := withBusyRetry(ctx, func() error { t.Error("called database after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled: %v", err)
	}
}
func TestBusyRetryDoesNotRetrySynchronousOverrun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		busy := errors.New("SQLITE_BUSY")
		start := time.Now()
		err := withBusyRetry(t.Context(), func() error { calls++; time.Sleep(6 * time.Second); return busy })
		if err != busy || calls != 1 || time.Since(start) != 6*time.Second { //nolint:errorlint // Preserve the last database failure when retry time is exhausted.
			t.Fatalf("synchronous call overrun started another retry: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
		}
	})
}

func TestBusyRetryPreservesAcceptedSynchronousResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		calls := 0
		err := withBusyRetry(ctx, func() error { calls++; time.Sleep(6 * time.Second); return nil })
		if err != nil || calls != 1 {
			t.Fatalf("accepted database result overwritten: %v", err)
		}
	})
}
