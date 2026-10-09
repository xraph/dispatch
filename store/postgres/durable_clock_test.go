//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/store/postgres"
)

// Wait against the same database clock used to grant and expire work. Docker's
// clock can differ from the host clock used by time.Until, even on one machine.
func waitDurableStoreTime(t *testing.T, s *postgres.Store, at time.Time) {
	t.Helper()
	if at.IsZero() {
		t.Fatal("cannot wait for a missing store timestamp")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pg := pgdriver.Unwrap(s.DB())
	for {
		var reached bool
		if err := pg.QueryRow(ctx, `SELECT clock_timestamp() >= $1`, at).Scan(&reached); err != nil {
			t.Fatal(err)
		}
		if reached {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("database clock did not reach %s: %v", at, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
