package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestDurableReads(t *testing.T) {
	dsn := os.Getenv("DISPATCH_READ_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_READ_REQUIRED") == "1" {
			t.Fatal("DISPATCH_READ_TEST_DSN required")
		}
		t.Skip("dedicated PostgreSQL fixture required")
	}
	s := openWakeStore(t, dsn)
	pg, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(context.Background())
	durabletest.RunReads(t, s, func(keys []durable.Key) {
		tx, err := pg.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err = tx.Exec(t.Context(), `ALTER TABLE dispatch_executions DISABLE TRIGGER USER`); err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if _, updateErr := tx.Exec(t.Context(), `UPDATE dispatch_executions SET created_at='2026-01-01T00:00:00Z',first_started_at='2026-01-01T00:00:00Z',run_available_at='2026-01-01T00:00:00Z' WHERE namespace=$1 AND workflow_id=$2 AND run_id=$3`, key.Namespace, key.WorkflowID, key.RunID); updateErr != nil {
				t.Fatal(updateErr)
			}
		}
		if _, err = tx.Exec(t.Context(), `SET CONSTRAINTS ALL IMMEDIATE; ALTER TABLE dispatch_executions ENABLE TRIGGER USER`); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}
