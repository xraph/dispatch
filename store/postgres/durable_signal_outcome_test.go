package postgres_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
)

func TestDurableSignalStartOutcome(t *testing.T) {
	dsn := os.Getenv("DISPATCH_READ_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_READ_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL fixture required")
		}
		t.Skip("dedicated PostgreSQL fixture required")
	}
	s := openWakeStore(t, dsn)
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			r := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "workflow", RunID: "proposed"}, RequestID: "signal-start", WorkflowType: "workflow", BuildID: "build", Queue: "queue"}, Name: "signal"}
			if existing {
				start := r.Start
				start.RunID = "original"
				start.RequestID = "start"
				if _, err := s.StartExecution(t.Context(), start); err != nil {
					t.Fatal(err)
				}
			}
			var fresh, recovered atomic.Int64
			var group sync.WaitGroup
			for range 6 {
				group.Go(func() {
					outcome, err := s.SignalWithStartOutcome(t.Context(), r)
					if err != nil {
						t.Error(err)
						return
					}
					if outcome.Recovered {
						recovered.Add(1)
					} else {
						fresh.Add(1)
					}
					if outcome.Receipt.Started == existing {
						t.Error("incorrect branch")
					}
				})
			}
			group.Wait()
			if fresh.Load() != 1 || recovered.Load() != 5 {
				t.Fatalf("fresh %d recovered %d", fresh.Load(), recovered.Load())
			}
			// A separately opened connection observes the same durable recovery fact.
			reopened := openWakeStore(t, dsn)
			outcome, err := reopened.SignalWithStartOutcome(t.Context(), r)
			if err != nil || !outcome.Recovered {
				t.Fatalf("reopen: %+v %v", outcome, err)
			}
		})
	}
}

func TestDurableSignalStartOutcomeRequiredOutboxRollback(t *testing.T) {
	dsn := os.Getenv("DISPATCH_READ_TEST_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_READ_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL fixture required")
		}
		t.Skip("dedicated PostgreSQL fixture required")
	}
	s := openWakeStore(t, dsn)
	pg := pgdriver.Unwrap(s.DB())
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			ctx := t.Context()
			config := durable.NamespaceConfig{InstallationID: "host", Namespace: t.Name(), AppID: "app", TenantID: "tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}
			if _, err := s.RegisterNamespace(ctx, config); err != nil {
				t.Fatal(err)
			}
			request := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: config.Namespace, WorkflowID: "workflow", RunID: "run"}, RequestID: "signal-start", WorkflowType: "workflow", BuildID: "build", Queue: "queue"}, Name: "signal"}
			if existing {
				start := request.Start
				start.RequestID = "start"
				if _, err := s.StartExecution(ctx, start); err != nil {
					t.Fatal(err)
				}
			}
			tables := []string{"dispatch_executions", "dispatch_execution_events", "dispatch_execution_tasks", "dispatch_execution_receipts", "dispatch_signal_receipts", "dispatch_durable_outbox"}
			counts := func() []int {
				out := make([]int, len(tables))
				for i, table := range tables {
					if err := pg.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE namespace=$1`, config.Namespace).Scan(&out[i]); err != nil {
						t.Fatal(err)
					}
				}
				return out
			}
			before := counts()
			if _, err := pg.Exec(ctx, `CREATE FUNCTION dispatch_task3_reject_signal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.destination='chronicle' AND NEW.source_kind='signal_receipt' THEN RAISE EXCEPTION 'injected signal receipt failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER task3_reject_signal BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION dispatch_task3_reject_signal()`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := pg.Exec(context.Background(), `DROP TRIGGER IF EXISTS task3_reject_signal ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS dispatch_task3_reject_signal()`); err != nil {
					t.Error(err)
				}
			})
			if _, err := s.SignalWithStartOutcome(ctx, request); err == nil {
				t.Fatal("required outbox failure accepted")
			}
			if after := counts(); !slices.Equal(before, after) {
				t.Fatalf("rollback counts before=%v after=%v", before, after)
			}
			if existing {
				execution, err := s.GetExecution(ctx, request.Start.Key)
				if err != nil || execution.Revision != 1 || execution.LastSequence != 1 {
					t.Fatalf("existing execution mutated: %+v %v", execution, err)
				}
			} else if _, err := s.GetExecution(ctx, request.Start.Key); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("new execution persisted: %v", err)
			}
			if _, err := pg.Exec(ctx, `DROP TRIGGER task3_reject_signal ON dispatch_durable_outbox; DROP FUNCTION dispatch_task3_reject_signal()`); err != nil {
				t.Fatal(err)
			}
			outcome, err := s.SignalWithStartOutcome(ctx, request)
			if err != nil || outcome.Recovered || outcome.Receipt.Started == existing {
				t.Fatalf("fresh retry after rollback: %+v %v", outcome, err)
			}
			replay, err := s.SignalWithStartOutcome(ctx, request)
			if err != nil || !replay.Recovered || replay.Receipt != outcome.Receipt {
				t.Fatalf("accepted replay: %+v %v", replay, err)
			}
			mismatch := request
			mismatch.Start.RequestID = "build-mismatch"
			mismatch.Start.BuildID = "other-build"
			if _, err := s.SignalWithStartOutcome(ctx, mismatch); !errors.Is(err, durable.ErrBuildMismatch) || !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("atomic build mismatch compatibility: %v", err)
			}
		})
	}
}
