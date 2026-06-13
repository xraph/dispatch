package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate" // registers the pg migrate executor

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

// startWakePostgres launches a disposable postgres container and returns
// its DSN. Skips when -short is set or no container runtime is available.
func startWakePostgres(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("dispatch"),
		tcpostgres.WithUsername("dispatch"),
		tcpostgres.WithPassword("dispatch"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(ctr); termErr != nil {
			t.Errorf("terminate postgres container: %v", termErr)
		}
	})

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

func openWakeStore(t *testing.T, dsn string) *pgstore.Store {
	t.Helper()
	ctx := context.Background()

	drv := pgdriver.New()
	if err := drv.Open(ctx, dsn); err != nil {
		t.Fatalf("open pgdriver: %v", err)
	}
	db, err := grove.Open(drv)
	if err != nil {
		t.Fatalf("grove open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := pgstore.New(db)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// TestWakeListenerFiresOnEnqueueJob proves the LISTEN/NOTIFY path: a wake
// listener started on one store connection fires when a job is enqueued
// through another connection (i.e. another app instance), without waiting
// for any poll interval.
func TestWakeListenerFiresOnEnqueueJob(t *testing.T) {
	dsn := startWakePostgres(t)
	listenerStore := openWakeStore(t, dsn)
	enqueuerStore := openWakeStore(t, dsn)
	ctx := context.Background()

	woke := make(chan struct{}, 8)
	stop, err := listenerStore.StartWakeListener(ctx, func() {
		select {
		case woke <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("start wake listener: %v", err)
	}
	t.Cleanup(stop)

	// Let the LISTEN subscription establish before the NOTIFY fires.
	time.Sleep(300 * time.Millisecond)

	now := time.Now().UTC()
	j := &job.Job{
		Entity:     dispatch.NewEntity(),
		ID:         id.NewJobID(),
		Name:       "wake-me",
		Queue:      "default",
		Payload:    []byte(`{}`),
		State:      job.StatePending,
		MaxRetries: 3,
		RunAt:      now,
	}
	if err := enqueuerStore.EnqueueJob(ctx, j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("wake listener did not fire after cross-connection enqueue")
	}
}

// TestWakeListenerStopReleasesConn verifies stop returns and releases the
// dedicated connection (the deferred db.Close in openWakeStore hangs if it
// never comes back to the pool).
func TestWakeListenerStopReleasesConn(t *testing.T) {
	dsn := startWakePostgres(t)
	s := openWakeStore(t, dsn)

	stop, err := s.StartWakeListener(context.Background(), func() {})
	if err != nil {
		t.Fatalf("start wake listener: %v", err)
	}

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return")
	}
}
