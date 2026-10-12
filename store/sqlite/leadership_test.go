package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/id"
)

func TestLeadershipAcquisitionUsesModelScan(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()
	first, second := id.NewWorkerID(), id.NewWorkerID()
	for _, workerID := range []id.WorkerID{first, second} {
		if err := s.RegisterWorker(ctx, &cluster.Worker{ID: workerID, State: cluster.WorkerActive, Queues: []string{"default"}, LastSeen: time.Now(), CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := s.AcquireLeadership(ctx, first, time.Minute); err != nil || !ok {
		t.Fatalf("first claim=%v, %v", ok, err)
	}
	if ok, err := s.AcquireLeadership(ctx, first, time.Minute); err != nil || !ok {
		t.Fatalf("same owner=%v, %v", ok, err)
	}
	if ok, err := s.AcquireLeadership(ctx, second, time.Minute); err != nil || ok {
		t.Fatalf("competitor=%v, %v", ok, err)
	}
	leader, err := s.GetLeader(ctx)
	if err != nil || leader == nil || leader.ID != first {
		t.Fatalf("leader=%+v, %v", leader, err)
	}
	if ok, renewErr := s.RenewLeadership(ctx, first, -time.Second); renewErr != nil || !ok {
		t.Fatalf("expire=%v, %v", ok, renewErr)
	}
	if expired, readErr := s.GetLeader(ctx); readErr != nil || expired != nil {
		t.Fatalf("expired leader=%+v, %v", expired, readErr)
	}
	if ok, claimErr := s.AcquireLeadership(ctx, second, time.Minute); claimErr != nil || !ok {
		t.Fatalf("replacement=%v, %v", ok, claimErr)
	}
	leader, err = s.GetLeader(ctx)
	if err != nil || leader == nil || leader.ID != second {
		t.Fatalf("new leader=%+v, %v", leader, err)
	}
}

func TestLeadershipConcurrentContendersHaveOneWinner(t *testing.T) {
	t.Run("empty", func(t *testing.T) { runConcurrentLeadership(t, false) })
	t.Run("expired", func(t *testing.T) { runConcurrentLeadership(t, true) })
}

func runConcurrentLeadership(t *testing.T, expired bool) {
	t.Helper()
	s := openSqliteStore(t)
	ctx := context.Background()
	const contenders = 24
	ids := make([]id.WorkerID, contenders)
	for i := range ids {
		ids[i] = id.NewWorkerID()
		if err := s.RegisterWorker(ctx, &cluster.Worker{ID: ids[i], State: cluster.WorkerActive, Queues: []string{"default"}, LastSeen: time.Now(), CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if expired {
		until := time.Now().Add(-time.Minute)
		if err := s.RegisterWorker(ctx, &cluster.Worker{ID: id.NewWorkerID(), State: cluster.WorkerActive, IsLeader: true, LeaderUntil: &until, LastSeen: time.Now(), CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	type result struct {
		acquired bool
		err      error
	}
	results := make(chan result, contenders)
	for _, workerID := range ids {
		go func() {
			<-start
			ok, err := s.AcquireLeadership(ctx, workerID, time.Minute)
			results <- result{ok, err}
		}()
	}
	close(start)
	winners := 0
	for range ids {
		got := <-results
		if got.err != nil {
			t.Error(got.err)
		}
		if got.acquired {
			winners++
		}
	}
	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leaders := 0
	for _, worker := range workers {
		if worker.IsLeader {
			leaders++
		}
	}
	if winners != 1 || leaders != 1 {
		t.Fatalf("successful claims=%d, leader rows=%d; want one of each", winners, leaders)
	}
}

func TestLeadershipRejectsAmbiguousLegacyRows(t *testing.T) {
	s := openSqliteStore(t)
	ctx := context.Background()
	until := time.Now().Add(time.Minute)
	for range 2 {
		if err := s.RegisterWorker(ctx, &cluster.Worker{ID: id.NewWorkerID(), State: cluster.WorkerActive, IsLeader: true, LeaderUntil: &until, LastSeen: time.Now(), CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.GetLeader(ctx); err == nil {
		t.Fatal("ambiguous registry returned an arbitrary leader")
	}
	if acquired, err := s.AcquireLeadership(ctx, id.NewWorkerID(), time.Minute); err == nil || acquired {
		t.Fatalf("ambiguous registry claim=%v, %v", acquired, err)
	}
}
