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
