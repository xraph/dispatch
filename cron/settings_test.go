package cron

import (
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/id"
)

func TestSettingsMatchSchedulerDefaultsAndOverrides(t *testing.T) {
	s := NewScheduler(nil, nil, nil, nil, id.NewWorkerID(), log.NewNoopLogger())
	got := s.Settings()
	if got.TickInterval != time.Second || got.LeaderTTL != time.Minute ||
		got.RefreshInterval != 30*time.Second || got.LockTTL != 30*time.Second ||
		got.StoreCallTimeout != defaultStoreCallTimeout || !got.StoreCallsBounded {
		t.Fatalf("defaults = %+v", got)
	}
	s = NewScheduler(nil, nil, nil, nil, id.NewWorkerID(), log.NewNoopLogger(), WithSchedulerStoreCallTimeout(-1))
	if s.Settings().StoreCallsBounded {
		t.Fatal("negative timeout should be unbounded")
	}
}
