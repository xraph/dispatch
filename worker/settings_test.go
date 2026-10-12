package worker

import (
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"
)

func TestSettingsResolveDefaultsAndCopyQueues(t *testing.T) {
	p := NewPool(nil, nil, nil, log.NewNoopLogger(), WithPollInterval(time.Minute),
		WithPoolQueues([]string{"mail"}), WithStaleJobThreshold(45*time.Second),
		WithHeartbeatInterval(0), WithStoreCallTimeout(-1))
	got := p.Settings()
	if got.MaxPollInterval != time.Minute || got.DefaultLeaseTTL != 45*time.Second ||
		got.ReapInterval != DefaultReapInterval || got.HeartbeatInterval != 0 || got.StoreCallsBounded {
		t.Fatalf("settings = %+v", got)
	}
	got.Queues[0] = "changed"
	if p.Settings().Queues[0] != "mail" {
		t.Fatal("caller changed queues")
	}
	if d := NewPool(nil, nil, nil, log.NewNoopLogger()).Settings(); d.StoreCallTimeout != defaultStoreCallTimeout || !d.StoreCallsBounded {
		t.Fatalf("default timeout = %+v", d)
	}
}
