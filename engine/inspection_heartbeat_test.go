package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

func TestInspectionReportsEffectiveWorkerHeartbeatAndStaleThreshold(t *testing.T) {
	for _, configured := range []time.Duration{0, -time.Second, 10 * time.Second, 2 * time.Minute} {
		t.Run(configured.String(), func(t *testing.T) {
			d, err := dispatch.New(dispatch.WithStore(memory.New()), dispatch.WithHeartbeatInterval(configured))
			if err != nil {
				t.Fatal(err)
			}
			eng, err := engine.Build(d)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = eng.Stop(context.Background()) })
			got := eng.Inspect()
			want := configured
			if want <= 0 {
				want = 10 * time.Second
			}
			if got.WorkerHeartbeatInterval != want || got.WorkerStaleThreshold != max(5*configured, 5*time.Minute) {
				t.Fatalf("timings=%+v", got)
			}
			if got.Pool.HeartbeatInterval != configured {
				t.Fatal("job heartbeat setting was replaced by the row heartbeat fallback")
			}
		})
	}
}
