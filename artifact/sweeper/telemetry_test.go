package sweeper_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact/sweeper"
	"github.com/xraph/dispatch/internal/metricstest"
	"github.com/xraph/dispatch/internal/oteltest"
	"github.com/xraph/dispatch/telemetry"
)

func TestSweepPassIsARootSpanAndFollowersStartNone(t *testing.T) {
	sr := oteltest.Record(t)
	r := newRig(t)
	r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)
	f := metricstest.New()

	leader := false
	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0),
		sweeper.WithInterval(5*time.Millisecond),
		sweeper.WithLeaderCheck(func() bool { return leader }),
		sweeper.WithTelemetry(telemetry.New(telemetry.WithMetrics(f))))

	if _, err := s.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if spans := sr.Ended(); len(spans) != 0 {
		t.Fatalf("a follower's pass started %d spans", len(spans))
	}

	leader = true
	// Started under a context carrying a span, as an extension's Start is.
	if err := s.Start(oteltest.SpanContext(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	pass := sr.WaitFor(t, "sweep artifacts", 1, 5*time.Second)[0]
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !oteltest.IsRoot(pass) {
		t.Error("a background sweep pass should start its own trace")
	}
	if got, _ := oteltest.Attr(pass, "dispatch.artifact.swept"); got != "1" {
		t.Errorf("swept = %q", got)
	}
	if v, ok := f.CounterValue(telemetry.MetricArtifactsSwept, "reason", "retention"); !ok || v != 1 {
		t.Errorf("swept metric = %v, %v", v, ok)
	}
	// A zero purge grace purges in the same pass that swept.
	if got, _ := oteltest.Attr(pass, "dispatch.artifact.purged"); got != "1" {
		t.Errorf("purged = %q", got)
	}
	if v, ok := f.CounterValue(telemetry.MetricArtifactsPurged, "result", telemetry.PurgeOK); !ok || v != 1 {
		t.Errorf("purged metric = %v, %v", v, ok)
	}
	if v, ok := f.CounterValue(telemetry.MetricArtifactBytesReclaimed); !ok || v != float64(len("payload")) {
		t.Errorf("bytes reclaimed = %v, %v", v, ok)
	}
}
