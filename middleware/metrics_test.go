package middleware_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	mw "github.com/xraph/dispatch/middleware"
)

func setupTestMeter() (*sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, mp
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func TestMetrics_RecordsDuration(t *testing.T) {
	reader, mp := setupTestMeter()
	meter := mp.Meter("test")
	m := mw.MetricsWithMeter(meter)
	j := newTestJob()

	_ = m(context.Background(), j, func(_ context.Context) error {
		return nil
	})

	rm := collectMetrics(t, reader)
	metric := findMetric(rm, "dispatch.job.duration")
	if metric == nil {
		t.Fatal("dispatch.job.duration metric not found")
	}

	hist, ok := metric.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("expected Histogram[float64] data type")
	}
	if len(hist.DataPoints) == 0 {
		t.Fatal("no data points recorded for duration")
	}
	if hist.DataPoints[0].Count != 1 {
		t.Errorf("expected count=1, got %d", hist.DataPoints[0].Count)
	}
}

func TestMetrics_RecordsExecutions_Success(t *testing.T) {
	reader, mp := setupTestMeter()
	meter := mp.Meter("test")
	m := mw.MetricsWithMeter(meter)
	j := newTestJob()

	_ = m(context.Background(), j, func(_ context.Context) error {
		return nil
	})

	rm := collectMetrics(t, reader)
	metric := findMetric(rm, "dispatch.job.executions")
	if metric == nil {
		t.Fatal("dispatch.job.executions metric not found")
	}

	sum, ok := metric.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("expected Sum[int64] data type")
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points recorded")
	}
	if sum.DataPoints[0].Value != 1 {
		t.Errorf("expected value=1, got %d", sum.DataPoints[0].Value)
	}

	// Verify status=ok attribute.
	found := false
	for _, attr := range sum.DataPoints[0].Attributes.ToSlice() {
		if string(attr.Key) == "status" && attr.Value.AsString() == "ok" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected status=ok attribute on executions counter")
	}
}

func TestMetrics_RecordsExecutions_Error(t *testing.T) {
	reader, mp := setupTestMeter()
	meter := mp.Meter("test")
	m := mw.MetricsWithMeter(meter)
	j := newTestJob()

	_ = m(context.Background(), j, func(_ context.Context) error {
		return errors.New("boom")
	})

	rm := collectMetrics(t, reader)
	metric := findMetric(rm, "dispatch.job.executions")
	if metric == nil {
		t.Fatal("dispatch.job.executions metric not found")
	}

	sum, ok := metric.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("expected Sum[int64] data type")
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points recorded")
	}

	// Verify status=error attribute.
	found := false
	for _, attr := range sum.DataPoints[0].Attributes.ToSlice() {
		if string(attr.Key) == "status" && attr.Value.AsString() == "error" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected status=error attribute on executions counter")
	}
}

func TestMetrics_Attributes(t *testing.T) {
	reader, mp := setupTestMeter()
	meter := mp.Meter("test")
	m := mw.MetricsWithMeter(meter)
	j := newTestJob()

	_ = m(context.Background(), j, func(_ context.Context) error {
		return nil
	})

	rm := collectMetrics(t, reader)

	// Check both instruments have correct attributes.
	for _, name := range []string{"dispatch.job.duration", "dispatch.job.executions"} {
		metric := findMetric(rm, name)
		if metric == nil {
			t.Errorf("%s metric not found", name)
			continue
		}

		var attrs []attribute.KeyValue
		switch data := metric.Data.(type) {
		case metricdata.Histogram[float64]:
			if len(data.DataPoints) > 0 {
				attrs = data.DataPoints[0].Attributes.ToSlice()
			}
		case metricdata.Sum[int64]:
			if len(data.DataPoints) > 0 {
				attrs = data.DataPoints[0].Attributes.ToSlice()
			}
		}

		attrMap := make(map[string]string, len(attrs))
		for _, a := range attrs {
			if a.Value.Type() == attribute.STRING {
				attrMap[string(a.Key)] = a.Value.AsString()
			}
		}

		expected := map[string]string{
			"job_name": "send-email",
			"queue":    "default",
			"status":   "ok",
		}
		for key, want := range expected {
			got, ok := attrMap[key]
			if !ok {
				t.Errorf("%s: missing attribute %q", name, key)
				continue
			}
			if got != want {
				t.Errorf("%s: attribute %q = %q, want %q", name, key, got, want)
			}
		}
	}
}

func TestMetrics_DefaultNoopSafe(t *testing.T) {
	// Calling Metrics() without a global provider should not panic.
	m := mw.Metrics()
	j := newTestJob()

	called := false
	err := m(context.Background(), j, func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}
