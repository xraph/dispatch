package middleware_test

import (
	"context"
	"errors"
	"testing"

	gu "github.com/xraph/go-utils/metrics"

	mw "github.com/xraph/dispatch/middleware"
)

func TestMetrics_RecordsExecution(t *testing.T) {
	factory := gu.NewMetricsCollector("test")
	m := mw.MetricsWithFactory(factory)

	called := false
	err := m(context.Background(), newTestJob(), func(_ context.Context) error {
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

func TestMetrics_PropagatesError(t *testing.T) {
	factory := gu.NewMetricsCollector("test")
	m := mw.MetricsWithFactory(factory)
	want := errors.New("boom")

	err := m(context.Background(), newTestJob(), func(_ context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Errorf("expected %v, got %v", want, err)
	}
}

func TestMetrics_DefaultNoopSafe(t *testing.T) {
	m := mw.Metrics()

	called := false
	err := m(context.Background(), newTestJob(), func(_ context.Context) error {
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
