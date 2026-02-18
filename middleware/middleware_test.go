package middleware_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/middleware"

	"github.com/xraph/forge"
)

func TestChain_ExecutionOrder(t *testing.T) {
	var order []string

	mw1 := func(ctx context.Context, _ *job.Job, next middleware.Handler) error {
		order = append(order, "mw1-before")
		err := next(ctx)
		order = append(order, "mw1-after")
		return err
	}

	mw2 := func(ctx context.Context, _ *job.Job, next middleware.Handler) error {
		order = append(order, "mw2-before")
		err := next(ctx)
		order = append(order, "mw2-after")
		return err
	}

	chain := middleware.Chain(mw1, mw2)
	j := &job.Job{Name: "test", ID: id.NewJobID()}
	handler := func(_ context.Context) error {
		order = append(order, "handler")
		return nil
	}

	err := chain(context.Background(), j, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d calls, got %d: %v", len(expected), len(order), order)
	}
	for i, want := range expected {
		if order[i] != want {
			t.Errorf("order[%d] = %q, want %q", i, order[i], want)
		}
	}
}

func TestChain_Empty(t *testing.T) {
	chain := middleware.Chain()
	called := false
	handler := func(_ context.Context) error {
		called = true
		return nil
	}

	err := chain(context.Background(), &job.Job{ID: id.NewJobID()}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler not called with empty chain")
	}
}

func TestChain_PropagatesError(t *testing.T) {
	mw := func(ctx context.Context, _ *job.Job, next middleware.Handler) error {
		return next(ctx)
	}
	chain := middleware.Chain(mw)
	want := errors.New("handler error")

	err := chain(context.Background(), &job.Job{ID: id.NewJobID()}, func(_ context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestRecover_CatchesPanic(t *testing.T) {
	logger := slog.Default()
	mw := middleware.Recover(logger)
	j := &job.Job{Name: "panicky", ID: id.NewJobID()}

	err := mw(context.Background(), j, func(_ context.Context) error {
		panic("test panic")
	})
	if err == nil {
		t.Fatal("expected error from panic recovery")
	}
	if got := err.Error(); got != "panic in job panicky: test panic" {
		t.Errorf("unexpected error message: %q", got)
	}
}

func TestRecover_PassesThrough(t *testing.T) {
	logger := slog.Default()
	mw := middleware.Recover(logger)
	j := &job.Job{Name: "normal", ID: id.NewJobID()}

	called := false
	err := mw(context.Background(), j, func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler not called")
	}
}

func TestLogging_Success(t *testing.T) {
	logger := slog.Default()
	mw := middleware.Logging(logger)
	j := &job.Job{Name: "log-test", ID: id.NewJobID(), Queue: "default"}

	called := false
	err := mw(context.Background(), j, func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler not called")
	}
}

func TestLogging_Error(t *testing.T) {
	logger := slog.Default()
	mw := middleware.Logging(logger)
	j := &job.Job{Name: "log-test", ID: id.NewJobID(), Queue: "default"}
	want := errors.New("fail")

	err := mw(context.Background(), j, func(_ context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestScope_RestoresFromJob(t *testing.T) {
	mw := middleware.Scope()
	j := &job.Job{
		Name:       "scoped",
		ID:         id.NewJobID(),
		ScopeAppID: "app_test123",
		ScopeOrgID: "org_test456",
	}

	err := mw(context.Background(), j, func(ctx context.Context) error {
		s, ok := forge.ScopeFrom(ctx)
		if !ok {
			t.Fatal("expected scope in context")
		}
		if got := s.AppID(); got != "app_test123" {
			t.Errorf("AppID = %q, want %q", got, "app_test123")
		}
		if got := s.OrgID(); got != "org_test456" {
			t.Errorf("OrgID = %q, want %q", got, "org_test456")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScope_NoOpWhenEmpty(t *testing.T) {
	mw := middleware.Scope()
	j := &job.Job{Name: "unscoped", ID: id.NewJobID()}

	err := mw(context.Background(), j, func(ctx context.Context) error {
		_, ok := forge.ScopeFrom(ctx)
		if ok {
			t.Fatal("expected no scope in context for unscoped job")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
