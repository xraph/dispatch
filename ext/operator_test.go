package ext_test

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

// operatorExt implements only the two operator-facing hooks.
type operatorExt struct {
	cancelled []*job.Job
	actions   []ext.Action
}

func (e *operatorExt) Name() string { return "operator" }

func (e *operatorExt) OnJobCancelled(_ context.Context, j *job.Job) error {
	e.cancelled = append(e.cancelled, j)
	return nil
}

func (e *operatorExt) OnOperatorAction(_ context.Context, a ext.Action) error {
	e.actions = append(e.actions, a)
	return nil
}

// failingOperatorExt returns an error from both operator-facing hooks.
type failingOperatorExt struct{}

func (e *failingOperatorExt) Name() string { return "failing-operator" }

func (e *failingOperatorExt) OnJobCancelled(_ context.Context, _ *job.Job) error {
	return errors.New("cancel boom")
}

func (e *failingOperatorExt) OnOperatorAction(_ context.Context, _ ext.Action) error {
	return errors.New("action boom")
}

func TestRegistry_JobCancelledReachesImplementorsOnly(t *testing.T) {
	r := ext.NewRegistry(log.NewNoopLogger())
	jo := &jobOnlyExt{}
	op := &operatorExt{}
	r.Register(jo)
	r.Register(op)

	j := &job.Job{ID: id.NewJobID(), Name: "test-job"}
	r.EmitJobCancelled(context.Background(), j)

	if len(op.cancelled) != 1 || op.cancelled[0] != j {
		t.Fatalf("operator ext: want the cancelled job once, got %v", op.cancelled)
	}
	if len(jo.calls) != 0 {
		t.Fatalf("job-only ext does not implement JobCancelled, got calls %v", jo.calls)
	}
}

func TestRegistry_OperatorActionFillsActorAndTime(t *testing.T) {
	r := ext.NewRegistry(log.NewNoopLogger())
	op := &operatorExt{}
	r.Register(op)

	ctx := ext.WithActor(context.Background(), "user_42")
	jobID := id.NewJobID()

	before := time.Now().UTC()
	r.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionJobCancelled, JobID: jobID})
	after := time.Now().UTC()

	if len(op.actions) != 1 {
		t.Fatalf("want 1 action, got %d", len(op.actions))
	}
	got := op.actions[0]
	if got.Kind != ext.ActionJobCancelled {
		t.Errorf("Kind = %q, want %q", got.Kind, ext.ActionJobCancelled)
	}
	if got.JobID != jobID {
		t.Errorf("JobID = %s, want %s", got.JobID, jobID)
	}
	if got.Actor != "user_42" {
		t.Errorf("Actor = %q, want %q (from ctx)", got.Actor, "user_42")
	}
	if got.At.Before(before) || got.At.After(after) {
		t.Errorf("At = %v, want between %v and %v", got.At, before, after)
	}
	if got.At.Location() != time.UTC {
		t.Errorf("At location = %v, want UTC", got.At.Location())
	}
}

func TestRegistry_OperatorActionKeepsExplicitActorAndTime(t *testing.T) {
	r := ext.NewRegistry(log.NewNoopLogger())
	op := &operatorExt{}
	r.Register(op)

	ctx := ext.WithActor(context.Background(), "user_42")
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	r.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionDLQPurged, Actor: "system", Count: 3, At: at})

	if len(op.actions) != 1 {
		t.Fatalf("want 1 action, got %d", len(op.actions))
	}
	got := op.actions[0]
	if got.Actor != "system" {
		t.Errorf("Actor = %q, want the explicit %q", got.Actor, "system")
	}
	if !got.At.Equal(at) {
		t.Errorf("At = %v, want the explicit %v", got.At, at)
	}
	if got.Count != 3 {
		t.Errorf("Count = %d, want 3", got.Count)
	}
}

func TestRegistry_OperatorActionWithoutActorStaysEmpty(t *testing.T) {
	r := ext.NewRegistry(log.NewNoopLogger())
	op := &operatorExt{}
	r.Register(op)

	r.EmitOperatorAction(context.Background(), ext.Action{Kind: ext.ActionCronTriggered, CronID: id.NewCronID()})

	if len(op.actions) != 1 {
		t.Fatalf("want 1 action, got %d", len(op.actions))
	}
	if op.actions[0].Actor != "" {
		t.Errorf("Actor = %q, want empty when ctx carries none", op.actions[0].Actor)
	}
}

func TestRegistry_OperatorHookErrorsLoggedNotPropagated(t *testing.T) {
	r := ext.NewRegistry(log.NewNoopLogger())
	op := &operatorExt{}
	r.Register(&failingOperatorExt{})
	r.Register(op)

	ctx := context.Background()
	r.EmitJobCancelled(ctx, &job.Job{ID: id.NewJobID()})
	r.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionJobRetried})

	if len(op.cancelled) != 1 || len(op.actions) != 1 {
		t.Fatalf("a failing hook must not stop later ones: cancelled=%d actions=%d", len(op.cancelled), len(op.actions))
	}
}

func TestActorFrom(t *testing.T) {
	if got := ext.ActorFrom(context.Background()); got != "" {
		t.Errorf("ActorFrom(background) = %q, want empty", got)
	}
	ctx := ext.WithActor(context.Background(), "user_7")
	if got := ext.ActorFrom(ctx); got != "user_7" {
		t.Errorf("ActorFrom = %q, want %q", got, "user_7")
	}
}

func TestRegistry_EmptyRegistryOperatorNoOp(_ *testing.T) {
	r := ext.NewRegistry(log.NewNoopLogger())
	ctx := context.Background()

	// Neither should panic with no extensions registered.
	r.EmitJobCancelled(ctx, &job.Job{})
	r.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionCronDeleted})
}
