package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

func TestReplayFromGenerationRejectsStalePreview(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	r, reg, ends := newReplayRunner(t, s, s)
	var calls atomic.Int32
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("preview", func(wf *workflow.Workflow, _ struct{}) error {
		if err := wf.Step("target", func(context.Context) error { return nil }); err != nil {
			return err
		}
		calls.Add(1)
		return nil
	}))
	run, err := r.StartRaw(ctx, "preview", []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	waitEnd(t, ends)
	plan, err := r.PlanReplay(ctx, run.ID, "target")
	if err != nil {
		t.Fatal(err)
	}
	if _, replayErr := r.ReplayFromGeneration(ctx, run.ID, "target", plan.Generation); replayErr != nil {
		t.Fatal(replayErr)
	}
	waitEnd(t, ends)
	for _, generation := range []int64{plan.Generation, -1, plan.Generation + 2} {
		if _, replayErr := r.ReplayFromGeneration(ctx, run.ID, "target", generation); !errors.Is(replayErr, dispatch.ErrInvalidState) {
			t.Fatalf("generation %d accepted: %v", generation, replayErr)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("stale preview executed: %d", calls.Load())
	}
	fresh, err := r.PlanReplay(ctx, run.ID, "target")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReplayFromGeneration(ctx, run.ID, "target", fresh.Generation); err != nil {
		t.Fatal(err)
	}
	waitEnd(t, ends)
	if calls.Load() != 3 {
		t.Fatalf("fresh preview did not execute: %d", calls.Load())
	}
}

func TestReplayFromGenerationStillFencesTheClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &delayedReopen{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	r, reg, ends := newReplayRunner(t, s, s.Store)
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("claim", func(wf *workflow.Workflow, _ struct{}) error {
		return wf.Step("target", func(context.Context) error { return nil })
	}))
	run, err := r.StartRaw(ctx, "claim", []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	waitEnd(t, ends)
	result := make(chan error, 1)
	go func() { _, err := r.ReplayFromGeneration(ctx, run.ID, "target", 0); result <- err }()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal("claim not reached")
	}
	if _, err := r.ReplayFromGeneration(ctx, run.ID, "target", 0); err != nil {
		close(s.release)
		t.Fatal(err)
	}
	waitEnd(t, ends)
	close(s.release)
	if err := <-result; !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("overlap accepted: %v", err)
	}
}
