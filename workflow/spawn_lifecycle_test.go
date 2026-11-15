package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/workflow"
)

// A spawned child is a background run: it outlives the caller that
// started its parent, Shutdown cancels it and waits for it, and a spawn
// after Shutdown is refused.
func TestSpawnChild_ShutdownCancelsAndWaits(t *testing.T) {
	runner, reg, _ := newTestRunner()

	started := make(chan struct{})
	finished := make(chan error, 1)
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("held-child", func(wf *workflow.Workflow, _ struct{}) error {
		close(started)
		<-wf.Context().Done()
		time.Sleep(50 * time.Millisecond) // Shutdown must wait this out.
		finished <- wf.Context().Err()
		return wf.Context().Err()
	}))
	workflow.RegisterDefinition(reg, workflow.NewWorkflow("spawning-parent", func(wf *workflow.Workflow, _ struct{}) error {
		_, err := workflow.SpawnChild[struct{}](wf, "held-child", struct{}{})
		return err
	}))

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	if _, err := workflow.Start(callerCtx, runner, "spawning-parent", struct{}{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelCaller()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("child never started")
	}
	select {
	case err := <-finished:
		t.Fatalf("child ended (%v) when its parent's caller was cancelled", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := runner.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("child ctx err = %v, want Canceled", err)
		}
	default:
		t.Fatal("Shutdown returned before the spawned child finished")
	}

	if _, err := runner.SpawnChildRaw(context.Background(), (&workflow.Run{}).ID, "held-child", []byte(`{}`)); !errors.Is(err, workflow.ErrRunnerShutdown) {
		t.Fatalf("SpawnChildRaw after Shutdown: err = %v, want ErrRunnerShutdown", err)
	}
}
