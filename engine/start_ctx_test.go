package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
)

// Start's ctx bounds the boot, not the engine's life. A caller that boots
// with a timeout must not have the durable worker stop when it expires.
func TestStartCtxCancellationLeavesDurableWorkerRunning(t *testing.T) {
	d, err := dispatch.New(dispatch.WithStore(memory.New()), dispatch.WithConcurrency(1), dispatch.WithPollInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	o := drt.Options{Namespace: "boot", Queue: "work", BuildID: "v1", Owner: "worker", PollInterval: time.Millisecond,
		Workflows: map[string]drt.WorkflowFunc{"noop": func(*drt.Workflow, []byte) ([]byte, error) { return nil, nil }}}
	eng, err := engine.Build(d, engine.WithDurableWorkflows(o))
	if err != nil {
		t.Fatal(err)
	}

	bootCtx, cancelBoot := context.WithCancel(context.Background())
	if err = eng.Start(bootCtx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })

	waitDurable := func(want drt.WorkerState) drt.WorkerState {
		deadline := time.Now().Add(2 * time.Second)
		for {
			status, e := eng.DurableStatus()
			if e != nil {
				t.Fatal(e)
			}
			if status.State == want || time.Now().After(deadline) {
				return status.State
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if got := waitDurable(drt.WorkerRunning); got != drt.WorkerRunning {
		t.Fatalf("durable worker state after Start = %s, want running", got)
	}

	cancelBoot()
	time.Sleep(100 * time.Millisecond)
	if status, e := eng.DurableStatus(); e != nil || status.State != drt.WorkerRunning {
		t.Fatalf("durable worker after Start's ctx was cancelled: %+v %v, want running", status, e)
	}
}
