package engine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
)

func TestEngineReplayFromGenerationEmitsOnlyForAcceptedPreview(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int32
	eng, _, rec := replayEngine(t, &fail, nil, &calls)
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })
	run, err := engine.StartWorkflow(context.Background(), eng, "eng-replay", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	rec.waitEnd(t)
	ctx := ext.WithActor(context.Background(), "preview-operator")
	if _, err := eng.ReplayWorkflowFromGeneration(ctx, run.ID, "step-1", 0); err != nil {
		t.Fatal(err)
	}
	rec.waitEnd(t)
	if _, err := eng.ReplayWorkflowFromGeneration(ctx, run.ID, "step-1", 0); !errors.Is(err, dispatch.ErrInvalidState) {
		t.Fatalf("stale preview: %v", err)
	}
	actions := rec.recorded()
	if len(actions) != 1 || actions[0].Actor != "preview-operator" || actions[0].Kind != ext.ActionWorkflowReplayed {
		t.Fatalf("actions = %+v", actions)
	}
}
