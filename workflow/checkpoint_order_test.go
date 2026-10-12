package workflow_test

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

type tiedCheckpointStore struct {
	*memory.Store
	checkpoints []*workflow.Checkpoint
}

func (s *tiedCheckpointStore) ListCheckpoints(context.Context, id.RunID) ([]*workflow.Checkpoint, error) {
	return append([]*workflow.Checkpoint(nil), s.checkpoints...), nil
}
func TestReplayPlanAndTimelineShareTieOrder(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "tied-steps", State: workflow.RunStateCompleted, StartedAt: at, Version: 3}
	s := &tiedCheckpointStore{Store: memory.New()}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	ordered := make([]*workflow.Checkpoint, 0, 3)
	for range 3 {
		ordered = append(ordered, &workflow.Checkpoint{ID: id.NewCheckpointID(), RunID: run.ID, CreatedAt: at})
	}
	slices.SortFunc(ordered, func(a, b *workflow.Checkpoint) int { return strings.Compare(a.ID.String(), b.ID.String()) })
	for i, name := range []string{"before", "target", "after"} {
		ordered[i].StepName = name
	}
	s.checkpoints = []*workflow.Checkpoint{ordered[2], ordered[0], ordered[1]}
	runner, reg, _ := newReplayRunner(t, s, s.Store)
	workflow.RegisterDefinition(reg, workflow.NewWorkflowV("tied-steps", 3, func(*workflow.Workflow, struct{}) error { return nil }))
	plan, err := runner.PlanReplay(ctx, run.ID, "target")
	if err != nil || !reflect.DeepEqual(plan.Reruns, []string{"after"}) {
		t.Fatalf("plan = %+v, %v", plan, err)
	}
	timeline, err := runner.GetTimeline(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(timeline))
	for _, cp := range timeline {
		got = append(got, cp.StepName)
	}
	if !reflect.DeepEqual(got, []string{"before", "target", "after"}) {
		t.Fatalf("timeline = %v", got)
	}
}
