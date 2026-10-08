//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/workflow"
)

type workflowIdentityActions struct{ count atomic.Int32 }

func (*workflowIdentityActions) Name() string { return "workflow-identity-actions" }
func (r *workflowIdentityActions) OnOperatorAction(context.Context, ext.Action) error {
	r.count.Add(1)
	return nil
}

func TestWorkflowRejectsMismatchedStoredIdentityBeforeReplay(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)
	client := redisdriver.UnwrapClient(kvStore)
	d, err := dispatch.New(dispatch.WithStore(s))
	if err != nil {
		t.Fatal(err)
	}
	actions := &workflowIdentityActions{}
	eng, err := engine.Build(d, engine.WithExtension(actions))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Stop(context.Background()) })
	var executions atomic.Int32
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("identity", func(*workflow.Workflow, struct{}) error { executions.Add(1); return nil }))
	parent := id.NewRunID()
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "identity", State: workflow.RunStateCompleted, ParentRunID: &parent}
	if createErr := s.CreateRun(ctx, run); createErr != nil {
		t.Fatal(createErr)
	}
	if saveErr := s.SaveCheckpoint(ctx, run.ID, "target", []byte("{}")); saveErr != nil {
		t.Fatal(saveErr)
	}
	if saveErr := s.SaveCheckpoint(ctx, run.ID, "later", []byte("{}")); saveErr != nil {
		t.Fatal(saveErr)
	}
	key := "dispatch:run:" + run.ID.String()
	original, err := client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if decodeErr := json.Unmarshal(original, &record); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	otherID := id.NewRunID()
	record["id"], err = json.Marshal(otherID.String())
	if err != nil {
		t.Fatal(err)
	}
	corrupt, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if setErr := client.Set(ctx, key, corrupt, 0).Err(); setErr != nil {
		t.Fatal(setErr)
	}
	reads := map[string]func() error{
		"get":      func() error { _, readErr := s.GetRun(ctx, run.ID); return readErr },
		"list":     func() error { _, readErr := s.ListRuns(ctx, workflow.ListOpts{}); return readErr },
		"children": func() error { _, readErr := s.ListChildRuns(ctx, parent); return readErr },
		"page":     func() error { _, readErr := s.ListRunsPage(ctx, workflow.ListRunsPageOpts{}); return readErr },
		"count":    func() error { _, readErr := s.CountRuns(ctx, workflow.CountRunsOpts{}); return readErr },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			if readErr := read(); readErr == nil {
				t.Fatal("accepted another valid run ID from this key")
			}
		})
	}
	if _, replayErr := eng.ReplayWorkflowFromGeneration(ctx, run.ID, "target", 0); replayErr == nil {
		t.Error("replay accepted mismatched run identity")
	}
	if stopErr := eng.Stop(ctx); stopErr != nil {
		t.Fatal(stopErr)
	}
	after, err := client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(corrupt) {
		t.Error("replay mutated the corrupt source run")
	}
	cps, err := s.ListCheckpoints(ctx, run.ID)
	if err != nil || len(cps) != 2 {
		t.Fatalf("replay pruned checkpoints: %v, %v", cps, err)
	}
	if actions.count.Load() != 0 || executions.Load() != 0 {
		t.Fatalf("actions=%d executions=%d", actions.count.Load(), executions.Load())
	}
}
