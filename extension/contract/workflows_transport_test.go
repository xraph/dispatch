package contract

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

func TestWorkflowTransportQueriesReplayAndInvalidations(t *testing.T) {
	ends := &workflowContractEnds{ends: make(chan struct{}, 1)}
	d := contractDeps(t, memory.New(), engine.WithExtension(ends))
	engine.RegisterWorkflow(d.Engine, workflow.NewWorkflow("transport", func(*workflow.Workflow, struct{}) error { return nil }))
	run := seedWorkflow(t, d, "transport", workflow.RunStateCompleted, "", "", nil)
	if err := d.Store.SaveCheckpoint(context.Background(), run.ID, "target", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	callContract(t, d, "query", "workflows.list", WorkflowsListInput{})
	callContract(t, d, "query", "workflows.get", IDInput{ID: run.ID.String()})
	input := WorkflowReplayInput{ID: run.ID.String(), FromStep: "target"}
	response := callContract(t, d, "query", "workflows.replayPreview", input)
	var preview WorkflowReplayPreview
	if err := json.Unmarshal(response.Data, &preview); err != nil {
		t.Fatal(err)
	}
	response = callContract(t, d, "command", "workflows.replayFrom", WorkflowReplayCommandInput{WorkflowReplayInput: input, ExpectedGeneration: &preview.Generation})
	want := []string{"workflows.list", "workflows.get", "workflows.replayPreview", "overview.summary"}
	if !reflect.DeepEqual(response.Meta.Invalidates, want) {
		t.Fatalf("invalidations=%v", response.Meta.Invalidates)
	}
	var result WorkflowReplayResult
	if err := json.Unmarshal(response.Data, &result); err != nil || result.AcceptedGeneration != 1 {
		t.Fatalf("result=%+v, %v", result, err)
	}
	ends.wait(t)
	current, err := d.Store.GetRun(context.Background(), run.ID)
	if err != nil || current.ReplayGeneration != 1 || current.State != workflow.RunStateCompleted {
		t.Fatalf("stored=%+v, %v", current, err)
	}
}
