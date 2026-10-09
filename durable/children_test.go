package durable_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func TestChildValidation(t *testing.T) {
	parent := durable.Key{Namespace: "n", WorkflowID: "parent", RunID: "run"}
	valid := durable.ChildStartSpec{CommandID: "child", Start: durable.StartRequest{Key: durable.Key{Namespace: "n", WorkflowID: "child", RunID: "run"}, RequestID: "start", WorkflowType: "child", BuildID: "v1", Queue: "q"}, ParentQueue: "q", ParentClosePolicy: durable.ParentCloseTerminate}
	if err := valid.Validate(parent); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"namespace", "self", "command", "policy", "queue", "input", "start", "utf8", "nul"} {
		t.Run(mode, func(t *testing.T) {
			bad := valid
			switch mode {
			case "namespace":
				bad.Start.Namespace = "other"
			case "self":
				bad.Start.Key = parent
			case "command":
				bad.CommandID = strings.Repeat("x", 513)
			case "policy":
				bad.ParentClosePolicy = ""
			case "queue":
				bad.ParentQueue = ""
			case "input":
				bad.Start.Input = make([]byte, (1<<20)+1)
			case "start":
				bad.Start.BuildID = ""
			case "utf8":
				bad.CommandID = "\xff"
			case "nul":
				bad.CommandID = "a\x00b"
			}
			if err := bad.Validate(parent); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("invalid %s accepted: %v", mode, err)
			}
		})
	}
	valid.Start.Input = make([]byte, 1<<20)
	valid.CommandID = strings.Repeat("c", 512)
	if err := valid.Validate(parent); err != nil {
		t.Fatalf("boundary: %v", err)
	}
	request := durable.CommitRequest{Key: parent, RequestID: "commit", ExpectedRevision: 1, Token: durable.TaskToken{TaskID: "w", Owner: "w", Epoch: 1}, Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{valid}}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"duplicate_command", "duplicate_workflow", "events", "terminal", "retained"} {
		bad := request
		switch mode {
		case "duplicate_command":
			second := valid
			second.Start.WorkflowID = "different"
			bad.Children = append([]durable.ChildStartSpec{valid}, second)
		case "duplicate_workflow":
			second := valid
			second.CommandID = "different"
			second.Start.RunID = "different"
			bad.Children = append([]durable.ChildStartSpec{valid}, second)
		case "events":
			bad.Events = make([]durable.EventInput, 1000)
			for i := range bad.Events {
				bad.Events[i].Type = "event"
			}
		case "terminal":
			bad.State = durable.StateCompleted
		case "retained":
			bad.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
		}
		if err := bad.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid %s: %v", mode, err)
		}
	}
}
