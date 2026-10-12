package durable_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func validChildDelivery() durable.ChildDelivery {
	parent := durable.Key{Namespace: "n", WorkflowID: "parent", RunID: "run"}
	child := durable.Key{Namespace: "n", WorkflowID: "child", RunID: "run"}
	return durable.ChildDelivery{Source: child, Target: parent, ID: "result", Kind: durable.ChildDeliveryResult, TargetBuildID: "v1", TargetQueue: "q", Message: durable.ChildMessage{Version: 1, CommandID: "child", Parent: parent, Child: child, Policy: durable.ParentCloseTerminate, State: durable.StateCompleted, Output: []byte("output"), CloseEvent: durable.EventInput{Type: "workflow.completed", Payload: []byte("output")}}}
}

func TestChildDeliveryValidation(t *testing.T) {
	valid := validChildDelivery()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	changes := map[string]func(*durable.ChildDelivery){
		"kind":          func(d *durable.ChildDelivery) { d.Kind = "unknown" },
		"namespace":     func(d *durable.ChildDelivery) { d.Target.Namespace = "other" },
		"source":        func(d *durable.ChildDelivery) { d.Source = d.Target },
		"self":          func(d *durable.ChildDelivery) { d.Message.Child = d.Message.Parent },
		"target":        func(d *durable.ChildDelivery) { d.Target.RunID = "other" },
		"version":       func(d *durable.ChildDelivery) { d.Message.Version = 2 },
		"policy":        func(d *durable.ChildDelivery) { d.Message.Policy = "unknown" },
		"command":       func(d *durable.ChildDelivery) { d.Message.CommandID = "" },
		"build":         func(d *durable.ChildDelivery) { d.TargetBuildID = "" },
		"queue":         func(d *durable.ChildDelivery) { d.TargetQueue = "" },
		"id":            func(d *durable.ChildDelivery) { d.ID = strings.Repeat("x", 513) },
		"state":         func(d *durable.ChildDelivery) { d.Message.State = durable.StateRunning },
		"close_event":   func(d *durable.ChildDelivery) { d.Message.CloseEvent.Type = "" },
		"result_cancel": func(d *durable.ChildDelivery) { d.Message.CancellationID = "cancel" },
		"result_ack":    func(d *durable.ChildDelivery) { d.Message.Disposition = durable.ChildDeliveryApplied },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			bad := valid.Clone()
			change(&bad)
			if err := bad.Validate(); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("accepted malformed message: %v", err)
			}
		})
	}
	cancel := valid
	cancel.Kind = durable.ChildDeliveryCancel
	cancel.Source, cancel.Target = valid.Target, valid.Source
	cancel.Message.State = ""
	cancel.Message.Output = nil
	cancel.Message.CloseEvent = durable.EventInput{}
	cancel.Message.CancellationID = strings.Repeat("c", 512)
	if err := cancel.Validate(); err != nil {
		t.Fatal(err)
	}
	request := cancel.CancellationRequest()
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	ack, err := cancel.CancellationAcknowledgment("v1", "q", durable.ChildDeliveryApplied, time.Now())
	if err != nil || len(ack.ID) > 512 || ack.Source != cancel.Target || ack.Target != cancel.Source {
		t.Fatalf("ack identity: %+v %v", ack, err)
	}
	for _, disposition := range []string{"", "completed"} {
		bad := ack
		bad.Message.Disposition = disposition
		if err = bad.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid acknowledgment: %v", err)
		}
	}
	cancel.Message.Output = []byte("not a result")
	if err = cancel.Validate(); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("cancel claimed output: %v", err)
	}
}

func TestChildDeliveryRequestValidation(t *testing.T) {
	source := validChildDelivery().Source
	claim := durable.ChildDeliveryClaimRequest{Namespace: source.Namespace, Owner: "owner", BuildID: "v1", LeaseDuration: time.Minute}
	for _, mode := range []string{"namespace", "owner", "build", "short", "long"} {
		bad := claim
		switch mode {
		case "namespace":
			bad.Namespace = ""
		case "owner":
			bad.Owner = "\x00"
		case "build":
			bad.BuildID = "\xff"
		case "short":
			bad.LeaseDuration = time.Nanosecond
		case "long":
			bad.LeaseDuration = 25 * time.Hour
		}
		if err := bad.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("claim %s: %v", mode, err)
		}
	}
	r := durable.ChildDeliveryRequest{Source: source, DeliveryID: "result", RequestID: "apply", Owner: "owner", Epoch: 1}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"source", "delivery", "request", "owner", "epoch"} {
		bad := r
		switch mode {
		case "source":
			bad.Source.RunID = ""
		case "delivery":
			bad.DeliveryID = ""
		case "request":
			bad.RequestID = ""
		case "owner":
			bad.Owner = ""
		case "epoch":
			bad.Epoch = 0
		}
		if err := bad.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("request %s: %v", mode, err)
		}
	}
}

func TestChildCancellationDecisionValidation(t *testing.T) {
	r := durable.CommitRequest{Key: validChildDelivery().Target, RequestID: "cancel", ExpectedRevision: 1, Token: durable.TaskToken{TaskID: "w", Owner: "owner", Epoch: 1}, Events: []durable.EventInput{{Type: "cancel"}}, CancelChildren: []durable.ChildCancellationSpec{{CommandID: "cancel", TargetID: "child"}}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"terminal", "retained", "duplicate", "command", "target", "count"} {
		bad := r
		bad.CancelChildren = append([]durable.ChildCancellationSpec(nil), r.CancelChildren...)
		switch mode {
		case "terminal":
			bad.State = durable.StateCompleted
		case "retained":
			bad.TaskUpdate = &durable.TaskUpdate{Action: durable.TaskKeep}
		case "duplicate":
			bad.CancelChildren = append(bad.CancelChildren, r.CancelChildren[0])
		case "command":
			bad.CancelChildren[0].CommandID = ""
		case "target":
			bad.CancelChildren[0].TargetID = ""
		case "count":
			bad.CancelChildren = make([]durable.ChildCancellationSpec, 1001)
		}
		if err := bad.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("decision %s: %v", mode, err)
		}
	}
	for _, task := range []durable.Task{{TaskSpec: durable.TaskSpec{Kind: durable.TaskActivity}}, {TaskSpec: durable.TaskSpec{Kind: durable.TaskWorkflow}, LeaseKind: durable.LeaseAsync}} {
		if err := durable.ValidateChildCancellationSource(task, r.CancelChildren); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid decision source: %v", err)
		}
	}
}
