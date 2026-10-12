package durable_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func TestSignalValidation(t *testing.T) {
	base := durable.SignalRequest{Key: durable.Key{Namespace: "n", WorkflowID: "w"}, RequestID: "request", BuildID: "v1", Name: "approve"}
	for _, mode := range []string{"valid", "name_limit", "name_excess", "id_limit", "id_excess", "build_limit", "build_excess", "payload_limit", "payload_excess", "blank", "nul", "utf8", "bad_run"} {
		t.Run(mode, func(t *testing.T) {
			r := base
			valid := false
			switch mode {
			case "valid":
				valid = true
			case "name_limit":
				r.Name = strings.Repeat("a", 200)
				valid = true
			case "name_excess":
				r.Name = strings.Repeat("a", 201)
			case "id_limit":
				r.RequestID = strings.Repeat("a", 512)
				valid = true
			case "id_excess":
				r.RequestID = strings.Repeat("a", 513)
			case "build_limit":
				r.BuildID = strings.Repeat("a", 512)
				valid = true
			case "build_excess":
				r.BuildID = strings.Repeat("a", 513)
			case "payload_limit":
				r.Input = make([]byte, 1<<20)
				valid = true
			case "payload_excess":
				r.Input = make([]byte, (1<<20)+1)
			case "blank":
				r.Name = " "
			case "nul":
				r.RequestID = "a\x00b"
			case "utf8":
				r.Namespace = string([]byte{255})
			case "bad_run":
				r.RunID = " "
			}
			err := r.Validate()
			if valid && err != nil || !valid && !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("validation: %v", err)
			}
		})
	}
	start := durable.SignalWithStartRequest{Start: durable.StartRequest{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}, RequestID: "request", WorkflowType: "order", BuildID: "v1", Queue: "orders"}, Name: "approve"}
	if err := start.Validate(); err != nil {
		t.Fatal(err)
	}
	start.Start.RunID = ""
	if err := start.Validate(); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("missing proposed identity: %v", err)
	}
}
