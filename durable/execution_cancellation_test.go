package durable_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func TestExecutionCancellationValidation(t *testing.T) {
	valid := durable.CancelExecutionRequest{Key: durable.Key{Namespace: "ns", WorkflowID: "w"}, RequestID: "cancel", BuildID: "v1"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"namespace", "workflow", "run", "request", "build", "reason_size", "reason_utf8", "reason_nul"} {
		r := valid
		switch mode {
		case "namespace":
			r.Namespace = ""
		case "workflow":
			r.WorkflowID = " "
		case "run":
			r.RunID = strings.Repeat("x", 513)
		case "request":
			r.RequestID = ""
		case "build":
			r.BuildID = ""
		case "reason_size":
			r.Reason = strings.Repeat("x", 4097)
		case "reason_utf8":
			r.Reason = string([]byte{0xff})
		case "reason_nul":
			r.Reason = "a\x00b"
		}
		if err := r.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("%s accepted: %v", mode, err)
		}
	}
	if err := (durable.ExecutionCancellation{Version: 2, RequestID: "c"}).Validate(); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("version accepted: %v", err)
	}
}
