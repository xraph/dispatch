package durable_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func TestExecutionTargetValidation(t *testing.T) {
	for _, selection := range []durable.RunSelection{durable.RunExplicit, durable.RunCurrent, durable.RunLatest} {
		valid := durable.ExecutionTarget{Key: durable.Key{Namespace: "ns", WorkflowID: "wf"}, Selection: selection}
		if selection == durable.RunExplicit {
			valid.RunID = "run"
		}
		if err := valid.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"", strings.Repeat("x", 513), " space", "\x00", string([]byte{255})} {
			invalid := valid
			invalid.Namespace = value
			if err := invalid.Validate(); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("namespace %q: %v", value, err)
			}
			invalid = valid
			invalid.WorkflowID = value
			if err := invalid.Validate(); !errors.Is(err, durable.ErrInvalid) {
				t.Fatalf("workflow %q: %v", value, err)
			}
		}
		boundary := valid
		boundary.Namespace = strings.Repeat("n", 512)
		boundary.WorkflowID = strings.Repeat("w", 512)
		if err := boundary.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
