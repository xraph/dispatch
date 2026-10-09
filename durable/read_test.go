package durable

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReadCursorMaximumEscaping(t *testing.T) {
	t.Logf("inner cursor bound: %d bytes", MaxReadCursorBytes)
	for _, char := range []string{"x", "<", "&", "\x01"} {
		id := strings.Repeat(char, MaxIdentifierBytes)
		r := ExecutionList{Namespace: id, WorkflowID: id, WorkflowType: id, BuildID: id, Limit: 1}
		execution := Execution{Key: Key{Namespace: id, WorkflowID: id, RunID: id}, CreatedAt: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("offset", -7*3600))}
		token, err := r.Next(execution)
		if err != nil || len(token) > MaxReadCursorBytes {
			t.Fatal("seal", len(token), err)
		}
		r.Cursor = token
		p, err := r.Position()
		if err != nil || p.ID != id || p.WorkflowID != id || !p.CreatedAt.Equal(execution.CreatedAt) {
			t.Fatal("round trip", err)
		}
		task := TaskList{Key: execution.Key, Limit: 1}
		task.Cursor, err = task.Next(Task{TaskSpec: TaskSpec{ID: id}})
		if err != nil {
			t.Fatal(err)
		}
		p, err = task.Position()
		if err != nil || p.ID != id {
			t.Fatal("task round trip", err)
		}
	}
}

func TestReadCursorBounds(t *testing.T) {
	r := ExecutionList{Namespace: "namespace", Limit: 1, Cursor: strings.Repeat("A", MaxReadCursorBytes+1)}
	if _, err := r.Position(); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized input", err)
	}
	r.Cursor = ""
	invalid := strings.Repeat("x", MaxIdentifierBytes+1)
	for _, value := range []ExecutionList{{Namespace: invalid, Limit: 1}, {Namespace: "ns", WorkflowID: invalid, Limit: 1}, {Namespace: "ns", WorkflowType: invalid, Limit: 1}, {Namespace: "ns", BuildID: invalid, Limit: 1}} {
		if _, err := value.Position(); !errors.Is(err, ErrInvalid) {
			t.Fatal("oversized filter", err)
		}
	}
	if token, err := r.Next(Execution{Key: Key{WorkflowID: "workflow", RunID: invalid}, CreatedAt: time.Now()}); token != "" || !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized output", err)
	}
	if token, err := (TaskList{Key: Key{Namespace: "ns", WorkflowID: "wf", RunID: "run"}, Limit: 1}).Next(Task{TaskSpec: TaskSpec{ID: invalid}}); token != "" || !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized task", err)
	}
	for _, pair := range [][2]string{{invalid, "build"}, {"ns", invalid}, {"ns", ""}} {
		if err := ValidateBuildRead(pair[0], pair[1]); !errors.Is(err, ErrInvalid) {
			t.Fatal("build bounds", err)
		}
	}
	if DeliveryIdentifier(strings.Repeat("x", MaxDeliveryIdentifierBytes+1)) {
		t.Fatal("catalog contract changed")
	}
}
