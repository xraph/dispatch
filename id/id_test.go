package id_test

import (
	"strings"
	"testing"

	"github.com/xraph/dispatch/id"
)

func TestConstructors(t *testing.T) {
	tests := []struct {
		name   string
		newFn  func() id.ID
		prefix string
	}{
		{"JobID", id.NewJobID, "job_"},
		{"WorkflowID", id.NewWorkflowID, "wf_"},
		{"RunID", id.NewRunID, "wfrun_"},
		{"CheckpointID", id.NewCheckpointID, "ckpt_"},
		{"CronID", id.NewCronID, "cron_"},
		{"DLQID", id.NewDLQID, "dlq_"},
		{"EventID", id.NewEventID, "evt_"},
		{"WorkerID", id.NewWorkerID, "wkr_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.newFn().String()
			if !strings.HasPrefix(got, tt.prefix) {
				t.Errorf("expected prefix %q, got %q", tt.prefix, got)
			}
		})
	}
}

func TestNew(t *testing.T) {
	i := id.New(id.PrefixJob)
	if i.IsNil() {
		t.Fatal("expected non-nil ID")
	}
	if i.Prefix() != id.PrefixJob {
		t.Errorf("expected prefix %q, got %q", id.PrefixJob, i.Prefix())
	}
}

func TestParseRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		newFn   func() id.ID
		parseFn func(string) (id.ID, error)
	}{
		{"JobID", id.NewJobID, id.ParseJobID},
		{"WorkflowID", id.NewWorkflowID, id.ParseWorkflowID},
		{"RunID", id.NewRunID, id.ParseRunID},
		{"CheckpointID", id.NewCheckpointID, id.ParseCheckpointID},
		{"CronID", id.NewCronID, id.ParseCronID},
		{"DLQID", id.NewDLQID, id.ParseDLQID},
		{"EventID", id.NewEventID, id.ParseEventID},
		{"WorkerID", id.NewWorkerID, id.ParseWorkerID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.newFn()
			parsed, err := tt.parseFn(original.String())
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if parsed.String() != original.String() {
				t.Errorf("round-trip mismatch: %q != %q", parsed.String(), original.String())
			}
		})
	}
}

func TestCrossTypeRejection(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		parseFn func(string) (id.ID, error)
	}{
		{"ParseJobID rejects wf_", id.NewWorkflowID().String(), id.ParseJobID},
		{"ParseWorkflowID rejects wfrun_", id.NewRunID().String(), id.ParseWorkflowID},
		{"ParseRunID rejects ckpt_", id.NewCheckpointID().String(), id.ParseRunID},
		{"ParseCheckpointID rejects cron_", id.NewCronID().String(), id.ParseCheckpointID},
		{"ParseCronID rejects dlq_", id.NewDLQID().String(), id.ParseCronID},
		{"ParseDLQID rejects evt_", id.NewEventID().String(), id.ParseDLQID},
		{"ParseEventID rejects wkr_", id.NewWorkerID().String(), id.ParseEventID},
		{"ParseWorkerID rejects job_", id.NewJobID().String(), id.ParseWorkerID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.parseFn(tt.input)
			if err == nil {
				t.Errorf("expected error for cross-type parse of %q, got nil", tt.input)
			}
		})
	}
}

func TestParseAny(t *testing.T) {
	ids := []id.ID{
		id.NewJobID(),
		id.NewWorkflowID(),
		id.NewRunID(),
		id.NewCheckpointID(),
		id.NewCronID(),
		id.NewDLQID(),
		id.NewEventID(),
		id.NewWorkerID(),
	}

	for _, i := range ids {
		t.Run(i.String(), func(t *testing.T) {
			parsed, err := id.ParseAny(i.String())
			if err != nil {
				t.Fatalf("ParseAny(%q) failed: %v", i.String(), err)
			}
			if parsed.String() != i.String() {
				t.Errorf("round-trip mismatch: %q != %q", parsed.String(), i.String())
			}
		})
	}
}

func TestParseWithPrefix(t *testing.T) {
	i := id.NewJobID()
	parsed, err := id.ParseWithPrefix(i.String(), id.PrefixJob)
	if err != nil {
		t.Fatalf("ParseWithPrefix failed: %v", err)
	}
	if parsed.String() != i.String() {
		t.Errorf("mismatch: %q != %q", parsed.String(), i.String())
	}

	_, err = id.ParseWithPrefix(i.String(), id.PrefixWorkflow)
	if err == nil {
		t.Error("expected error for wrong prefix")
	}
}

func TestParseEmpty(t *testing.T) {
	_, err := id.Parse("")
	if err == nil {
		t.Error("expected error for empty string")
	}
}

func TestNilID(t *testing.T) {
	var i id.ID
	if !i.IsNil() {
		t.Error("zero-value ID should be nil")
	}
	if i.String() != "" {
		t.Errorf("expected empty string, got %q", i.String())
	}
	if i.Prefix() != "" {
		t.Errorf("expected empty prefix, got %q", i.Prefix())
	}
}

func TestMarshalUnmarshalText(t *testing.T) {
	original := id.NewJobID()
	data, err := original.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText failed: %v", err)
	}

	var restored id.ID
	if unmarshalErr := restored.UnmarshalText(data); unmarshalErr != nil {
		t.Fatalf("UnmarshalText failed: %v", unmarshalErr)
	}
	if restored.String() != original.String() {
		t.Errorf("mismatch: %q != %q", restored.String(), original.String())
	}

	// Nil round-trip.
	var nilID id.ID
	data, err = nilID.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText(nil) failed: %v", err)
	}
	var restored2 id.ID
	if err := restored2.UnmarshalText(data); err != nil {
		t.Fatalf("UnmarshalText(nil) failed: %v", err)
	}
	if !restored2.IsNil() {
		t.Error("expected nil after round-trip of nil ID")
	}
}

func TestValueScan(t *testing.T) {
	original := id.NewWorkflowID()
	val, err := original.Value()
	if err != nil {
		t.Fatalf("Value failed: %v", err)
	}

	var scanned id.ID
	if scanErr := scanned.Scan(val); scanErr != nil {
		t.Fatalf("Scan failed: %v", scanErr)
	}
	if scanned.String() != original.String() {
		t.Errorf("mismatch: %q != %q", scanned.String(), original.String())
	}

	// Nil round-trip.
	var nilID id.ID
	val, err = nilID.Value()
	if err != nil {
		t.Fatalf("Value(nil) failed: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil value for nil ID, got %v", val)
	}

	var scanned2 id.ID
	if err := scanned2.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) failed: %v", err)
	}
	if !scanned2.IsNil() {
		t.Error("expected nil after scan of nil")
	}
}

func TestUniqueness(t *testing.T) {
	a := id.NewJobID()
	b := id.NewJobID()
	if a.String() == b.String() {
		t.Errorf("two consecutive NewJobID() calls returned the same ID: %q", a.String())
	}
}
