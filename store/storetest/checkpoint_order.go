package storetest

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/workflow"
)

// RunCheckpointOrderSuite checks the shared timestamp/ID order against actual
// persisted rows. setTime forces ties without depending on a backend's clock.
func RunCheckpointOrderSuite(t *testing.T, s workflow.Store, setTime func(context.Context, id.RunID, string, time.Time) error) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "checkpoint-order", State: workflow.RunStateCompleted, StartedAt: at}
	other := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "other-run", State: workflow.RunStateCompleted, StartedAt: at}
	for _, r := range []*workflow.Run{run, other} {
		if err := s.CreateRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	names := []string{"tie-a", "tie-b", "tie-c", "earlier", "later"}
	for _, name := range names {
		if err := s.SaveCheckpoint(ctx, run.ID, name, []byte(name)); err != nil {
			t.Fatal(err)
		}
		when := at
		if name == "earlier" {
			when = at.Add(-time.Hour)
		}
		if name == "later" {
			when = at.Add(time.Hour)
		}
		if err := setTime(ctx, run.ID, name, when); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SaveCheckpoint(ctx, other.ID, "untouched", []byte("other")); err != nil {
		t.Fatal(err)
	}
	if err := setTime(ctx, other.ID, "untouched", at.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ListCheckpoints(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("fixture rows = %d", len(rows))
	}
	tied := make([]*workflow.Checkpoint, 0, 3)
	for _, cp := range rows {
		if strings.HasPrefix(cp.StepName, "tie-") {
			if !cp.CreatedAt.Equal(at) {
				t.Fatalf("fixture did not persist a tie: %+v", cp)
			}
			tied = append(tied, cp)
		}
	}
	if len(tied) != 3 {
		t.Fatalf("fixture ties = %d", len(tied))
	}
	slices.SortFunc(tied, func(a, b *workflow.Checkpoint) int { return strings.Compare(a.ID.String(), b.ID.String()) })
	want := []string{"earlier", tied[0].StepName, tied[1].StepName, tied[2].StepName, "later"}
	actual := make([]string, 0, len(rows))
	for _, cp := range rows {
		actual = append(actual, cp.StepName)
	}
	if !reflect.DeepEqual(actual, want) {
		t.Errorf("list order = %v, want %v", actual, want)
	}
	if deleteErr := s.DeleteCheckpointsAfter(ctx, run.ID, "missing"); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	unchanged, err := s.ListCheckpoints(ctx, run.ID)
	if err != nil || len(unchanged) != 5 {
		t.Fatalf("missing target changed rows: %v, %v", unchanged, err)
	}
	if deleteErr := s.DeleteCheckpointsAfter(ctx, run.ID, tied[1].StepName); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	remaining, err := s.ListCheckpoints(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	actual = make([]string, 0, len(remaining))
	for _, cp := range remaining {
		actual = append(actual, cp.StepName)
	}
	if !reflect.DeepEqual(actual, want[:3]) {
		t.Fatalf("remaining = %v, want %v", actual, want[:3])
	}
	untouched, err := s.ListCheckpoints(ctx, other.ID)
	if err != nil || len(untouched) != 1 || untouched[0].StepName != "untouched" {
		t.Fatalf("other run changed: %v, %v", untouched, err)
	}
}
