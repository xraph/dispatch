package contract

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/store/memory"
)

func TestCronTransportPersistsCommandsAndInvalidates(t *testing.T) {
	for _, intent := range []string{"crons.enable", "crons.disable", "crons.runNow", "crons.delete"} {
		t.Run(intent, func(t *testing.T) {
			d := contractDeps(t, memory.New())
			e := seedCron(t, d, "daily", "0 9 * * *", intent == "crons.disable")
			response := callContract(t, d, "command", intent, IDInput{ID: e.ID.String()})
			var want []string
			for _, declared := range loadManifest(t).Intents {
				if declared.Name == intent {
					want = declared.Invalidates
				}
			}
			if !reflect.DeepEqual(response.Meta.Invalidates, want) {
				t.Fatalf("invalidates=%v, want %v", response.Meta.Invalidates, want)
			}
			stored, err := d.Store.GetCron(context.Background(), e.ID)
			switch intent {
			case "crons.delete":
				if !errors.Is(err, dispatch.ErrCronNotFound) {
					t.Fatalf("delete = %v", err)
				}
			case "crons.runNow":
				if err != nil || stored.Enabled || !reflect.DeepEqual(stored.NextRunAt, e.NextRunAt) {
					t.Fatalf("run now schedule = %+v, %v", stored, err)
				}
				counts, countErr := countJobs(context.Background(), d, "mail")
				if countErr != nil || counts.Total != 1 {
					t.Fatalf("run now count = %+v, %v", counts, countErr)
				}
			default:
				if err != nil || stored.Enabled != (intent == "crons.enable") {
					t.Fatalf("toggle = %+v, %v", stored, err)
				}
			}
		})
	}
}
func TestCronQueriesAreRegistered(t *testing.T) {
	d := contractDeps(t, memory.New())
	e := seedCron(t, d, "daily", "0 9 * * *", true)
	callContract(t, d, "query", "crons.list", EmptyInput{})
	callContract(t, d, "query", "crons.get", IDInput{ID: e.ID.String()})
	// A preview is a read: it does not move the stored schedule.
	stored, err := d.Store.GetCron(context.Background(), e.ID)
	if err != nil || !stored.NextRunAt.Equal(*e.NextRunAt) || !stored.NextRunAt.Before(time.Now()) {
		t.Fatalf("read changed schedule = %+v, %v", stored, err)
	}
}
