package contract

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
)

type partialPurgeStore struct{ store.Store }

func (s partialPurgeStore) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	page, err := s.ListDLQPage(ctx, dlq.PageOpts{})
	if err != nil {
		return 0, err
	}
	for _, entry := range page.Entries {
		if entry.FailedAt.Before(before) {
			if deleteErr := s.DeleteDLQ(ctx, entry.ID); deleteErr != nil {
				return 0, deleteErr
			}
			return 1, errors.New("private Redis pipeline diagnostic")
		}
	}
	return 0, errors.New("private Redis pipeline diagnostic")
}

func TestDLQPartialPurgeRetainsCountAndInvalidations(t *testing.T) {
	recorder := &actionRecorder{}
	d := contractDeps(t, partialPurgeStore{Store: memory.New()}, engine.WithExtension(recorder))
	seedDLQ(t, d, "one", "mail", "", "", time.Now().Add(-time.Hour), false)
	seedDLQ(t, d, "two", "mail", "", "", time.Now().Add(-time.Hour), false)
	response := callContract(t, d, "command", "dlq.purge", BeforeInput{Before: time.Now().UTC().Format(time.RFC3339Nano)})
	var result struct {
		Count       int64 `json:"count"`
		Interrupted bool  `json:"interrupted"`
		Failure     *struct {
			Code string `json:"code"`
		} `json:"failure"`
	}
	if err := json.Unmarshal(response.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 || !result.Interrupted || result.Failure == nil || result.Failure.Code != "INTERNAL" || strings.Contains(string(response.Data), "private") {
		t.Fatalf("partial purge = %s", response.Data)
	}
	want := []string{"dlq.list", "dlq.get", "dlq.counts", "dlq.purgePreview", "overview.summary"}
	if !reflect.DeepEqual(response.Meta.Invalidates, want) {
		t.Fatalf("invalidates = %v", response.Meta.Invalidates)
	}
	left, err := d.Store.CountDLQ(context.Background())
	if err != nil || left != 1 {
		t.Fatalf("remaining = %d, %v", left, err)
	}
	if len(recorder.actions) != 1 || recorder.actions[0].Kind != ext.ActionDLQPurged || recorder.actions[0].Count != 1 {
		t.Fatalf("actions = %+v", recorder.actions)
	}
}

func TestInterruptedDLQEngineOperationsEmitCommittedCount(t *testing.T) {
	for _, operation := range []string{"replay", "purge"} {
		t.Run(operation, func(t *testing.T) {
			var s store.Store = partialPurgeStore{Store: memory.New()}
			if operation == "replay" {
				s = &replayFailureStore{Store: memory.New(), interruptListing: true}
			}
			recorder := &actionRecorder{}
			d := contractDeps(t, s, engine.WithExtension(recorder))
			seedDLQ(t, d, "one", "mail", "", "", time.Now().Add(-time.Hour), false)
			seedDLQ(t, d, "two", "mail", "", "", time.Now().Add(-time.Hour), false)
			ctx := ext.WithActor(context.Background(), "operator")
			kind := ext.ActionDLQPurged
			var n int64
			var err error
			if operation == "replay" {
				var result engine.ReplayAllResult
				result, err = d.Engine.ReplayAllDLQ(ctx, engine.ReplayAllOpts{})
				n = int64(result.Replayed)
				kind = ext.ActionDLQReplayed
			} else {
				n, err = d.Engine.PurgeDLQ(ctx, time.Now())
			}
			if n != 1 || err == nil {
				t.Errorf("committed count = %d, error = %v", n, err)
			}
			if len(recorder.actions) != 1 || recorder.actions[0].Kind != kind || recorder.actions[0].Count != 1 || recorder.actions[0].Actor != "operator" {
				t.Fatalf("actions = %+v", recorder.actions)
			}
		})
	}
}
