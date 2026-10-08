package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/transport"

	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/store/memory"
)

func callContract(t *testing.T, deps Deps, kind, intent string, payload any) fc.Response {
	t.Helper()
	reg := fc.NewRegistry()
	wreg := fc.NewWardenRegistry()
	d := dispatcher.New(nil)
	if err := Register(d, reg, wreg, deps); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"envelope": "v1", "kind": kind, "contributor": "dispatch", "intent": intent, "csrf": "test", "idempotencyKey": "test-" + intent, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/dashboard/v1", bytes.NewReader(body))
	transport.NewHandler(reg, wreg, d, nil).ServeHTTP(recorder, req)
	var response fc.Response
	if decodeErr := json.Unmarshal(recorder.Body.Bytes(), &response); decodeErr != nil || !response.OK {
		t.Fatalf("response=%s, error=%v", recorder.Body, decodeErr)
	}
	return response
}
func TestDLQTransportPersistsEveryCommandAndInvalidates(t *testing.T) {
	for _, intent := range []string{"dlq.replay", "dlq.replayAll", "dlq.delete", "dlq.purge"} {
		t.Run(intent, func(t *testing.T) {
			d := contractDeps(t, memory.New())
			entry := seedDLQ(t, d, "mail", "mail", "", "", time.Now().Add(-time.Hour), false)
			var payload any = IDInput{ID: entry.ID.String()}
			if intent == "dlq.replayAll" {
				payload = DLQReplayAllInput{Queue: "mail", Limit: 1}
			}
			if intent == "dlq.purge" {
				payload = BeforeInput{Before: time.Now().UTC().Format(time.RFC3339Nano)}
			}
			response := callContract(t, d, "command", intent, payload)
			var want []string
			for _, declared := range loadManifest(t).Intents {
				if declared.Name == intent {
					want = declared.Invalidates
				}
			}
			if !reflect.DeepEqual(response.Meta.Invalidates, want) {
				t.Fatalf("invalidates=%v, want %v", response.Meta.Invalidates, want)
			}
			if intent == "dlq.replay" || intent == "dlq.replayAll" {
				stored, err := d.Store.GetDLQ(context.Background(), entry.ID)
				if err != nil || stored.ReplayedAt == nil || stored.ReplayedJobID == nil {
					t.Fatalf("entry=%+v, %v", stored, err)
				}
			} else {
				count, err := d.Store.CountDLQEntries(context.Background(), dlq.CountOpts{})
				if err != nil || count != 0 {
					t.Fatalf("remaining=%d, %v", count, err)
				}
			}
		})
	}
}
func TestDLQPartialTransportKeepsInvalidations(t *testing.T) {
	s := &replayFailureStore{Store: memory.New(), interruptListing: true}
	d := contractDeps(t, s)
	seedDLQ(t, d, "one", "mail", "", "", time.Now(), false)
	seedDLQ(t, d, "two", "mail", "", "", time.Now(), false)
	response := callContract(t, d, "command", "dlq.replayAll", DLQReplayAllInput{})
	var result DLQBulkResult
	if err := json.Unmarshal(response.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Replayed != 1 || !result.Interrupted || len(response.Meta.Invalidates) == 0 {
		t.Fatalf("response=%+v, result=%+v", response, result)
	}
}
func TestDLQQueriesAreRegistered(t *testing.T) {
	d := contractDeps(t, memory.New())
	e := seedDLQ(t, d, "one", "mail", "", "", time.Now().Add(-time.Hour), false)
	for intent, payload := range map[string]any{"dlq.list": DLQListInput{}, "dlq.get": IDInput{ID: e.ID.String()},
		"dlq.counts": DLQCountsInput{}, "dlq.purgePreview": BeforeInput{Before: time.Now().UTC().Format(time.RFC3339Nano)}} {
		t.Run(intent, func(t *testing.T) { callContract(t, d, "query", intent, payload) })
	}
}
