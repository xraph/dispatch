package relayhook_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	revent "github.com/xraph/relay/event"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	rh "github.com/xraph/dispatch/relay_hook"
)

// eventData round-trips an event's Data through JSON so a test sees the
// payload the way a webhook receiver would.
func eventData(t *testing.T, evt *revent.Event) map[string]any {
	t.Helper()
	raw, err := json.Marshal(evt.Data)
	if err != nil {
		t.Fatalf("marshal event data: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}
	return out
}

func TestRelayHookExtension_JobCancelled(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)
	j := newTestJob()

	if err := h.OnJobCancelled(context.Background(), j); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventJobCancelled)
	if evt.TenantID != "org-1" {
		t.Errorf("TenantID: want %q, got %q", "org-1", evt.TenantID)
	}
	data := eventData(t, evt)
	if data["job_id"] != j.ID.String() {
		t.Errorf("data.job_id: want %q, got %v", j.ID.String(), data["job_id"])
	}
}

func TestRelayHookExtension_OperatorJobCancelled(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)
	jobID := id.NewJobID()
	at := time.Date(2026, 10, 7, 12, 30, 0, 0, time.UTC)

	err := h.OnOperatorAction(context.Background(), ext.Action{
		Kind:  ext.ActionJobCancelled,
		Actor: "user_42",
		JobID: jobID,
		At:    at,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evt := lastEvent(t, r, rh.EventOperatorJobCancelled)
	// Operator actions are system-level, no tenant.
	if evt.TenantID != "" {
		t.Errorf("TenantID: want empty, got %q", evt.TenantID)
	}
	data := eventData(t, evt)
	want := map[string]any{
		"kind":   "job.cancelled",
		"actor":  "user_42",
		"job_id": jobID.String(),
		"at":     "2026-10-07T12:30:00Z",
	}
	for k, v := range want {
		if data[k] != v {
			t.Errorf("data.%s: want %v, got %v", k, v, data[k])
		}
	}
	for _, absent := range []string{"new_job_id", "dlq_id", "cron_id", "run_id", "step", "count"} {
		if v, ok := data[absent]; ok {
			t.Errorf("data.%s: want absent for a cancel, got %v", absent, v)
		}
	}
}

func TestRelayHookExtension_OperatorActionsUseRegisteredTypes(t *testing.T) {
	cases := map[ext.ActionKind]string{
		ext.ActionJobCancelled:     rh.EventOperatorJobCancelled,
		ext.ActionJobRetried:       rh.EventOperatorJobRetried,
		ext.ActionDLQReplayed:      rh.EventOperatorDLQReplayed,
		ext.ActionDLQDeleted:       rh.EventOperatorDLQDeleted,
		ext.ActionDLQPurged:        rh.EventOperatorDLQPurged,
		ext.ActionCronEnabled:      rh.EventOperatorCronEnabled,
		ext.ActionCronDisabled:     rh.EventOperatorCronDisabled,
		ext.ActionCronDeleted:      rh.EventOperatorCronDeleted,
		ext.ActionCronTriggered:    rh.EventOperatorCronTriggered,
		ext.ActionWorkflowReplayed: rh.EventOperatorWorkflowReplayed,
	}

	r := newTestRelay(t)
	h := rh.New(r)
	ctx := context.Background()

	for kind, eventType := range cases {
		if err := h.OnOperatorAction(ctx, ext.Action{Kind: kind, At: time.Now().UTC()}); err != nil {
			t.Errorf("%s: unexpected error: %v", kind, err)
			continue
		}
		evt := lastEvent(t, r, eventType)
		if got := eventData(t, evt)["kind"]; got != string(kind) {
			t.Errorf("%s: data.kind = %v", eventType, got)
		}
	}
}

func TestRelayHookExtension_OperatorActionUnknownKindErrors(t *testing.T) {
	r := newTestRelay(t)
	h := rh.New(r)

	// No catalog entry exists for a kind this version does not know, so
	// Relay refuses it and the registry logs the error.
	if err := h.OnOperatorAction(context.Background(), ext.Action{Kind: "queue.paused"}); err == nil {
		t.Fatal("want an error for an unregistered operator event type")
	}
}

func TestRelayHookExtension_OperatorActionViaRegistryTakesActorFromContext(t *testing.T) {
	r := newTestRelay(t)
	reg := ext.NewRegistry(log.NewNoopLogger())
	reg.Register(rh.New(r))

	ctx := ext.WithActor(context.Background(), "user_9")
	reg.EmitOperatorAction(ctx, ext.Action{Kind: ext.ActionDLQPurged, Count: 5})

	data := eventData(t, lastEvent(t, r, rh.EventOperatorDLQPurged))
	if data["actor"] != "user_9" {
		t.Errorf("data.actor: want %q, got %v", "user_9", data["actor"])
	}
	// JSON numbers decode as float64.
	if data["count"] != float64(5) {
		t.Errorf("data.count: want 5, got %v", data["count"])
	}
	if at, ok := data["at"].(string); !ok || at == "" {
		t.Errorf("data.at: want the registry's default time, got %v", data["at"])
	}
}
