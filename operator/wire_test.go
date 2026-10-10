package operator

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/durable"
)

func TestWireFixtures(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	s := &Service{}
	execution := s.project(durable.Execution{Key: durable.Key{Namespace: "production", WorkflowID: "invoice-42", RunID: "run-2"}, WorkflowType: "invoice", BuildID: "historical-build", State: durable.StateRunning, Revision: 9007199254740993, LastSequence: 9007199254740994, RunNumber: 9007199254740995, RetryAttempt: 1, CreatedAt: now, UpdatedAt: now, RunAvailableAt: now})
	examples := map[string]any{
		"discovery_incomplete_empty": Page[Namespace]{Items: []Namespace{}, Cursor: "v1.OPAQUE_CONTINUATION_EXAMPLE", Complete: false, AsOf: now, Observation: "bounded_catalog_scan"},
		"discovery_complete":         Page[Namespace]{Items: []Namespace{{Namespace: "production", AppID: "billing", TenantID: "acme"}}, Complete: true, AsOf: now, Observation: "bounded_catalog_scan"},
		"executions":                 Page[Execution]{Items: []Execution{execution}, Complete: true, AsOf: now, Observation: "current_page"},
		"detail":                     Detail{Execution: execution, Links: []Link{{Kind: "previous", Namespace: "production", WorkflowID: "invoice-42", RunID: "run-1"}}, LinksRestricted: true, AsOf: now},
		"history":                    Page[Event]{Items: []Event{{Type: "execution.started", Sequence: "9007199254740994", Time: now}}, Complete: true, AsOf: now, Observation: "captured_history_high_water", Revision: "9007199254740993", HighWater: "9007199254740994"},
		"tasks":                      Page[Task]{Items: []Task{{ID: "activity:payment", Kind: durable.TaskActivity, State: "awaiting_callback", Attempt: "9007199254740993", Version: "9007199254740994", AvailableAt: now, LeaseUntil: &now}}, Complete: true, AsOf: now, Observation: "current_page", Revision: "9007199254740993"},
		"chain_partial":              Chain{Page: Page[Execution]{Items: []Execution{execution}, Complete: false, AsOf: now, Observation: "current_page"}, Restricted: true},
		"children_restricted":        Children{Page: Page[Link]{Items: []Link{}, Complete: true, AsOf: now, Observation: "current_page"}, Restricted: true},
		"deliveries_blocked":         Deliveries{Page: Page[Delivery]{Items: []Delivery{{ID: "source-record", State: "blocked", Attempts: "9007199254740993", AcceptedAt: now}}, Complete: true, AsOf: now, Observation: "current_page"}, Pending: "2", Blocked: "1", RemoteDelivery: "unavailable", ExternalAnchoring: "unavailable"},
		"payload_reveal":             Payload{State: "revealed", Encoding: "base64", Input: []byte(`{"invoice":42}`), Output: nil, Revision: "9007199254740993"},
	}

	deferral := TaskDeferral{Active: true, Reason: "target_retiring", TargetBuildID: "next-build", TargetState: durable.DeferralRetiring, TargetRetirementEpoch: "9007199254740993", SourceEpoch: "9007199254740994", SourceRevision: "9007199254740995", TaskVersion: "9007199254740996", Count: "2", ReferenceKind: durable.DeferralChild, CommandID: "child:payment", RecordedAt: now, RetryAt: now.Add(2 * time.Second), PolicyVersion: "1"}
	examples["tasks_deferred"] = Page[Task]{Items: []Task{{ID: "workflow:1", Kind: durable.TaskWorkflow, State: "pending", Attempt: "2", Version: deferral.TaskVersion, AvailableAt: deferral.RetryAt, Deferral: &deferral}}, Complete: true, AsOf: now, Observation: "current_page", Revision: deferral.SourceRevision}
	retained := deferral
	retained.Active = false
	examples["tasks_deferral_retained"] = Page[Task]{Items: []Task{{ID: "workflow:1", Kind: durable.TaskWorkflow, State: "leased", Attempt: "3", Version: "9007199254740997", AvailableAt: deferral.RetryAt, LeaseUntil: &now, Deferral: &retained}}, Complete: true, AsOf: now, Observation: "current_page", Revision: deferral.SourceRevision}
	envelopes := map[string]any{}
	for name, data := range examples {
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		envelopes[name] = fc.Response{OK: true, Envelope: "v1", Kind: fc.KindQuery, Data: raw, Meta: fc.ResponseMeta{IntentVersion: 1}}
	}
	for name, err := range map[string]*fc.Error{"unauthenticated": fc.ErrUnauthenticated, "forbidden": fc.ErrPermissionDenied, "unavailable": fc.ErrUnavailable, "invalid_cursor": fc.ErrBadRequest, "not_found": fc.ErrNotFound} {
		envelopes[name] = fc.ErrorResponse{Envelope: "v1", Error: err}
	}
	actual, err := json.MarshalIndent(envelopes, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, '\n')
	const path = "testdata/durable-wire.json"
	if os.Getenv("UPDATE_DURABLE_WIRE") == "1" {
		if err = os.WriteFile(path, actual, 0600); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(expected) != string(actual) {
		t.Fatal("wire fixture differs from Go serialization; review DTO changes")
	}
}
