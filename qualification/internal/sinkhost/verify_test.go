package sinkhost

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	ca "github.com/xraph/chronicle/acceptance"
	ra "github.com/xraph/relay/acceptance"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func TestAcceptanceMapping(t *testing.T) {
	b := ecosystem.Binding{Producer: "dispatch", InstallationID: "install", Namespace: "ns", AppID: "app", OrgID: "org", TenantID: "tenant"}
	for _, dest := range []durable.Destination{durable.DestinationChronicle, durable.DestinationRelay} {
		d, err := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: b.InstallationID, Namespace: b.Namespace, AppID: b.AppID, TenantID: b.TenantID, SchemaVersion: 1}}, dest, durable.DeliverySource{Key: durable.Key{Namespace: b.Namespace, WorkflowID: "wf", RunID: "run"}, Kind: "event", ID: "90071992547409930", Sequence: 90071992547409930, OccurredAt: time.Date(2026, 10, 9, 1, 2, 3, 123456000, time.UTC), Action: "execution.completed", Outcome: "accepted", Metadata: durable.AuditMetadata{ActorKind: "user", ActorID: "user", RequestID: "request", ReasonCode: "reason"}})
		if err != nil {
			t.Fatal(err)
		}
		switch dest {
		case durable.DestinationChronicle:
			req, e := ecosystem.ChronicleRequest(b, d)
			if e != nil {
				t.Fatal(e)
			}
			raw, e := json.Marshal(req)
			if e != nil {
				t.Fatal(e)
			}
			var decoded ca.Request
			if e = decode(httptest.NewRequestWithContext(t.Context(), "POST", "/accept", bytes.NewReader(raw)), &decoded); e != nil {
				t.Fatal(e)
			}
			if e = VerifyChronicle(b, decoded); e != nil {
				t.Fatal(e)
			}
			decoded.Event.Metadata["source_sequence"] = json.Number("90071992547409920")
			if VerifyChronicle(b, decoded) == nil {
				t.Fatal("changed large sequence accepted")
			}
			decoded.Event.Metadata["source_sequence"] = json.Number("90071992547409930")
			decoded.Event.UserID = "other"
			if VerifyChronicle(b, decoded) == nil {
				t.Fatal("changed semantic actor accepted")
			}
		case durable.DestinationRelay:
			req, e := ecosystem.RelayRequest(b, d)
			if e != nil {
				t.Fatal(e)
			}
			if e = VerifyRelay(b, req); e != nil {
				t.Fatal(e)
			}
			req.OrgID = "other"
			if VerifyRelay(b, req) == nil {
				t.Fatal("changed scope accepted")
			}
		}
	}
	for _, body := range []string{`{"SourceKey":"a","SourceKey":"b"}`, `{} {}`, `{"unknown":true}`, strings.Repeat(" ", 65537)} {
		var req ra.Request
		if decode(httptest.NewRequestWithContext(t.Context(), "POST", "/accept", strings.NewReader(body)), &req) == nil {
			t.Fatal("ambiguous or unbounded body accepted")
		}
	}
}

func TestRelayExactSequence(t *testing.T) {
	b := ecosystem.Binding{Producer: "dispatch", InstallationID: "install", Namespace: "ns", AppID: "app", OrgID: "org", TenantID: "tenant"}
	for _, sequence := range []int64{10, 100, 90071992547409930, math.MaxInt64} {
		t.Run(strconv.FormatInt(sequence, 10), func(t *testing.T) {
			d, err := durable.NewDelivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: b.InstallationID, Namespace: b.Namespace, AppID: b.AppID, TenantID: b.TenantID, SchemaVersion: 1}}, durable.DestinationRelay, durable.DeliverySource{Key: durable.Key{Namespace: b.Namespace, WorkflowID: "wf", RunID: "run"}, Kind: "event", ID: strconv.FormatInt(sequence, 10), Sequence: sequence, OccurredAt: time.Date(2026, 10, 9, 1, 2, 3, 123456000, time.UTC), Action: "execution.completed", Outcome: "accepted", Metadata: durable.AuditMetadata{ActorKind: "user", ActorID: "user"}})
			if err != nil {
				t.Fatal(err)
			}
			req, err := ecosystem.RelayRequest(b, d)
			if err != nil {
				t.Fatal(err)
			}
			if err = VerifyRelay(b, req); err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err = json.Unmarshal(req.Data, &fields); err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []string{`1.5`, `9223372036854775808`, `-1`, `"10"`, `null`} {
				fields["Sequence"] = json.RawMessage(invalid)
				req.Data, err = json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if VerifyRelay(b, req) == nil {
					t.Fatalf("accepted invalid sequence %s", invalid)
				}
			}
		})
	}
}
