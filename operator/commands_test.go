package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

func commandFixture(t *testing.T) (*Service, StartInput) {
	t.Helper()
	s, store, _ := fixture(t)
	worker, err := drt.NewWorker(store, drt.Options{Namespace: "allowed", BuildID: "historic", Queue: "queue", Owner: "operator", Workflows: map[string]drt.WorkflowFunc{"workflow": func(w *drt.Workflow, input []byte) ([]byte, error) {
		w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
		w.SetQueryHandler("mutate", func([]byte) ([]byte, error) { w.Activity("forbidden", "forbidden", "", nil); return nil, nil })
		return nil, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = func(ns, build string) (*drt.Worker, error) {
		if ns != "allowed" || build != "historic" {
			return nil, ErrRuntimeUnavailable
		}
		return worker, nil
	}
	return s, StartInput{Key: durable.Key{Namespace: "allowed", WorkflowID: "workflow", RunID: "run"}, RequestID: "start", WorkflowType: "workflow", BuildID: "historic", Queue: "queue", Input: []byte(`{"large":9007199254740993}`)}
}
func TestCommandReceiptsReauthorizeAndPreserveBytes(t *testing.T) {
	s, start := commandFixture(t)
	first, err := s.Start(t.Context(), reader(), start)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Start(t.Context(), reader(), start)
	if err != nil || again != first {
		t.Fatalf("retry: %+v %v", again, err)
	}
	changed := start
	changed.Input = []byte("changed")
	if _, err = s.Start(t.Context(), reader(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("conflict: %v", err)
	}
	query, err := s.Query(t.Context(), reader(), drt.QueryRequest{Key: start.Key, BuildID: start.BuildID, Name: "status"})
	if err != nil || !bytes.Equal(query.Output, start.Input) || query.Revision != "1" {
		t.Fatalf("query: %+v %v", query, err)
	}
	if _, err = s.Query(t.Context(), reader(), drt.QueryRequest{Key: start.Key, BuildID: start.BuildID, Name: "mutate"}); !errors.Is(err, drt.ErrQueryMutation) {
		t.Fatalf("mutation: %v", err)
	}
	signal := durable.SignalRequest{Key: start.Key, RequestID: "signal", BuildID: start.BuildID, Name: "message", Input: start.Input}
	acceptedSignal, err := s.Signal(t.Context(), reader(), signal)
	if err != nil {
		t.Fatal(err)
	}
	signalAgain, err := s.Signal(t.Context(), reader(), signal)
	if err != nil || signalAgain != acceptedSignal {
		t.Fatalf("signal retry: %+v %v", signalAgain, err)
	}
	signal.RunID = ""
	if _, err = s.Signal(t.Context(), reader(), signal); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("selector accepted: %v", err)
	}
	cancel := durable.CancelExecutionRequest{Key: start.Key, RequestID: "cancel", BuildID: start.BuildID}
	receipt, err := s.Cancel(t.Context(), reader(), cancel)
	if err != nil || receipt.Status != "cancellation_requested" {
		t.Fatalf("cancel: %+v %v", receipt, err)
	}
	execution, err := s.store.GetExecution(t.Context(), start.Key)
	if err != nil || execution.State != durable.StateRunning {
		t.Fatalf("cancel changed terminal state: %+v %v", execution, err)
	}
	s.authorizer = AuthorizerFunc(func(context.Context, security.Principal, string, Resource) error { return security.ErrForbidden })
	if _, err = s.Start(t.Context(), reader(), start); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("revoked retry: %v", err)
	}
	if _, err = s.Cancel(t.Context(), reader(), cancel); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("revoked cancel: %v", err)
	}
}
func TestCommandRuntimeAndTrustedFacts(t *testing.T) {
	s, start := commandFixture(t)
	if _, err := s.Start(t.Context(), reader(), start); err != nil {
		t.Fatal(err)
	}
	s.authorizer = AuthorizerFunc(func(_ context.Context, _ security.Principal, _ string, r Resource) error {
		if r.AppID != "app-allowed" || r.TenantID != "tenant-allowed" || r.WorkflowType != "workflow" || r.BuildID != "historic" {
			t.Fatalf("untrusted resource: %+v", r)
		}
		return nil
	})
	signal := durable.SignalRequest{Key: start.Key, RequestID: "signal", BuildID: "new-build", Name: "message"}
	if _, err := s.Signal(t.Context(), reader(), signal); !errors.Is(err, ErrBuildMismatch) {
		t.Fatalf("build: %v", err)
	}
	signal.BuildID = start.BuildID
	s.runtime = nil
	if _, err := s.Signal(t.Context(), reader(), signal); !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("runtime: %v", err)
	}
}
func TestCallbackWireCountersAndHumanDenial(t *testing.T) {
	s, _ := commandFixture(t)
	in := HeartbeatInput{Handle: CallbackHandle{Token: CallbackToken{Epoch: 9007199254740993}, InitialHeartbeatSequence: 9007199254740993}, Sequence: 9007199254740993}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"epoch":"9007199254740993"`)) || !bytes.Contains(raw, []byte(`"sequence":"9007199254740993"`)) {
		t.Fatal(string(raw))
	}
	if _, err = s.Heartbeat(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("human callback: %v", err)
	}
	if err = json.Unmarshal([]byte(`{"sequence":9007199254740993}`), &in); err == nil {
		t.Fatal("numeric callback counter accepted")
	}
}
