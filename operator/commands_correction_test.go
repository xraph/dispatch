package operator

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

func TestCommandPreservesTrustedAuditFacts(t *testing.T) {
	s, start := commandFixture(t)
	metadata := durable.AuditMetadata{ActorKind: "service", ActorID: "obsolete", RequestID: "obsolete", CorrelationID: "correlation", DecisionID: "decision", PolicyVersion: "policy-v3", ReasonCode: "approved"}
	if _, err := s.Start(durable.WithAuditMetadata(t.Context(), metadata), reader(), start); err != nil {
		t.Fatal(err)
	}
	outbox, ok := s.store.(durable.OutboxStore)
	if !ok {
		t.Fatal("missing outbox")
	}
	status, err := outbox.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "install", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	metadata.ActorKind = "user"
	metadata.ActorID = "reader"
	metadata.RequestID = start.RequestID
	count := 0
	for _, record := range status.Records {
		if record.Delivery.Namespace == start.Namespace {
			count++
			if record.Delivery.Metadata != metadata {
				t.Fatalf("committed audit facts: %+v", record.Delivery.Metadata)
			}
		}
	}
	if count < 2 {
		t.Fatalf("expected event and receipt intents, got %d", count)
	}
}
func TestProposedStartInputBounds(t *testing.T) {
	for _, signalStart := range []bool{false, true} {
		for _, size := range []int{1 << 20, (1 << 20) + 1} {
			t.Run(map[bool]string{false: "start", true: "signal-start"}[signalStart]+map[bool]string{false: "-limit", true: "-oversized"}[size > 1<<20], func(t *testing.T) {
				s, start := commandFixture(t)
				start.Input = bytes.Repeat([]byte("x"), size)
				var err error
				if signalStart {
					_, err = s.SignalStart(t.Context(), reader(), SignalStartInput{Start: start, Name: "signal"})
				} else {
					_, err = s.Start(t.Context(), reader(), start)
				}
				if size <= 1<<20 {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if !errors.Is(err, durable.ErrInvalid) {
					t.Fatalf("oversized accepted: %v", err)
				}
				if _, err = s.store.GetExecution(t.Context(), start.Key); !errors.Is(err, durable.ErrNotFound) {
					t.Fatal("oversized start mutated", err)
				}
				outbox, ok := s.store.(durable.OutboxStore)
				if !ok {
					t.Fatal("missing outbox")
				}
				status, err := outbox.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "install", Destination: durable.DestinationChronicle}, Limit: 100})
				if err != nil || len(status.Records) != 0 {
					t.Fatalf("oversized start accepted audit: %+v %v", status, err)
				}
			})
		}
	}
}
func TestRuntimeResolverOwnsMetadataProjection(t *testing.T) {
	for _, mode := range []string{"exact", "missing", "wrong-build", "wrong-namespace"} {
		t.Run(mode, func(t *testing.T) {
			prior, store, _ := fixture(t)
			key := seed(t, store, "allowed", "workflow")
			namespace, build := "allowed", "historic"
			if mode == "wrong-build" {
				build = "active"
			}
			if mode == "wrong-namespace" {
				namespace = "foreign"
			}
			worker, err := drt.NewWorker(store, drt.Options{Namespace: namespace, BuildID: build, Queue: "queue", Owner: "worker"})
			if err != nil {
				t.Fatal(err)
			}
			resolve := func(string, string) (*drt.Worker, error) {
				if mode == "missing" {
					return nil, errors.New("internal runtime details")
				}
				return worker, nil
			}
			service, err := New(Options{Store: store, Reads: store, Catalog: store, InstallationID: "install", Authorizer: prior.authorizer, Audit: prior.audit, CursorKeys: CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Runtime: resolve, RuntimeAvailable: func(string, string) bool { return mode != "exact" }})
			if err != nil {
				t.Fatal(err)
			}
			detail, err := service.Detail(t.Context(), reader(), key)
			if err != nil {
				t.Fatal(err)
			}
			capabilities, err := service.Capabilities(t.Context(), reader(), CapabilitiesInput{Key: key, BuildID: "historic"})
			if err != nil {
				t.Fatal(err)
			}
			want := "unavailable"
			if mode == "exact" {
				want = "available"
			}
			if detail.Runtime != want || capabilities.Runtime != want {
				t.Fatalf("metadata=%s capabilities=%s want=%s", detail.Runtime, capabilities.Runtime, want)
			}
		})
	}
}

type noSignalOutcomeStore struct{ durable.Store }

func TestSignalStartCapabilityRequiresAtomicOutcome(t *testing.T) {
	s, start := commandFixture(t)
	worker, err := drt.NewWorker(noSignalOutcomeStore{s.store}, drt.Options{Namespace: start.Namespace, BuildID: start.BuildID, Queue: start.Queue, Owner: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = func(string, string) (*drt.Worker, error) { return worker, nil }
	caps, err := s.Capabilities(t.Context(), reader(), CapabilitiesInput{Key: durable.Key{Namespace: start.Namespace, WorkflowID: start.WorkflowID}, BuildID: start.BuildID, WorkflowType: start.WorkflowType})
	if err != nil {
		t.Fatal(err)
	}
	if caps.Runtime != "available" || caps.Actions[SignalStartWorkflow] {
		t.Fatalf("unsupported capability: %+v", caps)
	}
	if _, err = s.SignalStart(t.Context(), reader(), SignalStartInput{Start: start, Name: "signal"}); !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatal(err)
	}
	if _, err = s.store.GetExecution(t.Context(), start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatal("unsupported runtime mutated", err)
	}
}
func TestSignalStartAtomicBuildMismatch(t *testing.T) {
	s, start := commandFixture(t)
	if _, err := s.Start(t.Context(), reader(), start); err != nil {
		t.Fatal(err)
	}
	proposed := start
	proposed.BuildID = "next-build"
	proposed.RunID = "proposed"
	proposed.RequestID = "different-build"
	worker, err := drt.NewWorker(s.store, drt.Options{Namespace: start.Namespace, BuildID: proposed.BuildID, Queue: start.Queue, Owner: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = func(string, string) (*drt.Worker, error) { return worker, nil }
	if _, err = s.SignalStart(t.Context(), reader(), SignalStartInput{Start: proposed, Name: "signal"}); !errors.Is(err, ErrBuildMismatch) {
		t.Fatalf("atomic build mismatch: %v", err)
	}
	execution, err := s.store.GetExecution(t.Context(), start.Key)
	if err != nil || execution.Revision != 1 {
		t.Fatal("build mismatch mutated", err)
	}
}

func TestQuerySelectorRetainsRunAcrossContinuation(t *testing.T) {
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		t.Run(string(selection), func(t *testing.T) {
			s, start := commandFixture(t)
			worker, err := drt.NewWorker(s.store, drt.Options{Namespace: start.Namespace, BuildID: start.BuildID, Queue: start.Queue, Owner: "continuation", Workflows: map[string]drt.WorkflowFunc{"workflow": func(w *drt.Workflow, input []byte) ([]byte, error) {
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
				return nil, w.ContinueAsNew([]byte("successor"), drt.ContinueOptions{})
			}}})
			if err != nil {
				t.Fatal(err)
			}
			s.runtime = func(string, string) (*drt.Worker, error) { return worker, nil }
			if _, err = s.Start(t.Context(), reader(), start); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			s.authorizer = AuthorizerFunc(func(_ context.Context, _ security.Principal, action string, r Resource) error {
				if action == QueryWorkflow {
					if r.RunID != start.RunID {
						t.Fatalf("query authorized another run: %s", r.RunID)
					}
					once.Do(func() {
						if worked, runErr := worker.RunOnce(t.Context(), durable.TaskWorkflow); runErr != nil || !worked {
							t.Fatalf("continuation: %v %v", worked, runErr)
						}
					})
				}
				return nil
			})
			result, err := s.Query(t.Context(), reader(), drt.QueryRequest{Key: durable.Key{Namespace: start.Namespace, WorkflowID: start.WorkflowID}, Selection: selection, BuildID: start.BuildID, Name: "status"})
			if err != nil || result.Key != start.Key || !bytes.Equal(result.Output, start.Input) {
				t.Fatalf("query retargeted: %+v %v", result, err)
			}
			old, err := s.store.GetExecution(t.Context(), start.Key)
			if err != nil || old.NextRunID == "" {
				t.Fatalf("continuation absent: %+v %v", old, err)
			}
		})
	}
}
