package operator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

func TestSignalStartWorkflowGrantsAndRecoveredTarget(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			s, start := commandFixture(t)
			if existing {
				if _, err := s.Start(t.Context(), reader(), start); err != nil {
					t.Fatal(err)
				}
			}
			start.RunID = "proposed"
			in := SignalStartInput{Start: start, Name: "approve", Input: []byte("exact")}
			denyRun := true
			calls := map[string]int{}
			s.authorizer = AuthorizerFunc(func(_ context.Context, _ security.Principal, action string, r Resource) error {
				calls[action]++
				if r.RunID != "" && denyRun {
					return security.ErrForbidden
				}
				return nil
			})
			first, err := s.SignalStart(t.Context(), reader(), in)
			if err != nil || first.Started == existing {
				t.Fatalf("first acceptance: %+v %v", first, err)
			}
			if calls[StartWorkflow] != 1 || calls[SignalWorkflow] != 1 || calls[SignalStartWorkflow] != 1 {
				t.Fatal(calls)
			}
			before, _ := s.store.GetExecution(t.Context(), first.Key)
			if _, err = s.SignalStart(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) {
				t.Fatalf("revoked accepted run: %v", err)
			}
			after, _ := s.store.GetExecution(t.Context(), first.Key)
			if before.Revision != after.Revision {
				t.Fatal("denied replay mutated")
			}
			denyRun = false
			again, err := s.SignalStart(t.Context(), reader(), in)
			if err != nil || again != first {
				t.Fatalf("recovered: %+v %v", again, err)
			}
			in.Input = []byte("changed")
			if _, err = s.SignalStart(t.Context(), reader(), in); !errors.Is(err, durable.ErrRequestConflict) {
				t.Fatalf("changed content: %v", err)
			}
		})
	}
}

type lostStartOutcome struct {
	durable.Store
	capability durable.SignalStartOutcomeStore
	lost       atomic.Bool
}

func (s *lostStartOutcome) SignalWithStartOutcome(ctx context.Context, r durable.SignalWithStartRequest) (durable.SignalStartOutcome, error) {
	outcome, err := s.capability.SignalWithStartOutcome(ctx, r)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return durable.SignalStartOutcome{}, errors.New("lost acknowledgement")
	}
	return outcome, err
}
func TestSignalStartLostAcknowledgementReauthorizesRecoveredRun(t *testing.T) {
	s, start := commandFixture(t)
	capability, ok := s.store.(durable.SignalStartOutcomeStore)
	if !ok {
		t.Fatal("missing capability")
	}
	proxy := &lostStartOutcome{Store: s.store, capability: capability}
	worker, err := drt.NewWorker(proxy, drt.Options{Namespace: start.Namespace, BuildID: start.BuildID, Queue: start.Queue, Owner: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = func(string, string) (*drt.Worker, error) { return worker, nil }
	s.authorizer = AuthorizerFunc(func(_ context.Context, _ security.Principal, _ string, r Resource) error {
		if r.RunID != "" {
			return security.ErrForbidden
		}
		return nil
	})
	if _, err = s.SignalStart(t.Context(), reader(), SignalStartInput{Start: start, Name: "signal"}); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("lost ACK bypass: %v", err)
	}
	e, err := s.store.GetExecution(t.Context(), start.Key)
	if err != nil || e.LastSequence != 2 {
		t.Fatalf("accepted once: %+v %v", e, err)
	}
}
func TestSignalStartConcurrentOutcome(t *testing.T) {
	s, start := commandFixture(t)
	capability, ok := s.store.(durable.SignalStartOutcomeStore)
	if !ok {
		t.Fatal("missing capability")
	}
	var fresh, recovered atomic.Int64
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			outcome, err := capability.SignalWithStartOutcome(t.Context(), durable.SignalWithStartRequest{Start: start.request(), Name: "signal"})
			if err != nil {
				t.Error(err)
				return
			}
			if outcome.Recovered {
				recovered.Add(1)
			} else {
				fresh.Add(1)
			}
		})
	}
	group.Wait()
	if fresh.Load() != 1 || recovered.Load() != 7 {
		t.Fatalf("fresh %d recovered %d", fresh.Load(), recovered.Load())
	}
}

func TestExactCommandAndRecoveredSignalStartAfterContinuation(t *testing.T) {
	s, start := commandFixture(t)
	worker, err := drt.NewWorker(s.store, drt.Options{Namespace: start.Namespace, BuildID: start.BuildID, Queue: start.Queue, Owner: "continuation", Workflows: map[string]drt.WorkflowFunc{"workflow": func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return nil, w.ContinueAsNew(nil, drt.ContinueOptions{})
	}}})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = func(string, string) (*drt.Worker, error) { return worker, nil }
	in := SignalStartInput{Start: start, Name: "signal"}
	first, err := s.SignalStart(t.Context(), reader(), in)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	s.authorizer = AuthorizerFunc(func(_ context.Context, _ security.Principal, action string, r Resource) error {
		if action == SignalWorkflow && r.RunID == start.RunID {
			once.Do(func() {
				if worked, runErr := worker.RunOnce(t.Context(), durable.TaskWorkflow); runErr != nil || !worked {
					t.Fatalf("continuation: %v %v", worked, runErr)
				}
			})
		}
		return nil
	})
	signal := durable.SignalRequest{Key: start.Key, RequestID: "during-continuation", BuildID: start.BuildID, Name: "signal"}
	if _, err = s.Signal(t.Context(), reader(), signal); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("mutation retargeted: %v", err)
	}
	old, err := s.store.GetExecution(t.Context(), start.Key)
	if err != nil || old.NextRunID == "" {
		t.Fatalf("missing successor: %+v %v", old, err)
	}
	replay, err := s.SignalStart(t.Context(), reader(), in)
	if err != nil || replay != first {
		t.Fatalf("replay retargeted: %+v %v", replay, err)
	}
	s.authorizer = AuthorizerFunc(func(_ context.Context, _ security.Principal, _ string, r Resource) error {
		if r.RunID == start.RunID {
			return security.ErrForbidden
		}
		return nil
	})
	if _, err = s.SignalStart(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("old target revoke: %v", err)
	}
}
