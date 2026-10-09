package durabletest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func targetFor(key durable.Key, selection durable.RunSelection) durable.ExecutionTarget {
	if selection != durable.RunExplicit {
		key.RunID = ""
	}
	return durable.ExecutionTarget{Key: key, Selection: selection}
}

func assertTarget(t *testing.T, s durable.Store, key durable.Key, selection durable.RunSelection, want durable.Key, state durable.State) {
	t.Helper()
	got, err := s.ResolveExecution(t.Context(), targetFor(key, selection))
	if err != nil || got.Key != want || got.State != state {
		t.Fatalf("resolve %q: %+v %v", selection, got, err)
	}
}

func executionTargets(t *testing.T, s durable.Store) {
	for _, state := range []durable.State{durable.StateCompleted, durable.StateFailed, durable.StateCancelled, durable.StateTerminated, durable.StateTimedOut} {
		t.Run(string(state), func(t *testing.T) {
			first := start(t, s)
			for _, selection := range []durable.RunSelection{durable.RunExplicit, durable.RunCurrent, durable.RunLatest} {
				assertTarget(t, s, first.Key, selection, first.Key, durable.StateRunning)
			}
			got, err := s.ResolveExecution(t.Context(), targetFor(first.Key, durable.RunLatest))
			if err != nil {
				t.Fatal(err)
			}
			got.Input[0] = 'X'
			again, err := s.ResolveExecution(t.Context(), targetFor(first.Key, durable.RunLatest))
			if err != nil || string(again.Input) != "input" {
				t.Fatalf("aliased: %+v %v", again, err)
			}
			closeLinkedExecution(t, s, first, state)
			if _, err = s.ResolveExecution(t.Context(), targetFor(first.Key, durable.RunCurrent)); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("closed current: %v", err)
			}
			assertTarget(t, s, first.Key, durable.RunLatest, first.Key, state)
			if state == durable.StateCompleted {
				got, err = s.ResolveExecution(t.Context(), targetFor(first.Key, durable.RunLatest))
				if err != nil {
					t.Fatal(err)
				}
				got.Output[0] = 'X'
				again, err = s.GetExecution(t.Context(), first.Key)
				if err != nil || string(again.Output) != "child result" {
					t.Fatalf("aliased output: %+v %v", again, err)
				}
			}
			next := first
			next.RunID = "run-0"
			next.RequestID = "next"
			next.BuildID = "v2"
			if _, err = s.StartExecution(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			if _, err = s.StartExecution(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			assertTarget(t, s, first.Key, durable.RunExplicit, first.Key, state)
			assertTarget(t, s, first.Key, durable.RunLatest, next.Key, durable.StateRunning)
			assertTarget(t, s, first.Key, durable.RunCurrent, next.Key, durable.StateRunning)
			if _, err = s.ResolveExecution(t.Context(), targetFor(durable.Key{Namespace: first.Namespace + "other", WorkflowID: first.WorkflowID}, durable.RunLatest)); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("namespace: %v", err)
			}
		})
	}
}

func executionTargetCreationPaths(t *testing.T, s durable.Store) {
	t.Run("signal", func(t *testing.T) {
		first := durable.StartRequest{Key: durable.Key{Namespace: t.Name(), WorkflowID: "workflow", RunID: "signal-run"}, RequestID: "signal-start", WorkflowType: "order", BuildID: "v1", Queue: "orders"}
		request := durable.SignalWithStartRequest{Start: first, Name: "approve", Input: []byte("yes")}
		original, err := s.SignalWithStart(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		assertTarget(t, s, first.Key, durable.RunLatest, first.Key, durable.StateRunning)
		closeLinkedExecution(t, s, first, durable.StateCompleted)
		next := first
		next.RunID = "next"
		next.RequestID = "new"
		if _, err = s.StartExecution(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		if got, retryErr := s.SignalWithStart(t.Context(), request); retryErr != nil || got != original {
			t.Fatalf("retry: %+v %v", got, retryErr)
		}
		assertTarget(t, s, first.Key, durable.RunLatest, next.Key, durable.StateRunning)
	})
	t.Run("child", func(t *testing.T) {
		_, child := childPair(t, s, durable.ParentCloseAbandon)
		assertTarget(t, s, child.Start.Key, durable.RunLatest, child.Start.Key, durable.StateRunning)
		closeLinkedExecution(t, s, child.Start, durable.StateCompleted)
		next := child.Start
		next.RunID = "replacement"
		next.RequestID = "replacement"
		if _, err := s.StartExecution(t.Context(), next); err != nil {
			t.Fatal(err)
		}
		if _, err := s.StartExecution(t.Context(), child.Start); err != nil {
			t.Fatal(err)
		}
		assertTarget(t, s, child.Start.Key, durable.RunLatest, next.Key, durable.StateRunning)
	})
}

func executionTargetConcurrent(t *testing.T, s durable.Store) {
	first := start(t, s)
	closeLinkedExecution(t, s, first, durable.StateCompleted)
	var wg sync.WaitGroup
	winners := make(chan durable.Key, 8)
	failures := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			next := first
			next.RunID = fmt.Sprint("run-", i+2)
			next.RequestID = next.RunID
			if _, err := s.StartExecution(t.Context(), next); err == nil {
				winners <- next.Key
			} else if !errors.Is(err, durable.ErrExists) {
				failures <- err
			}
		})
	}
	wg.Wait()
	close(winners)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if len(winners) != 1 {
		t.Fatalf("winners: %d", len(winners))
	}
	winner := <-winners
	assertTarget(t, s, first.Key, durable.RunLatest, winner, durable.StateRunning)
	assertTarget(t, s, first.Key, durable.RunCurrent, winner, durable.StateRunning)
}

func executionTargetValidation(t *testing.T, s durable.Store) {
	first := start(t, s)
	for _, target := range []durable.ExecutionTarget{{}, {Key: first.Key, Selection: durable.RunLatest}, {Key: first.Key, Selection: "unknown"}, {Key: durable.Key{Namespace: first.Namespace, WorkflowID: first.WorkflowID}}} {
		if _, err := s.ResolveExecution(t.Context(), target); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid %+v: %v", target, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.ResolveExecution(ctx, targetFor(first.Key, durable.RunLatest)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	missing := first.Key
	missing.WorkflowID = "missing"
	for _, selection := range []durable.RunSelection{durable.RunExplicit, durable.RunCurrent, durable.RunLatest} {
		if _, err := s.ResolveExecution(t.Context(), targetFor(missing, selection)); !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
	}
}
