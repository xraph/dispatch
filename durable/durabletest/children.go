package durabletest

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func childSpec(r durable.StartRequest, id string) durable.ChildStartSpec {
	return durable.ChildStartSpec{CommandID: id, Start: durable.StartRequest{Key: durable.Key{Namespace: r.Namespace, WorkflowID: "child-" + id, RunID: "run"}, RequestID: "start-child", WorkflowType: "child", BuildID: "child-v1", Queue: "children", Input: []byte("child input")}, ParentQueue: r.Queue, ParentClosePolicy: durable.ParentCloseTerminate}
}

func childCommit(r durable.StartRequest, task *durable.Task, children ...durable.ChildStartSpec) durable.CommitRequest {
	return durable.CommitRequest{Key: r.Key, RequestID: "children", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: children, Tasks: []durable.TaskSpec{{ID: "next", Kind: durable.TaskWorkflow, Queue: r.Queue}}}
}

func childCreation(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	a, b := childSpec(r, "a"), childSpec(r, "b")
	b.ParentClosePolicy = durable.ParentCloseAbandon
	request := childCommit(r, task, a, b)
	receipt, err := s.CommitTransition(t.Context(), request)
	if err != nil || receipt.Revision != 2 || receipt.FirstSequence != 2 || receipt.LastSequence != 4 {
		t.Fatalf("creation: %+v %v", receipt, err)
	}
	request.Children[0].Start.Input[0] = 'X'
	saved, savedErr := s.GetChildExecution(t.Context(), r.Key, "a")
	if savedErr != nil || string(saved.Start.Input) != "child input" {
		t.Fatalf("caller input alias: %+v %v", saved, savedErr)
	}
	request.Children[0].Start.Input[0] = 'c'
	for _, spec := range []durable.ChildStartSpec{a, b} {
		child, readErr := s.GetExecution(t.Context(), spec.Start.Key)
		if readErr != nil || child.BuildID != spec.Start.BuildID || child.State != durable.StateRunning || child.Revision != 1 || string(child.Input) != "child input" {
			t.Fatalf("child: %+v %v", child, readErr)
		}
		link, linkErr := s.GetChildExecution(t.Context(), r.Key, spec.CommandID)
		if linkErr != nil || link.Parent != r.Key || !reflect.DeepEqual(link.ChildStartSpec, spec) || link.State != durable.StateRunning || !link.CreatedAt.Equal(child.CreatedAt) {
			t.Fatalf("relationship: %+v %v", link, linkErr)
		}
		parent, parentErr := s.GetParentExecution(t.Context(), spec.Start.Key)
		if parentErr != nil || !reflect.DeepEqual(link, parent) {
			t.Fatalf("reverse relationship: %+v %v", parent, parentErr)
		}
		link.Start.Input[0] = 'X'
		again, againErr := s.GetChildExecution(t.Context(), r.Key, spec.CommandID)
		if againErr != nil || string(again.Start.Input) != "child input" {
			t.Fatalf("aliased input: %+v %v", again, againErr)
		}
		started, startErr := s.StartExecution(t.Context(), spec.Start)
		if startErr != nil || started.Revision != 1 || started.LastSequence != 1 {
			t.Fatalf("child start receipt: %+v %v", started, startErr)
		}
		history, historyErr := s.ReadHistory(t.Context(), spec.Start.Key, 0, 100)
		if historyErr != nil || len(history) != 1 || string(history[0].Payload) != "child input" {
			t.Fatalf("child history: %+v %v", history, historyErr)
		}
		polled, pollErr := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: spec.Start.Queue, Kind: durable.TaskWorkflow, Owner: spec.CommandID, BuildID: "wrong", LeaseDuration: time.Minute})
		if pollErr != nil || polled != nil {
			t.Fatalf("wrong child build polled: %+v %v", polled, pollErr)
		}
	}
	history, err := s.ReadHistory(t.Context(), r.Key, 1, 100)
	if err != nil || len(history) != 3 {
		t.Fatalf("parent events: %+v %v", history, err)
	}
	for i, spec := range []durable.ChildStartSpec{a, b} {
		var event durable.ChildStarted
		if err = json.Unmarshal(history[i+1].Payload, &event); err != nil || history[i+1].Type != durable.EventChildStarted || event.Version != 1 || event.CommandID != spec.CommandID || event.Child != spec.Start.Key || event.ParentClosePolicy != spec.ParentClosePolicy {
			t.Fatalf("started event: %+v %v", event, err)
		}
	}
	page, err := s.ListChildExecutions(t.Context(), r.Key, "", 1)
	if err != nil || len(page) != 1 || page[0].CommandID != "a" {
		t.Fatalf("page one: %+v %v", page, err)
	}
	page, err = s.ListChildExecutions(t.Context(), r.Key, "a", 1)
	if err != nil || len(page) != 1 || page[0].CommandID != "b" {
		t.Fatalf("page two: %+v %v", page, err)
	}
	page, err = s.ListChildExecutions(t.Context(), r.Key, "b", 1)
	if err != nil || len(page) != 0 {
		t.Fatalf("page end: %+v %v", page, err)
	}
	childTask, err := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: a.Start.Queue, Kind: durable.TaskWorkflow, Owner: "child", BuildID: a.Start.BuildID, LeaseDuration: time.Minute})
	if err != nil || childTask == nil {
		t.Fatalf("child claim: %+v %v", childTask, err)
	}
	if _, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: childTask.Key, RequestID: "close", ExpectedRevision: 1, Token: childTask.Token(), Events: []durable.EventInput{{Type: "closed"}}, State: durable.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	link, err := s.GetParentExecution(t.Context(), childTask.Key)
	if err != nil || link.State != durable.StateCompleted {
		t.Fatalf("fresh child state: %+v %v", link, err)
	}
	parentTask := claim(t, s, r, time.Minute)
	closeRequest := completion(r, parentTask)
	closeRequest.ExpectedRevision = 2
	if _, err = s.CommitTransition(t.Context(), closeRequest); err != nil {
		t.Fatal(err)
	}
	if got, retryErr := s.CommitTransition(t.Context(), request); retryErr != nil || got != receipt {
		t.Fatalf("closed parent retry: %+v %v", got, retryErr)
	}
	changed := request
	changed.Children = append([]durable.ChildStartSpec(nil), request.Children...)
	changed.Children[0].ParentClosePolicy = durable.ParentCloseAbandon
	if _, err = s.CommitTransition(t.Context(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed child retry: %v", err)
	}
}

func childAtomicConflict(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	a, b := childSpec(r, "a"), childSpec(r, "b")
	if _, err := s.StartExecution(t.Context(), b.Start); err != nil {
		t.Fatal(err)
	}
	request := childCommit(r, task, a, b)
	_, err := s.CommitTransition(t.Context(), request)
	var conflict *durable.ChildStartError
	if !errors.As(err, &conflict) || !errors.Is(err, durable.ErrExists) || conflict.CommandID != "b" {
		t.Fatalf("conflict: %v", err)
	}
	if _, err = s.GetExecution(t.Context(), a.Start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("partial child: %v", err)
	}
	if _, err = s.GetParentExecution(t.Context(), b.Start.Key); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("adopted existing run: %v", err)
	}
	e, err := s.GetExecution(t.Context(), r.Key)
	if err != nil || e.Revision != 1 || e.LastSequence != 1 {
		t.Fatalf("partial parent: %+v %v", e, err)
	}
	request.Children = request.Children[:1]
	if _, err = s.CommitTransition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func childConcurrentIdentity(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	spec := childSpec(r, "same")
	request := childCommit(r, task, spec)
	var wg sync.WaitGroup
	var childErr, startErr error
	wg.Go(func() { _, childErr = s.CommitTransition(t.Context(), request) })
	wg.Go(func() { _, startErr = s.StartExecution(t.Context(), spec.Start) })
	wg.Wait()
	if childErr == nil {
		if startErr != nil {
			t.Fatalf("same child start receipt: %v", startErr)
		}
		if _, err := s.GetParentExecution(t.Context(), spec.Start.Key); err != nil {
			t.Fatal(err)
		}
	} else {
		if !errors.Is(childErr, durable.ErrExists) || startErr != nil {
			t.Fatalf("race child=%v external=%v", childErr, startErr)
		}
		if _, err := s.GetParentExecution(t.Context(), spec.Start.Key); !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("external run adopted: %v", err)
		}
	}
}

func childSourceGuards(t *testing.T, s durable.Store) {
	for _, mode := range []string{"queue", "activity", "expired", "namespace"} {
		t.Run(mode, func(t *testing.T) {
			r := start(t, s)
			ttl := time.Minute
			if mode == "expired" {
				ttl = 20 * time.Millisecond
			}
			task := claim(t, s, r, ttl)
			request := childCommit(r, task, childSpec(r, "a"))
			if mode == "activity" {
				if _, err := s.CommitTransition(t.Context(), durable.CommitRequest{Key: r.Key, RequestID: "seed", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "seed"}}, Tasks: []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: r.Queue}}}); err != nil {
					t.Fatal(err)
				}
				var err error
				task, err = s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: r.Namespace, Queue: r.Queue, Kind: durable.TaskActivity, Owner: "activity", LeaseDuration: time.Minute})
				if err != nil || task == nil {
					t.Fatalf("activity: %+v %v", task, err)
				}
				request.Token, request.ExpectedRevision = task.Token(), 2
			}
			if mode == "queue" {
				request.Children[0].ParentQueue = "wrong"
			}
			if mode == "namespace" {
				request.Children[0].Start.Namespace = "other"
			}
			if mode == "expired" {
				time.Sleep(30 * time.Millisecond)
			}
			_, err := s.CommitTransition(t.Context(), request)
			want := durable.ErrInvalid
			if mode == "expired" {
				want = durable.ErrLeaseLost
			}
			if !errors.Is(err, want) {
				t.Fatalf("source guard: %v want %v", err, want)
			}
			if _, err = s.GetExecution(t.Context(), request.Children[0].Start.Key); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("rejected child persisted: %v", err)
			}
		})
	}
}

func childConcurrentParents(t *testing.T, s durable.Store) {
	r := start(t, s)
	first := claim(t, s, r, time.Minute)
	other := r
	other.WorkflowID = "other-parent"
	if _, err := s.StartExecution(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	second := claim(t, s, other, time.Minute)
	spec := childSpec(r, "shared")
	requests := []durable.CommitRequest{childCommit(r, first, spec), childCommit(other, second, spec)}
	errorsFound := make([]error, 2)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Go(func() { _, errorsFound[i] = s.CommitTransition(t.Context(), requests[i]) })
	}
	wg.Wait()
	winners := 0
	for i, err := range errorsFound {
		if err == nil {
			winners++
			parent, readErr := s.GetParentExecution(t.Context(), spec.Start.Key)
			if readErr != nil || parent.Parent != requests[i].Key {
				t.Fatalf("owner: %+v %v", parent, readErr)
			}
		} else if !errors.Is(err, durable.ErrExists) {
			t.Fatalf("loser: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("identity owners: %d", winners)
	}
}

func childNamespaceAndReadValidation(t *testing.T, s durable.Store) {
	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			r := start(t, s)
			task := claim(t, s, r, time.Minute)
			spec := childSpec(r, "same")
			request := childCommit(r, task, spec)
			if _, err := s.CommitTransition(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			link, err := s.GetParentExecution(t.Context(), spec.Start.Key)
			if err != nil || link.Parent != r.Key {
				t.Fatalf("namespace: %+v %v", link, err)
			}
			for _, limit := range []int{0, 1001} {
				if _, err = s.ListChildExecutions(t.Context(), r.Key, "", limit); !errors.Is(err, durable.ErrInvalid) {
					t.Fatalf("invalid limit: %v", err)
				}
			}
			missing := r.Key
			missing.RunID = "missing"
			if _, err = s.ListChildExecutions(t.Context(), missing, "", 1); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("missing parent: %v", err)
			}
			if _, err = s.GetChildExecution(t.Context(), r.Key, "missing"); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("missing child: %v", err)
			}
			if _, err = s.GetParentExecution(t.Context(), r.Key); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("root parent: %v", err)
			}
		})
	}
}

func childSignalStartRace(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	spec := childSpec(r, "shared")
	request := childCommit(r, task, spec)
	var wg sync.WaitGroup
	var childErr, signalErr error
	var signal durable.SignalReceipt
	wg.Go(func() { _, childErr = s.CommitTransition(t.Context(), request) })
	wg.Go(func() {
		signal, signalErr = s.SignalWithStart(t.Context(), durable.SignalWithStartRequest{Start: spec.Start, Name: "hello", Input: []byte("hello")})
	})
	wg.Wait()
	if signalErr != nil {
		t.Fatal(signalErr)
	}
	link, err := s.GetParentExecution(t.Context(), spec.Start.Key)
	if signal.Started {
		if !errors.Is(childErr, durable.ErrExists) || !errors.Is(err, durable.ErrNotFound) {
			t.Fatalf("adopted signal run: child=%v parent=%+v %v", childErr, link, err)
		}
	} else if childErr != nil || err != nil || link.Parent != r.Key {
		t.Fatalf("lost child ownership: child=%v parent=%+v %v", childErr, link, err)
	}
}

func childDuplicateDecision(t *testing.T, s durable.Store) {
	r := start(t, s)
	task := claim(t, s, r, time.Minute)
	request := childCommit(r, task, childSpec(r, "same"))
	var wg sync.WaitGroup
	receipts := make([]durable.Receipt, 8)
	failures := make([]error, 8)
	for i := range receipts {
		wg.Go(func() { receipts[i], failures[i] = s.CommitTransition(t.Context(), request) })
	}
	wg.Wait()
	for i, err := range failures {
		if err != nil || receipts[i] != receipts[0] || receipts[i].LastSequence != 3 {
			t.Fatalf("duplicate %d: %+v %v", i, receipts[i], err)
		}
	}
	links, err := s.ListChildExecutions(t.Context(), r.Key, "", 100)
	if err != nil || len(links) != 1 {
		t.Fatalf("duplicate relationships: %+v %v", links, err)
	}
}
