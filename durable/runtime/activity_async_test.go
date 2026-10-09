package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func asyncOptions(maximum int64) drt.ActivityOptions {
	options := retryOptions(maximum)
	options.StartToCloseTimeout = time.Minute
	return options
}

func TestAsyncActivityLifecycle(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(1))
	var handle drt.AsyncActivityHandle
	var retained drt.ActivityInfo
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		retained = info
		if err := info.Heartbeat(ctx, []byte("before")); err != nil {
			return nil, err
		}
		var err error
		handle, err = info.DeferCompletion(ctx)
		if err != nil {
			return nil, err
		}
		second, err := info.DeferCompletion(ctx)
		if err != nil || second != handle {
			return nil, errors.New("handoff is not stable")
		}
		if err = info.Heartbeat(ctx, []byte("after")); !errors.Is(err, durable.ErrLeaseLost) {
			return nil, errors.New("worker heartbeat accepted after handoff")
		}
		if ctx.Err() != nil {
			return nil, errors.New("handoff cancelled the handler")
		}
		return []byte("ignored"), nil
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	if err := handle.Validate(); err != nil || handle.InitialHeartbeatSequence != 1 {
		t.Fatalf("handle: %v %v", handle, err)
	}
	if _, err := retained.DeferCompletion(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("retained handoff callback: %v", err)
	}
	task, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil || task.Done || task.LeaseKind != durable.LeaseAsync || string(task.Progress) != "before" {
		t.Fatalf("persisted handoff: %+v %v", task, err)
	}
	if worked, workErr := w.RunOnce(t.Context(), durable.TaskActivity); workErr != nil || worked {
		t.Fatalf("async task reclaimed: %v %v", worked, workErr)
	}
	encoded, err := json.Marshal(handle)
	if err != nil || !bytes.Contains(encoded, []byte(handle.Secret)) {
		t.Fatal("explicit handle JSON does not carry credential")
	}
	var decoded drt.AsyncActivityHandle
	if err = json.Unmarshal(encoded, &decoded); err != nil || decoded != handle {
		t.Fatal("handle JSON roundtrip failed")
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", handle, handle, handle), handle.Secret) {
		t.Fatal("handle formatting leaks credential")
	}
	options.Owner, options.Activities = "callback-client", nil
	client := newWorker(t, s, options)
	progress := drt.AsyncHeartbeatRequest{Handle: decoded, RequestID: "progress-1", Sequence: 2, Details: []byte("external")}
	hbReceipt, err := client.HeartbeatAsyncActivity(t.Context(), progress)
	if err != nil {
		t.Fatal(err)
	}
	request := drt.AsyncCompletionRequest{Handle: decoded, RequestID: "result-1", Output: []byte("paid")}
	receipt, err := client.CompleteAsyncActivity(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	execution, err := s.GetExecution(t.Context(), key)
	if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" {
		t.Fatalf("async result: %+v %v", execution, err)
	}
	if got, repeatErr := client.CompleteAsyncActivity(t.Context(), request); repeatErr != nil || got != receipt {
		t.Fatalf("receipt after closure: %+v %v", got, repeatErr)
	}
	if got, repeatErr := client.HeartbeatAsyncActivity(t.Context(), progress); repeatErr != nil || got != hbReceipt {
		t.Fatalf("heartbeat receipt after closure: %+v %v", got, repeatErr)
	}
	request.Output = []byte("different")
	if _, err = client.CompleteAsyncActivity(t.Context(), request); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("changed result reused receipt: %v", err)
	}
	outcome := lastActivityOutcome(t, s, key)
	if outcome.Heartbeat == nil || outcome.Heartbeat.Sequence != 2 || string(outcome.Heartbeat.Details) != "external" {
		t.Fatalf("final checkpoint: %+v", outcome.Heartbeat)
	}
	events, err := s.ReadHistory(t.Context(), key, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := durable.HashAsyncSecret(handle.Secret)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if bytes.Contains(event.Payload, []byte(handle.Secret)) || bytes.Contains(event.Payload, []byte(hash)) {
			t.Fatal("history contains callback credential")
		}
	}
}

func TestAsyncCompletionBeforeHandlerReturns(t *testing.T) {
	for _, panicAfter := range []bool{false, true} {
		t.Run(fmt.Sprint(panicAfter), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = retryWorkflow(asyncOptions(1))
			var w *drt.Worker
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				handle, err := info.DeferCompletion(ctx)
				if err != nil {
					return nil, err
				}
				if _, err = w.CompleteAsyncActivity(ctx, drt.AsyncCompletionRequest{Handle: handle, RequestID: "early", Output: []byte("paid")}); err != nil {
					return nil, err
				}
				if _, err = w.RunOnce(ctx, durable.TaskWorkflow); err != nil {
					return nil, err
				}
				if panicAfter {
					panic("handler no longer owns publication")
				}
				return nil, errors.New("ignored after handoff")
			}
			w = newWorker(t, s, options)
			key := startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			execution, err := s.GetExecution(t.Context(), key)
			if err != nil || execution.State != durable.StateCompleted || string(execution.Output) != "paid" {
				t.Fatalf("early completion: %+v %v", execution, err)
			}
		})
	}
}

func TestAsyncHandoffRequiresActiveDeadline(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			s := memory.New()
			options := workerOptions(t)
			options.Workflows["order"] = retryWorkflow(retryOptions(1))
			if legacy {
				options.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("charge", "charge", "", nil).Get() }
			}
			var rejected error
			options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
				_, rejected = info.DeferCompletion(ctx)
				return []byte("ordinary"), nil
			}
			w := newWorker(t, s, options)
			startWorkerRun(t, w, options)
			runTask(t, w, durable.TaskWorkflow)
			runTask(t, w, durable.TaskActivity)
			if !errors.Is(rejected, durable.ErrInvalid) {
				t.Fatalf("handoff without active deadline: %v", rejected)
			}
			runTask(t, w, durable.TaskWorkflow)
		})
	}
}

func TestAsyncHandleValidationAndIsolation(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(1))
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	for _, mode := range []string{"version", "namespace", "build", "epoch", "owner", "kind", "secret", "short_secret", "sequence", "request", "failure"} {
		r := drt.AsyncCompletionRequest{Handle: handle, RequestID: "reject", Output: []byte("paid")}
		switch mode {
		case "version":
			r.Handle.Version++
		case "namespace":
			r.Handle.Key.Namespace += "-other"
		case "build":
			r.Handle.BuildID += "-other"
		case "epoch":
			r.Handle.Token.Epoch++
		case "owner":
			r.Handle.Token.Owner += "-other"
		case "kind":
			r.Handle.Token.LeaseKind = ""
		case "secret":
			r.Handle.Secret = strings.Repeat("ff", 32)
		case "short_secret":
			r.Handle.Secret = "bad"
		case "sequence":
			r.Handle.InitialHeartbeatSequence = -1
		case "request":
			r.RequestID = ""
		case "failure":
			r.Failure = &drt.ApplicationError{Type: "failed", Message: "cannot also have output"}
		}
		if _, err := w.CompleteAsyncActivity(t.Context(), r); err == nil {
			t.Fatalf("invalid %s callback accepted", mode)
		}
	}
	task, err := s.GetTask(t.Context(), key, handle.Token.TaskID)
	if err != nil || task.Done || task.LeaseKind != durable.LeaseAsync {
		t.Fatalf("invalid callback mutated task: %+v %v", task, err)
	}
	if _, err = w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "valid", Output: []byte("paid")}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
}

func TestAsyncFailureRetriesWithProgress(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(2))
	var handles []drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		if info.Attempt == 2 && string(info.HeartbeatDetails()) != "offset:42" {
			return nil, errors.New("retry lost progress")
		}
		handle, err := info.DeferCompletion(ctx)
		handles = append(handles, handle)
		return nil, err
	}
	w := newWorker(t, s, options)
	key := startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	progress := drt.AsyncHeartbeatRequest{Handle: handles[0], RequestID: "progress", Sequence: 1, Details: []byte("offset:42")}
	hb, err := w.HeartbeatAsyncActivity(t.Context(), progress)
	if err != nil {
		t.Fatal(err)
	}
	failure := drt.AsyncCompletionRequest{Handle: handles[0], RequestID: "failed", Failure: &drt.ApplicationError{Type: drt.FailureWorkerLost, Message: "app-defined type"}}
	receipt, err := w.CompleteAsyncActivity(t.Context(), failure)
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.GetTask(t.Context(), key, "command:1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(task.AvailableAt) + time.Millisecond)
	runTask(t, w, durable.TaskActivity)
	if len(handles) != 2 || handles[0].Secret == handles[1].Secret || handles[1].Token.Epoch <= handles[0].Token.Epoch {
		t.Fatal("retry reused async authority")
	}
	if got, repeatErr := w.CompleteAsyncActivity(t.Context(), failure); repeatErr != nil || got != receipt {
		t.Fatalf("failure receipt after retry: %+v %v", got, repeatErr)
	}
	if got, repeatErr := w.HeartbeatAsyncActivity(t.Context(), progress); repeatErr != nil || got != hb {
		t.Fatalf("heartbeat receipt after retry: %+v %v", got, repeatErr)
	}
	progress.RequestID, progress.Sequence = "stale", 2
	if _, err = w.HeartbeatAsyncActivity(t.Context(), progress); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old callback heartbeat: %v", err)
	}
	if _, err = w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handles[1], RequestID: "paid", Output: []byte("paid")}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
}

func TestAsyncHeartbeatChecksPersistedBuild(t *testing.T) {
	s := memory.New()
	options := workerOptions(t)
	options.Workflows["order"] = retryWorkflow(asyncOptions(1))
	var handle drt.AsyncActivityHandle
	options.Activities["charge"] = func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		var err error
		handle, err = info.DeferCompletion(ctx)
		return nil, err
	}
	w := newWorker(t, s, options)
	startWorkerRun(t, w, options)
	runTask(t, w, durable.TaskWorkflow)
	runTask(t, w, durable.TaskActivity)
	options.BuildID += "-foreign"
	foreign := newWorker(t, s, options)
	copied := handle
	copied.BuildID = options.BuildID
	request := drt.AsyncHeartbeatRequest{Handle: copied, RequestID: "progress", Sequence: 1, Details: []byte("checkpoint")}
	if _, err := foreign.HeartbeatAsyncActivity(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("foreign persisted build accepted heartbeat: %v", err)
	}
	request.Handle = handle
	receipt, err := w.HeartbeatAsyncActivity(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.CompleteAsyncActivity(t.Context(), drt.AsyncCompletionRequest{Handle: handle, RequestID: "result", Output: []byte("paid")}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	request.Handle = copied
	if _, err = foreign.HeartbeatAsyncActivity(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("foreign build recovered heartbeat receipt: %v", err)
	}
	request.Handle = handle
	if got, replayErr := w.HeartbeatAsyncActivity(t.Context(), request); replayErr != nil || got != receipt {
		t.Fatalf("matching build lost closed receipt: %+v %v", got, replayErr)
	}
}
