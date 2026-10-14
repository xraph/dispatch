//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
)

const liveLoadExecutions = 128
const liveLoadSubmitters = 8

type liveLoadObservation struct {
	request       durable.StartRequest
	receipt       durable.Receipt
	submitLatency time.Duration
	execution     durable.Execution
}

// submitLiveLoad uses the trusted native store start boundary, as the existing
// service fixture does. It does not qualify remote operator start authorization.
func submitLiveLoad(ctx context.Context, f *liveServiceFixture, observations []liveLoadObservation) error {
	jobs := make(chan int, len(observations))
	failures := make(chan error, len(observations))
	for index := range observations {
		jobs <- index
	}
	close(jobs)
	var workers sync.WaitGroup
	for range liveLoadSubmitters {
		workers.Go(func() {
			for index := range jobs {
				started := time.Now()
				receipt, err := f.Native.StartExecution(ctx, observations[index].request)
				observations[index].submitLatency = time.Since(started)
				if err != nil {
					failures <- fmt.Errorf("load start %s: %w", observations[index].request.WorkflowID, err)
					continue
				}
				observations[index].receipt = receipt
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		return err
	}
	return nil
}

func awaitLiveLoad(ctx context.Context, f *liveServiceFixture, observations []liveLoadObservation) error {
	remaining := len(observations)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for remaining > 0 {
		for index := range observations {
			observed := &observations[index]
			if observed.execution.State == durable.StateCompleted {
				continue
			}
			execution, err := f.Native.GetExecution(ctx, observed.request.Key)
			if err != nil {
				return err
			}
			switch execution.State {
			case durable.StateRunning:
			case durable.StateCompleted:
				if !bytes.Equal(execution.Output, observed.request.Input) {
					return fmt.Errorf("load output %s differs from accepted input", execution.WorkflowID)
				}
				observed.execution = execution
				remaining--
			default:
				return fmt.Errorf("load execution %s ended in %s", execution.WorkflowID, execution.State)
			}
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d accepted load executions remain: %w", remaining, ctx.Err())
		case <-ticker.C:
		}
	}
	return nil
}

func assertLiveLoadHistory(ctx context.Context, t *testing.T, f *liveServiceFixture, observed liveLoadObservation) {
	t.Helper()
	request := observed.request
	before := observed.execution
	if observed.receipt != (durable.Receipt{Revision: 1, FirstSequence: 1, LastSequence: 1}) {
		t.Fatal("accepted start receipt", request.WorkflowID, observed.receipt)
	}
	events, err := f.Native.ReadHistory(ctx, request.Key, 0, 64)
	if err != nil || len(events) == 0 || len(events) >= 64 {
		t.Fatal("complete bounded workflow history", request.WorkflowID, err)
	}
	starts, completed := 0, 0
	for index, event := range events {
		if event.Sequence != int64(index+1) || event.Time.IsZero() {
			t.Fatal("history sequence/time", request.WorkflowID, event.Sequence)
		}
		switch event.Type {
		case drt.EventStarted:
			starts++
			if !bytes.Equal(event.Payload, request.Input) {
				t.Fatal("start payload changed", request.WorkflowID)
			}
		case drt.EventWorkflowCompleted:
			completed++
			if !bytes.Equal(event.Payload, request.Input) {
				t.Fatal("completion payload changed", request.WorkflowID)
			}
		default:
			t.Fatal("unexpected echo workflow effect", request.WorkflowID, event.Type)
		}
	}
	if starts != 1 || completed != 1 || before.LastSequence != int64(len(events)) {
		t.Fatal("lost or duplicate durable completion", request.WorkflowID, starts, completed)
	}
	task, err := f.Native.GetTask(ctx, request.Key, "workflow:1")
	if err != nil || !task.Done || task.Kind != durable.TaskWorkflow || task.Queue != request.Queue {
		t.Fatal("accepted task remains unresolved", request.WorkflowID, err)
	}
	// Start receipts retain the canonical start digest, not a client IntentDigest.
	// Exact StartExecution replay reads that saved receipt before run admission;
	// LookupReceipt intentionally refuses this receipt class.
	replay, err := f.Native.StartExecution(ctx, request)
	if err != nil || replay != observed.receipt {
		t.Fatal("closed execution start replay", request.WorkflowID, err)
	}
	after, err := f.Native.GetExecution(ctx, request.Key)
	if err != nil || after.State != durable.StateCompleted || after.Revision != before.Revision || after.LastSequence != before.LastSequence || !bytes.Equal(after.Output, before.Output) {
		t.Fatal("exact start replay changed completed execution", request.WorkflowID, err)
	}
	tail, err := f.Native.ReadHistory(ctx, request.Key, before.LastSequence, 1)
	if err != nil || len(tail) != 0 {
		t.Fatal("exact start replay appended an effect", request.WorkflowID, err)
	}
}

func liveLoadPercentile(sorted []time.Duration, percent int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := (len(sorted)*percent+99)/100 - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func TestLiveWorkerLoadAndMidpointDrain(t *testing.T) {
	totalStarted := time.Now()
	f := newLiveService(t)
	survivor := newLiveServiceSibling(t, f)
	_, client := newLiveOperator(t, f, survivor)
	// The workload deadline includes fixture setup already spent. Every native
	// operation below uses the same deadline; no production cap is changed.
	ctx, cancel := context.WithDeadline(t.Context(), totalStarted.Add(2*time.Minute))
	defer cancel()
	if err := ctx.Err(); err != nil {
		t.Fatal("load fixture exhausted its deadline", err)
	}
	observations := make([]liveLoadObservation, liveLoadExecutions)
	prefix := "load-" + liveRandom(t)
	for index := range observations {
		identity := fmt.Sprintf("%s-%03d", prefix, index)
		observations[index].request = durable.StartRequest{Key: durable.Key{Namespace: f.Plan.Namespace, WorkflowID: identity, RunID: "original"}, RequestID: "start-" + identity, WorkflowType: "managed-history", BuildID: f.Plan.Build, Queue: f.Plan.Queue, Input: []byte(fmt.Sprintf(`{"load_index":%d,"identity":%q}`, index, identity))}
	}
	started := time.Now()
	if err := submitLiveLoad(ctx, f, observations[:liveLoadExecutions/2]); err != nil {
		t.Fatal(err)
	}
	control, err := f.Runtime.WorkerControl(ctx, f.NativeIdentity.QueryRuntimeTarget)
	if err != nil {
		t.Fatal(err)
	}
	before, err := control.Status(ctx)
	if err != nil || before.AdmissionClosed || !before.Ready {
		t.Fatal("candidate not polling before midpoint drain", err)
	}
	drainInput := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: operator.BuildInput{Namespace: f.Plan.Namespace, BuildID: f.Plan.Build}, RuntimeID: f.Plan.RuntimeID}, RequestID: "load-midpoint-drain", OperationID: "load-process-drain", Deadline: durable.Timestamp(time.Now().Add(25 * time.Second))}
	drainStarted := time.Now()
	drained, err := client.DrainWorker(ctx, drainInput)
	drainLatency := time.Since(drainStarted)
	if err != nil || !drained.Complete || !drained.Quiescent || drained.RuntimeID != f.Plan.RuntimeID || drained.InstanceID != f.Plan.InstanceID || drained.OperationID != drainInput.OperationID || !drained.Deadline.Equal(drainInput.Deadline) {
		t.Fatal("authenticated midpoint drain", err)
	}
	after, err := control.Status(ctx)
	if err != nil || !after.AdmissionClosed || after.InFlight != 0 || after.UnknownClaims != 0 || after.Failure != "" {
		t.Fatal("drained worker still has unresolved local work", err)
	}
	// These starts happen after the original process has permanently closed its
	// claim admission. The independent already-enrolled survivor must consume them.
	if err = submitLiveLoad(ctx, f, observations[liveLoadExecutions/2:]); err != nil {
		t.Fatal(err)
	}
	if err = awaitLiveLoad(ctx, f, observations); err != nil {
		t.Fatal(err)
	}
	executionElapsed := time.Since(started)
	if replay, e := client.ReadDrain(ctx, drainInput); e != nil || replay.OperationID != drained.OperationID || replay.RuntimeID != drained.RuntimeID || !replay.Deadline.Equal(drained.Deadline) {
		t.Fatal("exact retained drain receipt", e)
	}
	acceptedLatency := make([]time.Duration, len(observations))
	persistedLatency := make([]time.Duration, len(observations))
	for index, observed := range observations {
		assertLiveLoadHistory(ctx, t, f, observed)
		acceptedLatency[index] = observed.submitLatency
		persistedLatency[index] = observed.execution.UpdatedAt.Sub(observed.execution.CreatedAt)
		if persistedLatency[index] < 0 {
			t.Fatal("negative durable completion latency")
		}
	}
	finalStatus, err := control.Status(ctx)
	if err != nil || !finalStatus.AdmissionClosed || finalStatus.InFlight != 0 || finalStatus.UnknownClaims != 0 || finalStatus.RuntimeID != f.Plan.RuntimeID {
		t.Fatal("original drain was reopened during later load", err)
	}
	if identity, e := survivor.Runtime.ResolveBinding(ctx, survivor.NativeIdentity.QueryRuntimeTarget); e != nil || identity != survivor.NativeIdentity {
		t.Fatal("exact independent survivor unavailable after load", e)
	}
	slices.Sort(acceptedLatency)
	slices.Sort(persistedLatency)
	t.Logf("actual durable load: accepted=%d completed=%d submitters=%d worker_concurrency=1 each; midpoint accepted=%d candidate_inflight=%d; workload=%s throughput=%.2f completed/s; submit p50=%s p95=%s max=%s; durable completion p50=%s p95=%s max=%s; drain=%s; total=%s", len(observations), len(observations), liveLoadSubmitters, liveLoadExecutions/2, before.InFlight, executionElapsed, float64(len(observations))/executionElapsed.Seconds(), liveLoadPercentile(acceptedLatency, 50), liveLoadPercentile(acceptedLatency, 95), acceptedLatency[len(acceptedLatency)-1], liveLoadPercentile(persistedLatency, 50), liveLoadPercentile(persistedLatency, 95), persistedLatency[len(persistedLatency)-1], drainLatency, time.Since(totalStarted))
}
