package postgres_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

type nativeMutationCase struct {
	operation string
	fields    map[string]any
}

func nativeDecode[T any](t *testing.T, payload json.RawMessage) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(payload, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func nativeStart(t *testing.T, p *nativeOldWriter, name string) durable.StartRequest {
	t.Helper()
	r := durable.StartRequest{Key: durable.Key{Namespace: "enrolled", WorkflowID: name, RunID: "r"}, RequestID: "start", WorkflowType: "wf", BuildID: "old", Queue: name}
	p.call(t, "start", map[string]any{"Start": r}, "")
	return r
}
func nativeClaim(t *testing.T, p *nativeOldWriter, r durable.StartRequest, kind durable.TaskKind, queue string) durable.Task {
	t.Helper()
	claim := durable.ClaimRequest{Namespace: r.Namespace, BuildID: r.BuildID, Queue: queue, Kind: kind, Owner: "old", LeaseDuration: time.Minute}
	task := nativeDecode[durable.Task](t, p.call(t, "claim", map[string]any{"Claim": claim}, ""))
	if task.ID == "" {
		t.Fatal("native claim returned no grant")
	}
	return task
}
func nativeSchedule(t *testing.T, p *nativeOldWriter, name string, tasks []durable.TaskSpec) durable.StartRequest {
	t.Helper()
	r := nativeStart(t, p, name)
	grant := nativeClaim(t, p, r, durable.TaskWorkflow, r.Queue)
	commit := durable.CommitRequest{Key: r.Key, RequestID: "schedule", ExpectedRevision: 1, Token: grant.Token(), Events: []durable.EventInput{{Type: "scheduled"}}, Tasks: tasks}
	p.call(t, "commit", map[string]any{"Commit": commit}, "")
	return r
}
func prepareNativeMutationCases(t *testing.T, p *nativeOldWriter) []nativeMutationCase {
	t.Helper()
	cases := prepareNativeTransitionCases(t, p)
	signalRun := nativeStart(t, p, "signal")
	signal := durable.SignalRequest{Key: signalRun.Key, RequestID: "signal", BuildID: "old", Name: "wake"}
	p.call(t, "signal", map[string]any{"Signal": signal}, "")
	signal.RequestID = "pending-signal"
	cases = append(cases, nativeMutationCase{"signal", map[string]any{"Signal": signal}})
	existing := durable.SignalWithStartRequest{Start: signalRun, Name: "wake"}
	existing.Start.RequestID = "existing-signal-start"
	p.call(t, "signal_start", map[string]any{"SignalStart": existing}, "")
	existing.Start.RequestID = "pending-existing"
	cases = append(cases, nativeMutationCase{"signal_start", map[string]any{"SignalStart": existing}})
	created := existing
	created.Start.WorkflowID = "created-signal-start"
	created.Start.Queue = "created-signal-start"
	p.call(t, "signal_start", map[string]any{"SignalStart": created}, "")
	created.Start.WorkflowID = "pending-created"
	cases = append(cases, nativeMutationCase{"signal_start", map[string]any{"SignalStart": created}})
	activity := nativeSchedule(t, p, "activity", []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: "activity"}, {ID: "unclaimed", Kind: durable.TaskActivity, Queue: "activity"}})
	a := nativeClaim(t, p, activity, durable.TaskActivity, "activity")
	minute := time.Minute
	keep := durable.CommitRequest{Key: activity.Key, RequestID: "attempt", ExpectedRevision: 2, Token: a.Token(), Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &minute, LeaseDuration: time.Minute, Heartbeat: &durable.HeartbeatConfig{}}}
	p.call(t, "commit", map[string]any{"Commit": keep}, "")
	a = nativeDecode[durable.Task](t, p.call(t, "get_task", map[string]any{"Key": activity.Key, "TaskID": a.ID}, ""))
	heartbeat := durable.HeartbeatRequest{Key: activity.Key, RequestID: "heartbeat", Token: a.Token(), Sequence: 1, LeaseDuration: time.Minute}
	p.call(t, "heartbeat", map[string]any{"Heartbeat": heartbeat}, "")
	heartbeat.RequestID = "pending-heartbeat"
	heartbeat.Sequence = 2
	cases = append(cases, nativeMutationCase{"heartbeat", map[string]any{"Heartbeat": heartbeat}}, nativeMutationCase{"claim", map[string]any{"Claim": durable.ClaimRequest{Namespace: "enrolled", BuildID: "old", Queue: "activity", Kind: durable.TaskActivity, Owner: "old", LeaseDuration: time.Minute}}})
	for _, name := range []string{"async-baseline", "async-pending"} {
		r := nativeSchedule(t, p, name, []durable.TaskSpec{{ID: "activity", Kind: durable.TaskActivity, Queue: name}})
		task := nativeClaim(t, p, r, durable.TaskActivity, name)
		init := durable.CommitRequest{Key: r.Key, RequestID: "attempt", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "attempt.started"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep, DeadlineAfter: &minute, LeaseDuration: time.Minute, Heartbeat: &durable.HeartbeatConfig{}}}
		p.call(t, "commit", map[string]any{"Commit": init}, "")
		secret := strings.Repeat("ab", 32)
		hash, err := durable.HashAsyncSecret(secret)
		if err != nil {
			t.Fatal(err)
		}
		handoff := durable.CommitRequest{Key: r.Key, RequestID: "await", ExpectedRevision: 3, Token: task.Token(), Events: []durable.EventInput{{Type: "activity.awaited"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskAwait, AsyncKeyHash: hash}}
		p.call(t, "commit", map[string]any{"Commit": handoff}, "")
		task = nativeDecode[durable.Task](t, p.call(t, "get_task", map[string]any{"Key": r.Key, "TaskID": task.ID}, ""))
		beat := durable.HeartbeatRequest{Key: r.Key, RequestID: "async-beat", Token: task.Token(), AsyncSecret: secret, Sequence: 1}
		finish := durable.CommitRequest{Key: r.Key, RequestID: "callback", ExpectedRevision: 4, Token: task.Token(), AsyncSecret: secret, Events: []durable.EventInput{{Type: "activity.completed"}}, State: durable.StateCompleted}
		if name == "async-baseline" {
			p.call(t, "heartbeat", map[string]any{"Heartbeat": beat}, "")
			p.call(t, "commit", map[string]any{"Commit": finish}, "")
		} else {
			cases = append(cases, nativeMutationCase{"heartbeat", map[string]any{"Heartbeat": beat}}, nativeMutationCase{"commit", map[string]any{"Commit": finish}})
		}
	}
	timer := nativeSchedule(t, p, "timer", []durable.TaskSpec{{ID: "one", Kind: durable.TaskTimer, Queue: "timer", AvailableAt: time.Now().Add(-time.Second)}, {ID: "two", Kind: durable.TaskTimer, Queue: "timer", AvailableAt: time.Now().Add(-time.Second)}})
	timerGrant := nativeClaim(t, p, timer, durable.TaskTimer, "timer")
	cases = append(cases, nativeMutationCase{"claim", map[string]any{"Claim": durable.ClaimRequest{Namespace: "enrolled", BuildID: "old", Queue: "timer", Kind: durable.TaskTimer, Owner: "old", LeaseDuration: time.Minute}}}, nativeMutationCase{"commit", map[string]any{"Commit": durable.CommitRequest{Key: timer.Key, RequestID: "timer-fire", ExpectedRevision: 2, Token: timerGrant.Token(), Events: []durable.EventInput{{Type: "timer.fired"}}}}})
	expired := nativeSchedule(t, p, "activity-timeout", []durable.TaskSpec{{ID: "one", Kind: durable.TaskActivity, Queue: "absent", DeadlineAfter: time.Microsecond}, {ID: "two", Kind: durable.TaskActivity, Queue: "absent", DeadlineAfter: time.Microsecond}})
	timeoutClaim := durable.TimeoutClaimRequest{Namespace: "enrolled", BuildID: "old", Owner: "timeout", LeaseDuration: time.Minute}
	timeout := nativeDecode[durable.Task](t, p.call(t, "claim_timeout", map[string]any{"TimeoutClaim": timeoutClaim}, ""))
	if timeout.ID == "" {
		t.Fatal("missing activity timeout grant")
	}
	cases = append(cases, nativeMutationCase{"claim_timeout", map[string]any{"TimeoutClaim": timeoutClaim}}, nativeMutationCase{"commit", map[string]any{"Commit": durable.CommitRequest{Key: expired.Key, RequestID: "timeout-result", ExpectedRevision: 2, Token: timeout.Token(), Events: []durable.EventInput{{Type: "activity.timed_out"}}}}})
	executionClaim := durable.ExecutionTimeoutClaimRequest{Namespace: "enrolled", Owner: "expiry", LeaseDuration: time.Minute}
	for _, name := range []string{"expiry-baseline", "expiry-held", "expiry-unclaimed"} {
		r := durable.StartRequest{Key: durable.Key{Namespace: "enrolled", WorkflowID: name, RunID: "r"}, RequestID: "start", WorkflowType: "wf", BuildID: "old", Queue: name, RunTimeout: time.Microsecond}
		p.call(t, "start", map[string]any{"Start": r}, "")
		if name == "expiry-unclaimed" {
			continue
		}
		grant := nativeDecode[durable.ExecutionTimeoutTask](t, p.call(t, "claim_execution_timeout", map[string]any{"ExecutionTimeoutClaim": executionClaim}, ""))
		if grant.RunID == "" {
			t.Fatal("missing execution timeout grant")
		}
		request := durable.ExecutionTimeoutRequest{Key: grant.Key, RequestID: "expire", Owner: grant.Owner, Epoch: grant.Epoch}
		if name == "expiry-baseline" {
			p.call(t, "execution_timeout", map[string]any{"Timeout": request}, "")
		} else {
			cases = append(cases, nativeMutationCase{"execution_timeout", map[string]any{"Timeout": request}})
		}
	}
	cases = append(cases, nativeMutationCase{"claim_execution_timeout", map[string]any{"ExecutionTimeoutClaim": executionClaim}})
	for _, name := range []string{"child-baseline", "child-held", "child-unclaimed"} {
		parent := nativeStart(t, p, name)
		grant := nativeClaim(t, p, parent, durable.TaskWorkflow, name)
		child := parent
		child.WorkflowID = name + "-child"
		child.Queue = child.WorkflowID
		request := durable.CommitRequest{Key: parent.Key, RequestID: "child", ExpectedRevision: 1, Token: grant.Token(), Events: []durable.EventInput{{Type: "decision"}}, Children: []durable.ChildStartSpec{{CommandID: "child", Start: child, ParentQueue: name, ParentClosePolicy: durable.ParentCloseAbandon}}}
		p.call(t, "commit", map[string]any{"Commit": request}, "")
		childGrant := nativeClaim(t, p, child, durable.TaskWorkflow, child.Queue)
		closeChild := durable.CommitRequest{Key: child.Key, RequestID: "close", ExpectedRevision: 1, Token: childGrant.Token(), State: durable.StateCompleted, Events: []durable.EventInput{{Type: "workflow.completed"}}}
		p.call(t, "commit", map[string]any{"Commit": closeChild}, "")
		if name == "child-unclaimed" {
			continue
		}
		message := nativeDecode[durable.ChildDelivery](t, p.call(t, "claim_child", map[string]any{"ChildClaim": durable.ChildDeliveryClaimRequest{Namespace: "enrolled", BuildID: "old", Owner: "child", LeaseDuration: time.Minute}}, ""))
		if message.ID == "" {
			t.Fatal("missing child delivery")
		}
		apply := durable.ChildDeliveryRequest{Source: message.Source, DeliveryID: message.ID, RequestID: "apply", Owner: message.Owner, Epoch: message.Epoch}
		if name == "child-baseline" {
			p.call(t, "child", map[string]any{"Child": apply}, "")
		} else {
			cases = append(cases, nativeMutationCase{"child", map[string]any{"Child": apply}})
		}
	}
	cases = append(cases, nativeMutationCase{"claim_child", map[string]any{"ChildClaim": durable.ChildDeliveryClaimRequest{Namespace: "enrolled", BuildID: "old", Owner: "child", LeaseDuration: time.Minute}}})
	return cases
}

func prepareNativeTransitionCases(t *testing.T, p *nativeOldWriter) []nativeMutationCase {
	t.Helper()
	var cases []nativeMutationCase
	for _, kind := range []durable.TaskKind{durable.TaskTimer, durable.TaskActivity} {
		name := "baseline-finish-" + string(kind)
		spec := durable.TaskSpec{ID: "task", Kind: kind, Queue: name, AvailableAt: time.Now().Add(-time.Second)}
		if kind == durable.TaskActivity {
			spec.DeadlineAfter = time.Microsecond
		}
		r := nativeSchedule(t, p, name, []durable.TaskSpec{spec})
		var task durable.Task
		if kind == durable.TaskActivity {
			task = nativeDecode[durable.Task](t, p.call(t, "claim_timeout", map[string]any{"TimeoutClaim": durable.TimeoutClaimRequest{Namespace: "enrolled", BuildID: "old", Owner: "timeout", LeaseDuration: time.Minute}}, ""))
		} else {
			task = nativeClaim(t, p, r, kind, name)
		}
		if task.ID == "" {
			t.Fatal("missing baseline completion grant")
		}
		p.call(t, "commit", map[string]any{"Commit": durable.CommitRequest{Key: r.Key, RequestID: "finish", ExpectedRevision: 2, Token: task.Token(), Events: []durable.EventInput{{Type: "task.completed"}}}}, "")
	}
	for _, name := range []string{"continue-baseline", "continue-pending", "child-create-pending"} {
		r := nativeStart(t, p, name)
		task := nativeClaim(t, p, r, durable.TaskWorkflow, name)
		request := durable.CommitRequest{Key: r.Key, RequestID: "decision", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}}
		if name == "child-create-pending" {
			child := r
			child.WorkflowID += "-child"
			child.Queue = child.WorkflowID
			request.Children = []durable.ChildStartSpec{{CommandID: "child", Start: child, ParentQueue: r.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}
		} else {
			request.State = durable.StateContinuedAsNew
			request.Continuation = &durable.ContinueSpec{RunID: "next", WorkflowType: r.WorkflowType, BuildID: r.BuildID, Queue: r.Queue}
		}
		if name == "continue-baseline" {
			p.call(t, "commit", map[string]any{"Commit": request}, "")
		} else {
			cases = append(cases, nativeMutationCase{"commit", map[string]any{"Commit": request}})
		}
	}
	return cases
}
