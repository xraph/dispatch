package sinkhost

import (
	"encoding/json"
	"os"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
)

func (r *processRig) callbackWhileSinkStopped(role string) {
	r.t.Helper()
	for _, path := range []string{"/v1/jobs/not-a-job/cancel", "/v1/workflows/not-a-workflow/cancel"} {
		r.post("dispatch", path, r.c.Credentials["operator"].Secret, []byte(`{}`), 404, nil)
	}
	workflow := "callback-outage-" + role
	start := operator.StartInput{Key: durable.Key{Namespace: r.c.Binding.Namespace, WorkflowID: workflow, RunID: "run"}, RequestID: "start", WorkflowType: "callback", BuildID: "callback-v1", Queue: "callbacks", Input: []byte("qualification-payload-must-not-leak")}
	raw, err := json.Marshal(start)
	if err != nil {
		r.t.Fatal(err)
	}
	r.post("dispatch", "/durable/start", r.c.Credentials["operator"].Secret, raw, 202, nil)
	var handle drt.AsyncActivityHandle
	r.eventually(func() bool {
		handleRaw, e := os.ReadFile(callbackFile(r.c.CallbackDirectory, workflow))
		return e == nil && json.Unmarshal(handleRaw, &handle) == nil
	})
	wire := operator.CallbackHandle{Version: handle.Version, Key: handle.Key, BuildID: handle.BuildID, Secret: handle.Secret, InitialHeartbeatSequence: handle.InitialHeartbeatSequence, Token: operator.CallbackToken{TaskID: handle.Token.TaskID, Owner: handle.Token.Owner, Epoch: handle.Token.Epoch, LeaseKind: handle.Token.LeaseKind}}
	before := r.count(role, "SELECT count(*) FROM "+role+"_acceptances")
	heartbeat := operator.HeartbeatInput{Handle: wire, RequestID: "heartbeat", Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("qualification-payload-must-not-leak")}
	raw, err = json.Marshal(heartbeat)
	if err != nil {
		r.t.Fatal(err)
	}
	r.post("dispatch", "/v1/durable/activities/heartbeat", r.c.Credentials["operator"].Secret, raw, 200, nil)
	complete := operator.CompletionInput{Handle: wire, RequestID: "complete", Output: []byte("qualification-payload-must-not-leak")}
	raw, err = json.Marshal(complete)
	if err != nil {
		r.t.Fatal(err)
	}
	r.post("dispatch", "/v1/durable/activities/complete", r.c.Credentials["operator"].Secret, raw, 200, nil)
	r.eventually(func() bool {
		return r.count("dispatch", "SELECT count(*) FROM dispatch_executions WHERE workflow_id=$1 AND state='completed'", workflow) == 1
	})
	r.equal(before, r.count(role, "SELECT count(*) FROM "+role+"_acceptances"), "stopped sink callback receipts")
	pending := r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE workflow_id=$1 AND destination=$2 AND delivered_at IS NULL", workflow, role)
	if pending == 0 {
		r.t.Fatal("callback had no required pending local intents")
	}
	revision := r.count("dispatch", "SELECT revision FROM dispatch_executions WHERE workflow_id=$1", workflow)
	r.post("dispatch", "/v1/durable/activities/complete", r.c.Credentials["operator"].Secret, raw, 200, nil)
	r.equal(revision, r.count("dispatch", "SELECT revision FROM dispatch_executions WHERE workflow_id=$1", workflow), "callback replay revision")
	r.t.Logf("%s outage: genuine async heartbeat/completion accepted with %d local pending intents and no remote acceptance", role, pending)
}
