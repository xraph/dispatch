package operatorhost

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

func TestPostgresLateChildCompletionRetainsDrainedParentObligation(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	c := newConfiguredCommandClient(t, postgresCommands(t), &LifecycleOptions{InstanceID: "late-child"})
	enrollAndStartDeployment(t, c)
	parent := durable.Key{Namespace: "production", WorkflowID: "late-child-parent", RunID: "run-1"}
	c.command("durable.start", operator.StartInput{Key: parent, RequestID: "parent-start", WorkflowType: "deferred-child", BuildID: "operator-v1", Queue: "operator"}, 200)
	c.command("durable.signal", durable.SignalRequest{Key: parent, RequestID: "parent-handoff", BuildID: "operator-v1", Name: "handoff"}, 200)
	var child durable.Key
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		status, raw := call(t, c.server.URL, c.host.Credentials["reader"].Token, "durable.children", parent, false)
		if status != 200 {
			t.Fatalf("children HTTP %d", status)
		}
		children := data[operator.Children](t, raw)
		if len(children.Items) == 1 {
			link := children.Items[0]
			child = durable.Key{Namespace: link.Namespace, WorkflowID: link.WorkflowID, RunID: link.RunID}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if child.RunID == "" {
		t.Fatal("real child did not start")
	}
	build := operator.BuildInput{Namespace: "production", BuildID: "operator-v1"}
	retired := data[operator.LifecycleAcceptance](t, c.command("durable.buildRetire", operator.BuildRetirementInput{BuildInput: build, RequestID: "parent-retire", ExpectedVersion: "1", ExpectedEpoch: "1"}, 200))
	worker := c.host.runtime.workers[build.BuildID]
	in := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: build, RuntimeID: worker.Status().RuntimeID}, RequestID: "parent-drain", OperationID: "parent-stop", Deadline: time.Now().UTC().Add(time.Minute)}
	until = time.Now().Add(5 * time.Second)
	for !data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrain", in, 200)).Complete {
		if time.Now().After(until) {
			t.Fatal("parent worker did not drain")
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.command("durable.signal", durable.SignalRequest{Key: child, RequestID: "child-finish", BuildID: "operator-v2", Name: "finish"}, 200)
	until = time.Now().Add(10 * time.Second)
	var facts operator.BuildLifecycle
	for time.Now().Before(until) {
		facts = data[operator.BuildLifecycle](t, c.command("durable.build", build, 200))
		if facts.Blockers["pending_child_deliveries"] == "1" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if facts.Blockers["pending_child_deliveries"] != "1" || facts.Blockers["open_executions"] != "1" || !worker.Status().AdmissionClosed {
		t.Fatalf("late child lost drained parent obligation: %+v", facts)
	}
	c.command("durable.buildFinalize", operator.BuildRetirementInput{BuildInput: build, RequestID: "parent-finalize", ExpectedVersion: retired.Version, ExpectedEpoch: retired.Epoch}, 409)
	t.Log("child completed on active target after parent drain; pending delivery and open parent prevented finalization")
}
