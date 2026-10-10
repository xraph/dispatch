package operatorhost

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/store/memory"
)

func TestMemoryDeploymentBlockersAndLateCallbacks(t *testing.T) {
	testDeploymentBlockers(t, memory.New())
}
func TestPostgresDeploymentBlockersAndLateCallbacks(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	testDeploymentBlockers(t, postgresCommands(t))
}
func testDeploymentBlockers(t *testing.T, store Store) {
	c := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "blocker-host"})
	enrollAndStartDeployment(t, c)
	build := operator.BuildInput{Namespace: "production", BuildID: "operator-v1"}
	keys := map[string]durable.Key{}
	for _, kind := range []string{"sleep", "async", "retry"} {
		keys[kind] = durable.Key{Namespace: "production", WorkflowID: "blocker-" + kind, RunID: "run-1"}
		c.command("durable.start", operator.StartInput{Key: keys[kind], RequestID: "blocker-" + kind, WorkflowType: kind, BuildID: build.BuildID, Queue: "operator"}, 200)
	}
	deadline := time.Now().Add(10 * time.Second)
	observed := map[string]bool{}
	for time.Now().Before(deadline) {
		for kind, key := range keys {
			status, raw := call(t, c.server.URL, c.host.Credentials["reader"].Token, "durable.tasks", key, false)
			if status != 200 {
				t.Fatal(status)
			}
			tasks := data[operator.Page[operator.Task]](t, raw)
			for _, task := range tasks.Items {
				switch kind {
				case "sleep":
					observed[kind] = observed[kind] || (task.Kind == durable.TaskTimer && task.AvailableAt.After(time.Now()))
				case "async":
					observed[kind] = observed[kind] || task.State == "awaiting_callback"
				case "retry":
					observed[kind] = observed[kind] || (task.Kind == durable.TaskActivity && task.Attempt != "0" && task.AvailableAt.After(time.Now()))
				}
			}
		}
		if observed["sleep"] && observed["async"] && observed["retry"] {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !observed["sleep"] || !observed["async"] || !observed["retry"] {
		t.Fatalf("persisted blockers missing: %+v", observed)
	}
	retired := data[operator.LifecycleAcceptance](t, c.command("durable.buildRetire", operator.BuildRetirementInput{BuildInput: build, RequestID: "blocker-retire", ExpectedVersion: "1", ExpectedEpoch: "1"}, 200))
	finalize := operator.BuildRetirementInput{BuildInput: build, RequestID: "blocked-finalize", ExpectedVersion: retired.Version, ExpectedEpoch: retired.Epoch}
	c.command("durable.buildFinalize", finalize, 409)
	facts := data[operator.BuildLifecycle](t, c.command("durable.build", build, 200))
	if !facts.HasBlockers || facts.Blockers["open_executions"] != "3" || facts.Blockers["async_callbacks"] != "1" {
		t.Fatalf("incorrect persisted blockers: %+v", facts)
	}
	if _, err := drt.NewWorker(store, drt.Options{Namespace: "production", BuildID: build.BuildID, Queue: "operator", Owner: "incompatible-rollback", Retirement: &drt.RetirementOptions{InstallationID: "operator-host", WriterProtocol: 0}}); !errors.Is(err, durable.ErrWriterCompatibility) {
		t.Fatalf("old writer rollback admitted: %v", err)
	}
	worker := c.host.runtime.workers[build.BuildID]
	in := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: build, RuntimeID: worker.Status().RuntimeID}, RequestID: "blocker-drain", OperationID: "blocker-stop", Deadline: time.Now().UTC().Add(5 * time.Second)}
	deadline = time.Now().Add(5 * time.Second)
	complete := false
	for time.Now().Before(deadline) {
		result := data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrain", in, 200))
		if result.Complete {
			complete = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !complete {
		t.Fatal("local worker did not drain")
	}
	c.host.runtime.mu.Lock()
	handles := append([]drt.AsyncActivityHandle(nil), c.host.runtime.handles[keys["async"]]...)
	c.host.runtime.mu.Unlock()
	if len(handles) != 1 {
		t.Fatal("callback handle missing")
	}
	completion := operator.CompletionInput{Handle: callbackHandle(handles[0]), RequestID: "late-complete", Output: []byte("completed-after-drain")}
	first := c.callback("complete", c.host.Machine.Secret, completion, 200)
	if again := c.callback("complete", c.host.Machine.Secret, completion, 200); again != first {
		t.Fatal("late callback replay changed")
	}
	c.command("durable.signal", durable.SignalRequest{Key: keys["sleep"], BuildID: build.BuildID, RequestID: "late-signal", Name: "late"}, 200)
	c.command("durable.buildFinalize", finalize, 409)
	facts = data[operator.BuildLifecycle](t, c.command("durable.build", build, 200))
	if !facts.HasBlockers || facts.Blockers["async_callbacks"] != "0" || facts.Blockers["open_executions"] != "3" {
		t.Fatalf("late delivery lost obligations: %+v", facts)
	}
	resume := finalize
	resume.RequestID = "blocker-resume"
	c.command("durable.buildResume", resume, 200)
	compatibility := data[operator.Compatibility](t, c.command("durable.compatibility", operator.NamespaceLifecycleInput{Namespace: "production"}, 200))
	if !compatibility.Enrolled || compatibility.WriterProtocol != "1" || !worker.Status().AdmissionClosed {
		t.Fatal("resume lowered floor or reopened process")
	}
	t.Logf("persisted sleep/retry/callback blockers survived drain; late callback and signal accepted; finalize refused; protocol floor preserved")
}
