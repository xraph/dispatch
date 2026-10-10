package operatorhost

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/store/memory"
)

func TestMemoryDeploymentDeferralHTTP(t *testing.T) { testDeploymentDeferralHTTP(t, memory.New()) }
func TestPostgresDeploymentDeferralHTTP(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	testDeploymentDeferralHTTP(t, postgresCommands(t))
}
func testDeploymentDeferralHTTP(t *testing.T, store Store) {
	c := newConfiguredCommandClient(t, store, &LifecycleOptions{InstanceID: "deferral-host"})
	enrollAndStartDeployment(t, c)
	target := operator.BuildInput{Namespace: "production", BuildID: "operator-v2"}
	retired := data[operator.LifecycleAcceptance](t, c.command("durable.buildRetire", operator.BuildRetirementInput{BuildInput: target, RequestID: "deferral-retire", ExpectedVersion: "1", ExpectedEpoch: "1"}, 200))
	for _, kind := range []string{"deferred-child", "deferred-continue"} {
		key := durable.Key{Namespace: "production", WorkflowID: kind, RunID: "run-1"}
		c.command("durable.start", operator.StartInput{Key: key, RequestID: "start-" + kind, WorkflowType: kind, BuildID: "operator-v1", Queue: "operator"}, 200)
		c.command("durable.signal", durable.SignalRequest{Key: key, RequestID: "handoff-" + kind, BuildID: "operator-v1", Name: "handoff"}, 200)
	}
	waitDeferrals(t, c, true)
	c.command("durable.buildResume", operator.BuildRetirementInput{BuildInput: target, RequestID: "deferral-resume", ExpectedVersion: retired.Version, ExpectedEpoch: retired.Epoch}, 200)
	waitDeferrals(t, c, false)
}

func waitDeferrals(t *testing.T, c *commandClient, active bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		matched := 0
		for _, kind := range []string{"deferred-child", "deferred-continue"} {
			status, raw := call(t, c.server.URL, c.host.Credentials["reader"].Token, "durable.tasks", map[string]any{"namespace": "production", "workflow_id": kind, "run_id": "run-1", "limit": 100}, false)
			if status != 200 {
				t.Fatalf("task read HTTP %d: %s", status, raw)
			}
			page := data[operator.Page[operator.Task]](t, raw)
			for _, task := range page.Items {
				d := task.Deferral
				if d != nil && d.Active == active && d.TargetBuildID == "operator-v2" && d.Count != "0" {
					if d.TargetRetirementEpoch == "" || d.SourceEpoch == "" || (kind == "deferred-child" && (d.CommandID == "" || d.ReferenceKind != durable.DeferralChild)) || (kind == "deferred-continue" && (d.CommandID != "" || d.ReferenceKind != durable.DeferralContinuation)) {
						t.Fatal("deferral lost exact references")
					}
					matched++
					break
				}
			}
		}
		if matched == 2 {
			t.Logf("secured task reads show child and continuation active=%t deferrals", active)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child/continuation active=%t deferrals not visible", active)
}

func enrollAndStartDeployment(t *testing.T, c *commandClient) {
	t.Helper()
	c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "deferral-enroll"}, 200)
	for _, identity := range c.host.RuntimeIdentities() {
		build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
		c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "deferral-build-" + identity.BuildID, ExpectedVersion: "0"}, 200)
		c.command("durable.queryRuntimeRegister", operator.QueryRuntimeCommand{QueryRuntimeInput: operator.QueryRuntimeInput{BuildInput: build, RuntimeID: identity.RuntimeID}, RequestID: "deferral-runtime-" + identity.RuntimeID, ExpectedVersion: "0"}, 200)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stop := c.host.StartWorkers(ctx)
	t.Cleanup(func() {
		cancel()
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
}
