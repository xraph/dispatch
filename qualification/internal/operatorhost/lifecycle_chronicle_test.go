package operatorhost

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/dispatch/operator"
)

func TestNativeLifecycleChronicleOutage(t *testing.T) {
	if os.Getenv("DISPATCH_SINK_HOST_BINARY") == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("native Chronicle executable required")
		}
		t.Skip("native Chronicle executable required")
	}
	config, path, db := lifecycleSinkConfig(t)
	t.Setenv("DISPATCH_OPERATOR_DSN", config.DSNs["dispatch"])
	sink := startNativeChronicle(t, config, path)
	activation := filepath.Join(t.TempDir(), "activate")
	host := startNativeLifecycle(t, "chronicle-lifecycle", "--chronicle-config", path, "--run-workers=true", "--activation-file", activation)
	c := host.client
	c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "live-enroll"}, 200)
	for _, identity := range host.identities {
		build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
		c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "live-build-" + identity.BuildID, ExpectedVersion: "0"}, 200)
		c.command("durable.queryRuntimeRegister", operator.QueryRuntimeCommand{QueryRuntimeInput: operator.QueryRuntimeInput{BuildInput: build, RuntimeID: identity.RuntimeID}, RequestID: "live-runtime-" + identity.BuildID, ExpectedVersion: "0"}, 200)
	}
	if err := os.WriteFile(activation, []byte("start"), 0600); err != nil {
		t.Fatal(err)
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := db["dispatch"].QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
			t.Fatal("source observation failed")
		}
		return n
	}
	eventually := func(label string, check func() bool) {
		t.Helper()
		until := time.Now().Add(20 * time.Second)
		for time.Now().Before(until) {
			if check() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal(label)
	}
	for _, identity := range host.identities {
		eventually("registered worker did not start", func() bool {
			return data[operator.WorkerObservation](t, c.command("durable.workerStatus", operator.WorkerInput{BuildInput: operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}, RuntimeID: identity.RuntimeID}, 200)).State == "running"
		})
	}
	eventually("initial lifecycle delivery incomplete", func() bool {
		return count(`SELECT count(*) FROM dispatch_durable_outbox WHERE destination='chronicle' AND delivered_at IS NULL`) == 0
	})
	sink.kill(t)
	identity := host.identities[0]
	build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
	retire := operator.BuildRetirementInput{BuildInput: build, RequestID: "live-retire", ExpectedVersion: "1", ExpectedEpoch: "1"}
	accepted := c.command("durable.buildRetire", retire, 200)
	retired := data[operator.LifecycleAcceptance](t, accepted)
	for _, identity := range host.identities {
		in := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}, RuntimeID: identity.RuntimeID}, RequestID: "live-drain-" + identity.BuildID, OperationID: "live-stop-" + identity.BuildID, Deadline: time.Now().UTC().Add(time.Minute)}
		eventually("native worker drain incomplete", func() bool {
			return data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrain", in, 200)).Complete
		})
	}
	eventually("sink outage did not retain attempted lifecycle intent", func() bool {
		return count(`SELECT count(*) FROM dispatch_lifecycle_receipts r JOIN dispatch_durable_outbox o ON o.id=r.delivery_id WHERE r.request_id='live-retire' AND o.delivered_at IS NULL AND o.attempts>0 AND o.error_category<>''`) == 1
	})
	if replay := c.command("durable.buildRetire", retire, 200); !bytes.Equal(dataJSON(t, replay), dataJSON(t, accepted)) {
		t.Fatal("outage changed accepted lifecycle response")
	}
	if count(`SELECT count(*) FROM dispatch_lifecycle_receipts WHERE request_id='live-retire'`) != 1 {
		t.Fatal("outage retry duplicated acceptance")
	}
	sink = startNativeChronicle(t, config, path)
	eventually("drained workers stopped publication recovery", func() bool {
		return count(`SELECT count(*) FROM dispatch_durable_outbox WHERE destination='chronicle' AND delivered_at IS NULL`) == 0
	})
	for _, identity := range host.identities {
		state := data[operator.WorkerObservation](t, c.command("durable.workerStatus", operator.WorkerInput{BuildInput: operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}, RuntimeID: identity.RuntimeID}, 200))
		if !state.AdmissionClosed || state.InFlight != "0" {
			t.Fatal("recovery reopened worker admission")
		}
	}
	c.command("durable.buildResume", operator.BuildRetirementInput{BuildInput: build, RequestID: "live-resume", ExpectedVersion: retired.Version, ExpectedEpoch: retired.Epoch}, 200)
	eventually("resume audit delivery incomplete", func() bool {
		return count(`SELECT count(*) FROM dispatch_durable_outbox WHERE destination='chronicle' AND delivered_at IS NULL`) == 0
	})
	verifyLifecycleChronicle(t, config, db, c.host.Credentials["commander"].Subject)
	if count(`SELECT count(*) FROM dispatch_durable_outbox WHERE delivered_at IS NULL`) != 0 {
		t.Fatal("clean lifecycle profile left unrelated backlog")
	}
	if err := host.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-host.done:
		host.stopped = true
		if err != nil {
			t.Fatal("publisher shutdown reported incomplete")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("publisher shutdown timed out")
	}
	sink.kill(t)
	t.Log("native lifecycle accepted during Chronicle outage; stable source receipt recovered after restart while both workers remained drained; publisher shutdown completed")
}
func dataJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var result struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result.Data
}
