package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

type nativeLifecycleHost struct {
	statePath  string
	client     *commandClient
	identities []durable.QueryRuntimeIdentity
	command    *exec.Cmd
	done       chan error
	stopped    bool
	logs       *bytes.Buffer
}

func startNativeLifecycle(t *testing.T, instance string, extra ...string) *nativeLifecycleHost {
	t.Helper()
	binary := os.Getenv("DISPATCH_OPERATOR_BINARY")
	if binary == "" || os.Getenv("DISPATCH_OPERATOR_DSN") == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("native binary and dedicated PostgreSQL required")
		}
		t.Skip("native binary and dedicated PostgreSQL required")
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	args := append([]string{"--state-file", statePath, "--lifecycle-instance", instance, "--run-workers=false"}, extra...)
	command := exec.CommandContext(t.Context(), binary, args...)
	logs := new(bytes.Buffer)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	n := &nativeLifecycleHost{statePath: statePath, command: command, done: make(chan error, 1), logs: logs}
	go func() { n.done <- command.Wait() }()
	t.Cleanup(func() {
		if !n.stopped {
			_ = command.Process.Kill()
			<-n.done
			n.stopped = true
		}
		if t.Failed() && n.client == nil {
			for _, line := range nativeQueryDiagnostics(logs.String(), nil) {
				t.Log(line)
			}
		}
	})

	var state struct {
		URL         string                         `json:"url"`
		Credentials map[string]Credential          `json:"credentials"`
		Runtimes    []durable.QueryRuntimeIdentity `json:"runtimes"`
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-n.done:
			n.stopped = true
			t.Fatal("native lifecycle exited before readiness", err)
		default:
		}
		raw, err := os.ReadFile(statePath)
		if err == nil && json.Unmarshal(raw, &state) == nil && state.URL != "" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if state.URL == "" || len(state.Runtimes) != 2 {
		t.Fatal("native lifecycle did not publish process identities")
	}
	h := &Host{Credentials: state.Credentials}
	h.Machine.Secret = "not-a-real-native-credential"
	n.client = &commandClient{t: t, host: h, server: &httptest.Server{URL: state.URL}}
	status, raw := n.client.request(http.MethodGet, "/api/dashboard/v1/csrf", h.Credentials["commander"].Token, nil)
	var token struct {
		Token string `json:"token"`
	}
	if status != 200 || json.Unmarshal(raw, &token) != nil || token.Token == "" {
		t.Fatal("native CSRF failed")
	}
	n.client.csrf = token.Token
	n.identities = state.Runtimes
	t.Cleanup(func() {
		if !n.stopped {
			_ = command.Process.Kill()
			<-n.done
			n.stopped = true
		}
		if t.Failed() {
			for _, line := range nativeQueryDiagnostics(logs.String(), state.Credentials) {
				t.Log(line)
			}
		}
		for _, credential := range state.Credentials {
			if bytes.Contains(logs.Bytes(), []byte(credential.Token)) {
				t.Error("credential appeared in native log")
			}
		}
	})
	return n
}

func (n *nativeLifecycleHost) kill(t *testing.T) {
	t.Helper()
	if err := n.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-n.done
	n.stopped = true
}

func TestNativeLifecycleRestartKeepsOriginalDrainUnknown(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	first := startNativeLifecycle(t, "native-physical-instance")
	c := first.client
	c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "native-enroll"}, 200)
	identity := first.identities[0]
	build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
	c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "native-build", ExpectedVersion: "0"}, 200)
	in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: identity.RuntimeID}
	registration := operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "native-register", ExpectedVersion: "0"}
	c.command("durable.queryRuntimeRegister", registration, 200)
	probe := defaultProbes(identity.BuildID)[0]
	c.command("durable.start", operator.StartInput{Key: probe.Key, RequestID: "native-history", WorkflowType: "operator", BuildID: identity.BuildID, Queue: "operator", Input: []byte("history")}, 200)
	rawRejection := c.command("durable.queryRuntimeVerify", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "native-open-probe", ExpectedVersion: "1"}, 409)
	for _, private := range []string{"host_drain", "admission", "observed_at", "verified_at"} {
		if bytes.Contains(rawRejection, []byte(private)) {
			t.Fatal("private diagnostic leaked through HTTP")
		}
	}
	drain := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: build, RuntimeID: identity.RuntimeID}, RequestID: "native-drain", OperationID: "native-stop", Deadline: time.Now().UTC().Add(time.Minute)}
	c.command("durable.workerDrain", drain, 200)
	result := data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrain", drain, 200))
	if !result.Complete {
		t.Fatalf("native prestart drain incomplete: %+v", result)
	}
	proof := data[operator.QueryRuntimeAcceptance](t, c.command("durable.queryRuntimeVerify", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "native-verify", ExpectedVersion: "1"}, 200))
	if proof.Binding.ProofID == "" {
		t.Fatal("native historical proof missing")
	}
	first.kill(t)
	records := nativeQueryDiagnostics(first.logs.String(), first.client.host.Credentials)
	if len(records) != 1 || !strings.Contains(records[0], `"stage":"host_drain"`) || !strings.Contains(records[0], `"reason":"admission"`) {
		t.Fatalf("native rejection diagnostic missing: %v", records)
	}
	replacement := startNativeLifecycle(t, "native-physical-instance")
	current := replacement.identities[0]
	if current.RuntimeID == identity.RuntimeID || current.BuildIdentity != identity.BuildIdentity || current.InstanceID != identity.InstanceID {
		t.Fatal("native replacement identity mismatch")
	}
	replay := data[operator.WorkerDrainAcceptance](t, replacement.client.command("durable.workerDrain", drain, 200))
	if replay.Process != "unknown" || replay.Complete || replay.RuntimeID != identity.RuntimeID || replay.OperationID != drain.OperationID || !replay.Deadline.Equal(drain.Deadline) {
		t.Fatalf("old drain redirected or rewritten: %+v", replay)
	}
	replacement.client.command("durable.queryRuntimeRegister", registration, 200)
	state := data[operator.WorkerObservation](t, replacement.client.command("durable.workerStatus", operator.WorkerInput{BuildInput: build, RuntimeID: current.RuntimeID}, 200))
	if state.AdmissionClosed {
		t.Fatal("old drain closed replacement admission")
	}
	t.Logf("native original=%s replacement=%s instance=%s artifact=%s configuration=%s proof=%s old-drain=unknown", identity.RuntimeID, current.RuntimeID, identity.InstanceID, identity.BuildIdentity.ArtifactDigest, identity.BuildIdentity.ConfigurationDigest, proof.Binding.ProofID)
}

// Only joined-child structured diagnostics are retained. Other output is not a diagnostic.
func nativeQueryDiagnostics(output string, credentials map[string]Credential) []string {
	for _, credential := range credentials {
		if credential.Token != "" {
			output = strings.ReplaceAll(output, credential.Token, "[redacted]")
		}
	}
	var records []string
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, queryDiagnosticPrefix) {
			continue
		}
		var d durable.QueryRejectionDiagnostic
		if json.Unmarshal([]byte(strings.TrimPrefix(line, queryDiagnosticPrefix)), &d) != nil {
			continue
		}
		if len(records) == queryDiagnosticRecords {
			copy(records, records[1:])
			records = records[:queryDiagnosticRecords-1]
		}
		records = append(records, queryDiagnosticLine(d))
	}
	return records
}

func TestNativeQueryDiagnosticFailureCapture(t *testing.T) {
	if os.Getenv("DISPATCH_QUERY_DIAGNOSTIC_CHILD") == "1" {
		t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
		n := startNativeLifecycle(t, "diagnostic-failure")
		c := n.client
		identity := n.identities[0]
		c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "enroll"}, 200)
		build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
		c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "build", ExpectedVersion: "0"}, 200)
		in := operator.QueryRuntimeInput{BuildInput: build, RuntimeID: identity.RuntimeID}
		c.command("durable.queryRuntimeRegister", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "register", ExpectedVersion: "0"}, 200)
		// Deliberately fail the actual HTTP assertion. Cleanup must join and retain it.
		c.command("durable.queryRuntimeVerify", operator.QueryRuntimeCommand{QueryRuntimeInput: in, RequestID: "verify", ExpectedVersion: "1"}, 200)
		t.Fatal("expected rejection did not fail the child")
	}
	if os.Getenv("DISPATCH_OPERATOR_BINARY") == "" || os.Getenv("DISPATCH_OPERATOR_DSN") == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("native binary and dedicated PostgreSQL required")
		}
		t.Skip("native binary and dedicated PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeQueryDiagnosticFailureCapture$", "-test.v", "-test.timeout=30s")
	child.Env = append(os.Environ(), "DISPATCH_QUERY_DIAGNOSTIC_CHILD=1")
	output, err := child.CombinedOutput()
	if err == nil || ctx.Err() != nil || !bytes.Contains(output, []byte(`"stage":"host_drain"`)) || !bytes.Contains(output, []byte(`"reason":"admission"`)) || !bytes.Contains(output, []byte("HTTP 409 want 200")) {
		t.Fatalf("failed child did not retain its private rejection (exit=%v, watchdog=%v, bytes=%d)", err, ctx.Err(), len(output))
	}
	for _, line := range strings.Split(string(output), "\n") {
		if start := strings.Index(line, queryDiagnosticPrefix); start >= 0 {
			for _, record := range nativeQueryDiagnostics(line[start:], nil) {
				t.Log(record)
			}
		}
	}
	t.Log("actual failed native HTTP child retained host_drain/admission after cleanup")
}
