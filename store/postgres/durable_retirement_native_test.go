package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable"
)

type nativeOldWriter struct {
	cmd     *exec.Cmd
	in      io.WriteCloser
	encoder *json.Encoder
	decoder *json.Decoder
}
type nativeOldResponse struct {
	Operation string
	Value     json.RawMessage
	Failed    bool
	SQLState  string
}

func startNativeOldWriter(t *testing.T, binary, dsn string) *nativeOldWriter {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(os.Environ(), "DISPATCH_OLDWRITER_DSN="+dsn)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &nativeOldWriter{cmd: cmd, in: in, encoder: json.NewEncoder(in), decoder: json.NewDecoder(out)}
	t.Cleanup(func() { _ = p.in.Close(); _ = cmd.Wait(); cancel() })
	return p
}
func (p *nativeOldWriter) call(t *testing.T, operation string, fields map[string]any, want string) json.RawMessage {
	t.Helper()
	if fields == nil {
		fields = map[string]any{}
	}
	fields["Operation"] = operation
	if err := p.encoder.Encode(fields); err != nil {
		t.Fatal(err)
	}
	var r nativeOldResponse
	if err := p.decoder.Decode(&r); err != nil {
		t.Fatal(err)
	}
	if r.Operation != operation || r.SQLState != want || r.Failed != (want != "") {
		t.Fatalf("native operation %s: failed=%t SQLSTATE=%s want=%s", operation, r.Failed, r.SQLState, want)
	}
	t.Logf("native old operation=%s failed=%t SQLSTATE=%s", operation, r.Failed, r.SQLState)
	return r.Value
}

func TestRetirementNativeOldWriter(t *testing.T) {
	binary := os.Getenv("DISPATCH_OLDWRITER_BINARY")
	dsn := os.Getenv("DISPATCH_LIFECYCLE_TEST_DSN")
	if binary == "" || dsn == "" {
		if os.Getenv("DISPATCH_LIFECYCLE_REQUIRED") == "1" {
			t.Fatal("native old writer binary and dedicated PostgreSQL required")
		}
		t.Skip("native old writer binary and dedicated PostgreSQL required")
	}
	// A separate database lets the real old artifact migrate first. The same old
	// process and pool stay live across current expansion and floor enrollment.
	admin := retirementConn(t, dsn)
	name := fmt.Sprintf("dispatch_old_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP DATABASE "+identifier+" WITH (FORCE)") })
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid fixture DSN")
	}
	parsed.Path = "/" + name
	nativeDSN := parsed.String()
	old := startNativeOldWriter(t, binary, nativeDSN)
	old.call(t, "migrate", nil, "")
	for _, namespace := range []string{"enrolled", "control"} {
		old.call(t, "namespace", map[string]any{"Namespace": durable.NamespaceConfig{InstallationID: "i", Namespace: namespace, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}}, "")
	}
	start := durable.StartRequest{Key: durable.Key{Namespace: "enrolled", WorkflowID: "old-run", RunID: "r"}, RequestID: "start", WorkflowType: "wf", BuildID: "old", Queue: "q"}
	old.call(t, "start", map[string]any{"Start": start}, "")
	claim := durable.ClaimRequest{Namespace: "enrolled", Queue: "q", BuildID: "old", Kind: durable.TaskWorkflow, Owner: "old-process", LeaseDuration: time.Minute}
	var task durable.Task
	if err = json.Unmarshal(old.call(t, "claim", map[string]any{"Claim": claim}, ""), &task); err != nil {
		t.Fatal(err)
	}
	old.call(t, "renew", map[string]any{"Key": start.Key, "Token": task.Token()}, "")
	additional := prepareNativeMutationCases(t, old)
	current := openWakeStore(t, nativeDSN)
	expanded := start
	expanded.WorkflowID = "expanded"
	old.call(t, "start", map[string]any{"Start": expanded}, "")
	request := durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: "enrolled"}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}
	if _, err = current.EnrollRetirement(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	after := start
	after.WorkflowID = "refused"
	checks := func(p *nativeOldWriter) {
		for _, c := range additional {
			p.call(t, c.operation, c.fields, "DL001")
		}
		p.call(t, "start", map[string]any{"Start": after}, "DL001")
		p.call(t, "start", map[string]any{"Start": start}, "DL001")
		p.call(t, "claim", map[string]any{"Claim": claim}, "DL001")
		p.call(t, "renew", map[string]any{"Key": start.Key, "Token": task.Token()}, "DL001")
		p.call(t, "get", map[string]any{"Key": start.Key}, "")
		p.call(t, "history", map[string]any{"Key": start.Key}, "")
	}
	checks(old)
	control := after
	control.Namespace = "control"
	old.call(t, "start", map[string]any{"Start": control}, "")
	checks(startNativeOldWriter(t, binary, nativeDSN))
	// Refusals cannot append evidence or consume the eligible workflow grant.
	pg := retirementConn(t, nativeDSN)
	var count int
	if err = pg.QueryRow(t.Context(), `SELECT count(*) FROM dispatch_executions WHERE namespace='enrolled' AND workflow_id='refused'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("refused root persisted: %d %v", count, err)
	}
	if _, err = current.RenewTask(t.Context(), start.Key, task.Token(), time.Minute); err != nil {
		t.Fatal(err)
	}
	currentTask, err := current.ClaimTask(t.Context(), claim)
	if err != nil || currentTask == nil {
		t.Fatalf("capable claim after refusal: %+v %v", currentTask, err)
	}
}
