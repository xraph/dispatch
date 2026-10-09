package operatorhost

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRunnableFixture(t *testing.T) {
	binary := os.Getenv("DISPATCH_OPERATOR_BINARY")
	if binary == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("operator binary required")
		}
		t.Skip("operator binary required")
	}
	stateFile := filepath.Join(t.TempDir(), "state.json")
	command := exec.CommandContext(t.Context(), binary, "--state-file", stateFile)
	var logs bytes.Buffer
	command.Stdout = &logs
	command.Stderr = &logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = command.Process.Kill()
			<-done
		}
	})
	var state struct {
		URL         string                `json:"url"`
		Credentials map[string]Credential `json:"credentials"`
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case processErr := <-done:
			exited = true
			t.Fatal("fixture exited before readiness", processErr)
		default:
		}
		raw, err := os.ReadFile(stateFile)
		if err == nil && json.Unmarshal(raw, &state) == nil && state.URL != "" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if state.URL == "" {
		t.Fatal("fixture did not become ready")
	}
	info, err := os.Stat(stateFile)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("credential file permissions", err)
	}
	status, _ := call(t, state.URL, state.Credentials["reader"].Token, "durable.execution", map[string]any{"namespace": "production", "workflow_id": "invoice", "run_id": "run-2"}, false)
	if status != 200 {
		t.Fatal(status)
	}
	if err = command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		exited = true
		if err != nil {
			t.Fatal("fixture exit", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("fixture failed to stop")
	}
	if _, err = os.Stat(stateFile); !os.IsNotExist(err) {
		t.Fatal("private state retained", err)
	}
	for _, credential := range state.Credentials {
		if bytes.Contains(logs.Bytes(), []byte(credential.Token)) {
			t.Fatal("credential in process logs")
		}
	}
	t.Logf("native fixture pid=%d stopped; private state removed", command.Process.Pid)
}
