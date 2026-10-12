package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the actual CLI, stock HTTP boundary and private-file shutdown in both modes.
func TestNativeWorkerModes(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "operatorhost")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("fixture build failed: %v\n%s", err, output)
	}
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			args := []string{"--state-file", stateFile}
			if !enabled {
				args = append(args, "--run-workers=false")
			}
			processContext, cancelProcess := context.WithTimeout(context.Background(), 30*time.Second)
			cmd := exec.CommandContext(processContext, binary, args...)
			cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
			if err := cmd.Start(); err != nil {
				cancelProcess()
				t.Fatal("fixture process did not start")
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			t.Cleanup(func() {
				defer cancelProcess()
				_ = cmd.Process.Signal(os.Interrupt)
				select {
				case err := <-exited:
					if err != nil {
						t.Error("fixture did not shut down cleanly")
					}
				case <-time.After(10 * time.Second):
					_ = cmd.Process.Kill()
					<-exited
					t.Error("fixture shutdown timed out")
				}
				if _, err := os.Stat(stateFile); !os.IsNotExist(err) {
					t.Error("private state file remains after shutdown")
				}
			})
			var state struct {
				URL         string `json:"url"`
				Credentials map[string]struct {
					Token string `json:"token"`
				} `json:"credentials"`
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				raw, err := os.ReadFile(stateFile)
				if err == nil && json.Unmarshal(raw, &state) == nil && state.URL != "" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("fixture state was not ready")
				}
				time.Sleep(10 * time.Millisecond)
			}
			info, err := os.Stat(stateFile)
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatal("fixture state must remain private")
			}
			client := &nativeClient{t: t, url: state.URL, token: state.Credentials["commander"].Token}
			var csrf struct {
				Token string `json:"token"`
			}
			if json.Unmarshal(client.request(http.MethodGet, "/csrf", nil), &csrf) != nil || csrf.Token == "" {
				t.Fatal("real CSRF token missing")
			}
			client.csrf = csrf.Token
			key := map[string]any{"namespace": "production", "workflow_id": "native-" + name, "run_id": "run"}
			start := map[string]any{"namespace": "production", "workflow_id": "native-" + name, "run_id": "run", "request_id": "start", "workflow_type": "operator", "build_id": "operator-v1", "queue": "operator", "input": "ZXhhY3Q="}
			accepted := client.envelope("command", "durable.start", start)
			if accepted["status"] != "accepted" {
				t.Fatal("start was not accepted")
			}
			// The default mode must really execute. Disabled workers leave only the start.
			deadline = time.Now().Add(5 * time.Second)
			for {
				current := client.envelope("query", "durable.execution", key)
				if enabled && current["revision"] != "1" {
					break
				}
				if !enabled {
					time.Sleep(100 * time.Millisecond)
					current = client.envelope("query", "durable.execution", key)
					if current["revision"] != "1" {
						t.Fatal("disabled worker executed workflow")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("default worker did not execute workflow")
				}
				time.Sleep(10 * time.Millisecond)
			}
			query := map[string]any{"namespace": "production", "workflow_id": "native-" + name, "run_id": "run", "build_id": "operator-v1", "name": "status"}
			if output := client.envelope("query", "durable.query", query); output["output"] != "ZXhhY3Q=" {
				t.Fatal("registered runtime query unavailable")
			}
			cancel := map[string]any{"namespace": "production", "workflow_id": "native-" + name, "run_id": "run", "build_id": "operator-v1", "request_id": "cancel"}
			if outcome := client.envelope("command", "durable.cancel", cancel); outcome["status"] != "cancellation_requested" {
				t.Fatal("cancellation acceptance changed")
			}
			if !enabled {
				time.Sleep(100 * time.Millisecond)
				current := client.envelope("query", "durable.execution", key)
				if current["state"] != "running" || current["revision"] != "2" {
					t.Fatal("disabled worker cancellation became terminal")
				}
			}
		})
	}
}

type nativeClient struct {
	t                *testing.T
	url, token, csrf string
}

func (c *nativeClient) request(method, path string, body []byte) []byte {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, c.url+"/api/dashboard/v1"+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal("invalid fixture request")
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		c.t.Fatal("fixture request failed")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || response.StatusCode != 200 {
		c.t.Fatalf("fixture HTTP status %d", response.StatusCode)
	}
	return raw
}
func (c *nativeClient) envelope(kind, intent string, value map[string]any) map[string]any {
	c.t.Helper()
	envelope := map[string]any{"envelope": "v1", "kind": kind, "contributor": "dispatch", "intent": intent, "context": map[string]any{}}
	if kind == "command" {
		envelope["payload"], envelope["idempotencyKey"], envelope["csrf"] = value, intent, c.csrf
	} else {
		envelope["params"] = value
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		c.t.Fatal("could not encode fixture envelope")
	}
	var reply struct {
		Data map[string]any `json:"data"`
	}
	if json.Unmarshal(c.request(http.MethodPost, "", raw), &reply) != nil || reply.Data == nil {
		c.t.Fatal("fixture response data missing")
	}
	return reply.Data
}
