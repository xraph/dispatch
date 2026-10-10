package operatorhost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

func TestNativeDeferralFixtureCommands(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	activation := filepath.Join(t.TempDir(), "activate")
	host := startNativeLifecycle(t, "fixture-native", "--run-workers=true", "--activation-file", activation)
	for _, action := range []string{"prepare", "inspect", "resume", "inspect"} {
		args := []string{"../../lifecycle-fixture.py", action, "--state-file", host.statePath}
		if action == "prepare" {
			args = append(args, "--activation-file", activation)
		}
		raw, err := exec.CommandContext(t.Context(), "python3", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("fixture %s failed: %v: %s", action, err, raw)
		}
		var result map[string][]struct {
			Active bool `json:"active"`
		}
		if err = json.Unmarshal(raw, &result); err != nil || len(result) != 2 {
			t.Fatalf("fixture output: %v: %s", err, raw)
		}
		if action != "inspect" {
			for kind, records := range result {
				if len(records) == 0 {
					t.Fatalf("fixture %s omitted %s records", action, kind)
				}
				for _, record := range records {
					if record.Active != (action == "prepare") {
						t.Fatalf("fixture %s did not settle every %s record", action, kind)
					}
				}
			}
		}
		t.Logf("native secured fixture %s: %s", action, raw)
	}
}

// This deterministic client regression serves mixed rows before the settled page.
// The native test above separately qualifies the real authorization and store.
func TestDeferralFixtureWaitsForAllInactiveRecords(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/dashboard/v1/csrf" {
			_ = json.NewEncoder(w).Encode(map[string]string{"token": "fixture-csrf"})
			return
		}
		var request struct {
			Intent  string `json:"intent"`
			Payload struct {
				WorkflowID string `json:"workflow_id"`
			} `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		data := map[string]any{"version": "2", "epoch": "2"}
		if request.Intent == "durable.tasks" {
			mu.Lock()
			calls[request.Payload.WorkflowID]++
			first := calls[request.Payload.WorkflowID] == 1
			mu.Unlock()
			data = map[string]any{"items": []any{map[string]any{"deferral": map[string]any{"active": false}}, map[string]any{"deferral": map[string]any{"active": first}}}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "data": data})
	}))
	defer server.Close()
	state := filepath.Join(t.TempDir(), "state.json")
	raw, err := json.Marshal(map[string]any{"url": server.URL, "credentials": map[string]any{"commander": map[string]string{"token": "client-regression"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(state, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(t.Context(), "python3", "../../lifecycle-fixture.py", "resume", "--state-file", state).CombinedOutput()
	if err != nil {
		t.Fatalf("fixture client failed: %v %s", err, output)
	}
	var records map[string][]struct {
		Active bool `json:"active"`
	}
	if err = json.Unmarshal(output, &records); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"deferred-child", "deferred-continue"} {
		if len(records[kind]) != 2 {
			t.Fatal("fixture omitted retained rows")
		}
		for _, record := range records[kind] {
			if record.Active {
				t.Fatal("resume returned success while a deferral remained active")
			}
		}
		mu.Lock()
		count := calls[kind]
		mu.Unlock()
		if count < 2 {
			t.Fatal("resume did not await settled task page")
		}
	}
}
