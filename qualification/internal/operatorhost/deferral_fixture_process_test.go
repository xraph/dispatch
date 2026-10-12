package operatorhost

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/dispatch/operator"
)

func TestNativeDeferralFixtureCommands(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	activation := filepath.Join(t.TempDir(), "activate")
	host := startNativeLifecycle(t, "fixture-native", "--run-workers=true", "--activation-file", activation)
	for _, action := range []string{"prepare", "inspect", "resume", "inspect"} {
		if action == "resume" {
			waitMatureFixtureDeferrals(t, host)
		}
		args := []string{"../../lifecycle-fixture.py", action, "--state-file", host.statePath}
		if action == "prepare" {
			args = append(args, "--activation-file", activation)
		}
		started := time.Now()
		raw, err := exec.CommandContext(t.Context(), "python3", args...).CombinedOutput()
		elapsed := time.Since(started)
		if err != nil {
			t.Fatalf("fixture %s failed: %v: %s", action, err, raw)
		}
		if action == "resume" && elapsed <= 20*time.Second {
			t.Fatalf("mature fixture resume did not observe a late retry: %s", elapsed)
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
		t.Logf("native secured fixture %s elapsed=%s: %s", action, elapsed, raw)
	}
}

func waitMatureFixtureDeferrals(t *testing.T, host *nativeLifecycleHost) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		observed := make(map[string][]operator.TaskDeferral)
		mature := true
		for _, kind := range []string{"deferred-child", "deferred-continue"} {
			status, raw := call(t, host.client.server.URL, host.client.host.Credentials["reader"].Token, "durable.tasks", map[string]any{"namespace": "production", "workflow_id": kind, "run_id": "run-1", "limit": 100}, false)
			if status != http.StatusOK {
				t.Fatalf("maturity task read HTTP %d: %s", status, raw)
			}
			page := data[operator.Page[operator.Task]](t, raw)
			for _, task := range page.Items {
				if task.Deferral == nil {
					continue
				}
				d := *task.Deferral
				count, err := strconv.ParseUint(d.Count, 10, 64)
				if err != nil {
					t.Fatalf("invalid deferral count: %v", err)
				}
				if !d.Active || d.TargetBuildID != "operator-v2" || count < 6 || d.RetryAt.Sub(d.RecordedAt) != 30*time.Second {
					mature = false
				}
				observed[kind] = append(observed[kind], d)
			}
			if len(observed[kind]) == 0 {
				mature = false
			}
		}
		now := time.Now()
		for _, records := range observed {
			for _, d := range records {
				if d.RetryAt.Sub(now) < 25*time.Second {
					mature = false
				}
			}
		}
		if mature {
			raw, err := json.Marshal(observed)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native mature deferrals observed_at=%s: %s", now.UTC().Format(time.RFC3339Nano), raw)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("native deferrals did not reach a fresh 30-second retry within 60 seconds")
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

func TestDeferralFixtureResumeObservationDeadline(t *testing.T) {
	const acceleratedClock = `import runpy, sys, time
now = 0.0
def monotonic():
    return now
def sleep(seconds):
    global now
    now += seconds * 100
time.monotonic = monotonic
time.sleep = sleep
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")
`
	for _, tc := range []struct {
		name      string
		wantError string
		wantReads int
	}{
		{name: "late_settlement", wantReads: 4},
		{name: "permanently_active", wantError: "expected persisted deferrals did not appear", wantReads: 5},
		{name: "empty_rows", wantError: "expected persisted deferrals did not appear", wantReads: 5},
		{name: "http_denied", wantError: "fixture request refused: HTTP 403"},
		{name: "command_rejected", wantError: "fixture command was not accepted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
					http.Error(w, "invalid", http.StatusBadRequest)
					return
				}
				if request.Intent == "durable.buildResume" {
					if tc.name == "http_denied" {
						http.Error(w, "denied", http.StatusForbidden)
						return
					}
					if tc.name == "command_rejected" {
						_ = json.NewEncoder(w).Encode(map[string]any{"ok": false})
						return
					}
				}
				data := map[string]any{"version": "2", "epoch": "2"}
				if request.Intent == "durable.tasks" {
					mu.Lock()
					calls[request.Payload.WorkflowID]++
					read := calls[request.Payload.WorkflowID]
					mu.Unlock()
					active := tc.name == "permanently_active" || (tc.name == "late_settlement" && read < 4)
					items := []any{map[string]any{"deferral": map[string]any{"active": false}}, map[string]any{"deferral": map[string]any{"active": active}}}
					if tc.name == "empty_rows" && request.Payload.WorkflowID == "deferred-child" {
						items = []any{}
					}
					data = map[string]any{"items": items}
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
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-c", acceleratedClock, "../../lifecycle-fixture.py", "resume", "--state-file", state)
			var stdout, stderr strings.Builder
			command.Stdout, command.Stderr = &stdout, &stderr
			err = command.Run()
			if ctx.Err() != nil {
				t.Fatalf("fixture subprocess exceeded its bound: %v", ctx.Err())
			}
			if tc.wantError != "" {
				if err == nil || !strings.Contains(stderr.String(), tc.wantError) || stdout.Len() != 0 {
					t.Fatalf("expected failure %q: err=%v stdout=%s stderr=%s", tc.wantError, err, &stdout, &stderr)
				}
				t.Logf("expected fixture failure: %s", &stderr)
			} else {
				if err != nil {
					t.Fatalf("fixture client failed: %v %s", err, &stderr)
				}
				var records map[string][]struct {
					Active bool `json:"active"`
				}
				if err = json.Unmarshal([]byte(stdout.String()), &records); err != nil || len(records) != 2 {
					t.Fatalf("fixture output: %v: %s", err, &stdout)
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
				}
			}
			mu.Lock()
			defer mu.Unlock()
			for _, kind := range []string{"deferred-child", "deferred-continue"} {
				if calls[kind] != tc.wantReads {
					t.Fatalf("%s task reads=%d, want %d", kind, calls[kind], tc.wantReads)
				}
			}
		})
	}
}
