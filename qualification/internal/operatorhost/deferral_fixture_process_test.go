package operatorhost

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
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
				if len(records) == 0 || records[0].Active != (action == "prepare") {
					t.Fatalf("fixture %s did not produce expected %s state", action, kind)
				}
			}
		}
		t.Logf("native secured fixture %s: %s", action, raw)
	}
}
