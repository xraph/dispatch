package exec_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/exec"
)

func TestNewPolicy_Defaults(t *testing.T) {
	p := exec.NewPolicy()

	if p.Level != exec.LevelNone {
		t.Errorf("Level = %v, want %v", p.Level, exec.LevelNone)
	}
	if p.GracePeriod != 30*time.Second {
		t.Errorf("GracePeriod = %v, want %v", p.GracePeriod, 30*time.Second)
	}
	if p.AllowDowngrade {
		t.Error("AllowDowngrade = true, want false")
	}
	if p.Image != "" {
		t.Errorf("Image = %q, want empty", p.Image)
	}
}

func TestNewPolicy_Options(t *testing.T) {
	p := exec.NewPolicy(
		exec.Isolate(exec.LevelSandboxed),
		exec.GracePeriod(90*time.Second),
		exec.AllowDowngrade(),
		exec.Image("twinos/worker:v3"),
	)

	if p.Level != exec.LevelSandboxed {
		t.Errorf("Level = %v, want %v", p.Level, exec.LevelSandboxed)
	}
	if p.GracePeriod != 90*time.Second {
		t.Errorf("GracePeriod = %v, want %v", p.GracePeriod, 90*time.Second)
	}
	if !p.AllowDowngrade {
		t.Error("AllowDowngrade = false, want true")
	}
	if p.Image != "twinos/worker:v3" {
		t.Errorf("Image = %q, want %q", p.Image, "twinos/worker:v3")
	}
}

func TestNewPolicy_NonPositiveGracePeriodKeepsDefault(t *testing.T) {
	// A zero or negative grace period would make the kill ladder in later
	// rungs degenerate into an immediate SIGKILL, losing every chance of a
	// clean shutdown. Reject it at construction rather than at kill time.
	for _, d := range []time.Duration{0, -1 * time.Second} {
		p := exec.NewPolicy(exec.GracePeriod(d))
		if p.GracePeriod != 30*time.Second {
			t.Errorf("GracePeriod(%v) = %v, want default %v", d, p.GracePeriod, 30*time.Second)
		}
	}
}

func TestLevel_String(t *testing.T) {
	tests := []struct {
		level exec.Level
		want  string
	}{
		{exec.LevelNone, "none"},
		{exec.LevelProcess, "process"},
		{exec.LevelSandboxed, "sandboxed"},
		{exec.LevelVM, "vm"},
		{exec.Level(99), "Level(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.level.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
