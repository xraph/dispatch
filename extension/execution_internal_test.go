package extension

import (
	"context"
	"os"
	"reflect"
	"testing"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/subprocess"
	"github.com/xraph/dispatch/id"
)

// TestResolveExecutionOptionsDisabledByDefault pins the backward-
// compatibility guarantee: a Config that never mentions execution
// registers no extra executor at all.
func TestResolveExecutionOptionsDisabledByDefault(t *testing.T) {
	e := New()

	opts, err := e.resolveExecutionOptions()
	if err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}
	if len(opts) != 0 {
		t.Fatalf("resolveExecutionOptions() = %d options, want 0", len(opts))
	}
}

// TestResolveExecutionOptionsEnablesSubprocess checks that enabling the
// block produces exactly one engine.Option (WithExecutor; no scratch dir
// configured here to add a second). The actual registration behaviour —
// that this is what lets exec.Isolate(exec.LevelProcess) satisfy —  is
// covered end to end in engine/execution_subprocess_test.go, which
// exercises engine.WithExecutor directly and needs no Forge app.
func TestResolveExecutionOptionsEnablesSubprocess(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true

	opts, err := e.resolveExecutionOptions()
	if err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}
	if len(opts) != 1 {
		t.Fatalf("resolveExecutionOptions() = %d options, want 1", len(opts))
	}
}

// TestResolveExecutionOptionsScratchDirAddsSecondOption checks that a
// configured scratch_dir produces the extra engine.WithScratchRoot
// option alongside WithExecutor.
func TestResolveExecutionOptionsScratchDirAddsSecondOption(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true
	e.config.Execution.Subprocess.ScratchDir = t.TempDir()

	opts, err := e.resolveExecutionOptions()
	if err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}
	if len(opts) != 2 {
		t.Fatalf("resolveExecutionOptions() = %d options, want 2 (WithExecutor + WithScratchRoot)", len(opts))
	}
}

// TestResolveExecutionOptionsRefusesSameUserAtStartup proves the
// resolveExecutionOptions fix: a config naming the worker's own uid, with
// no allow_same_user, must fail HERE, at startup — not pass cleanly and
// then fail launch on every single job, forever, discovered only in
// production. subprocess.Available alone cannot catch this (it is a
// platform check, not a per-config one); this exercises the separate
// same-uid check added alongside it.
func TestResolveExecutionOptionsRefusesSameUserAtStartup(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true
	e.config.Execution.Subprocess.User = os.Getuid()
	e.config.Execution.Subprocess.Group = os.Getgid()

	_, err := e.resolveExecutionOptions()
	if err == nil {
		t.Fatal("resolveExecutionOptions() = nil error, want one for a uid matching the worker's own")
	}
}

// TestResolveExecutionOptionsAllowsSameUserExplicitly proves the same-uid
// startup check does not fire when allow_same_user opted in — the
// deliberate escape hatch stays available, exactly as it does at Run()
// time in checkLaunch.
func TestResolveExecutionOptionsAllowsSameUserExplicitly(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true
	e.config.Execution.Subprocess.User = os.Getuid()
	e.config.Execution.Subprocess.Group = os.Getgid()
	e.config.Execution.Subprocess.AllowSameUser = true

	if _, err := e.resolveExecutionOptions(); err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}
}

// TestResolveExecutionOptionsWarnsOnMissingUser proves enabling the rung
// with no uid configured — the minimal `execution: {subprocess: {enabled:
// true}}` — logs a WARN. Without one, an operator has no signal that the
// sandboxed child runs as the worker's own uid, with full read access to
// its credentials and filesystem, which exec/subprocess's own doc.go
// calls the thing that "defeats most of this rung's purpose."
func TestResolveExecutionOptionsWarnsOnMissingUser(t *testing.T) {
	tl := log.NewTestLogger().(*log.TestLogger)

	e := New()
	e.SetLogger(tl)
	e.config.Execution.Subprocess.Enabled = true

	if _, err := e.resolveExecutionOptions(); err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}

	if tl.CountLogs("WARN") == 0 {
		t.Error("no WARN logged for execution.subprocess enabled with no user configured")
	}
}

// TestResolveExecutionOptionsNoWarnWhenUserConfigured is the negative
// case for the above: a properly configured uid must not also trigger
// the missing-user warning.
func TestResolveExecutionOptionsNoWarnWhenUserConfigured(t *testing.T) {
	tl := log.NewTestLogger().(*log.TestLogger)

	e := New()
	e.SetLogger(tl)
	e.config.Execution.Subprocess.Enabled = true
	e.config.Execution.Subprocess.User = 65532
	e.config.Execution.Subprocess.Group = 65532

	if _, err := e.resolveExecutionOptions(); err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}

	if tl.CountLogs("WARN") != 0 {
		t.Errorf("CountLogs(WARN) = %d, want 0 — a configured user must not warn", tl.CountLogs("WARN"))
	}
}

// TestBuildSubprocessOptionsRejectsLopsidedUserGroup pins the guard that
// keeps a config from silently running the child under the worker's own
// primary group when only a uid was configured.
func TestBuildSubprocessOptionsRejectsLopsidedUserGroup(t *testing.T) {
	e := New()

	if _, err := e.buildSubprocessOptions(SubprocessConfig{User: 65532}); err == nil {
		t.Fatal("buildSubprocessOptions() = nil error, want one for a uid with no configured gid")
	}
	if _, err := e.buildSubprocessOptions(SubprocessConfig{Group: 65532}); err == nil {
		t.Fatal("buildSubprocessOptions() = nil error, want one for a gid with no configured uid")
	}
}

// TestBuildSubprocessOptionsNeverDefaultsAllowSameUser proves config
// cannot accidentally defeat WithUser's own same-uid refusal: nothing in
// buildSubprocessOptions adds WithAllowSameUser unless the config
// explicitly asked for it, so an executor built from a config that names
// the worker's own uid must still refuse to launch.
//
// checkLaunch (exec/subprocess/limits_unix.go) runs before any pipe or
// process is created, so this reaches the refusal without actually
// spawning anything — the launch failure comes back through
// Result.Status, not a returned error, exactly like every other launch
// failure this rung reports.
func TestBuildSubprocessOptionsNeverDefaultsAllowSameUser(t *testing.T) {
	e := New()

	uid := os.Getuid()
	opts, err := e.buildSubprocessOptions(SubprocessConfig{User: uid, Group: os.Getgid()})
	if err != nil {
		t.Fatalf("buildSubprocessOptions() = %v, want nil", err)
	}

	ex := subprocess.New(opts...)

	res, runErr := ex.Run(context.Background(), &exec.Request{
		JobID:     id.NewJobID(),
		Name:      "test.subprocess.same-uid",
		Payload:   []byte("{}"),
		OutputDir: t.TempDir(),
		Policy:    exec.NewPolicy(),
	})
	if runErr != nil {
		t.Fatalf("Run() = %v, want nil (a launch refusal reports through Result, not error)", runErr)
	}
	if res.Status != exec.StatusLaunchFailed {
		t.Fatalf("Result.Status = %q, want %q — the same-uid refusal must survive config translation",
			res.Status, exec.StatusLaunchFailed)
	}
}

// TestConfigExecutionYAMLShape pins the YAML/mapstructure/json keys an
// operator writes for execution.subprocess — see
// TestResourceConfigYAMLShape in config_internal_test.go for why this
// matters: a struct-tag typo silently accepts a config key that does
// nothing.
func TestConfigExecutionYAMLShape(t *testing.T) {
	want := map[string]string{
		"Enabled":       "enabled",
		"Binary":        "binary",
		"User":          "user",
		"Group":         "group",
		"AllowSameUser": "allow_same_user",
		"ScratchDir":    "scratch_dir",
		"Rlimits":       "rlimits",
		"StrictRlimits": "strict_rlimits",
	}

	rt := reflect.TypeOf(SubprocessConfig{})
	for i := range rt.NumField() {
		f := rt.Field(i)

		key, known := want[f.Name]
		if !known {
			t.Errorf("field %s has no expected config key; update this test", f.Name)
			continue
		}

		for _, tag := range []string{"yaml", "mapstructure", "json"} {
			if got := f.Tag.Get(tag); got != key {
				t.Errorf("%s: %s tag = %q, want %q", f.Name, tag, got, key)
			}
		}
	}

	if got := reflect.TypeOf(Config{}).Field(fieldIndex(t, Config{}, "Execution")).Tag.Get("yaml"); got != "execution" {
		t.Errorf("Config.Execution yaml tag = %q, want %q", got, "execution")
	}
}
