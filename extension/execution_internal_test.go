package extension

import (
	"context"
	"os"
	"reflect"
	"testing"

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
	e.config.Execution.Subprocess.AllowSameUser = true // no user configured; see TestResolveExecutionOptionsRefusesMissingUserAtStartup

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
	e.config.Execution.Subprocess.AllowSameUser = true // no user configured; see TestResolveExecutionOptionsRefusesMissingUserAtStartup

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

// TestResolveExecutionOptionsRefusesMissingUserAtStartup proves enabling
// the rung with no uid configured — the minimal `execution: {subprocess:
// {enabled: true}}` — refuses to start HERE, at startup, rather than
// warning and letting every job silently run unisolated. This used to be a
// WARN, on the theory that leaving `user` unset was a legitimate if weak
// choice; it is treated the same as a configured uid equal to the
// worker's own instead, because both leave the child with full read
// access to the worker's credentials and filesystem — see checkLaunch
// (exec/subprocess/limits_unix.go) and doc.go's "uid/gid boundary"
// section.
func TestResolveExecutionOptionsRefusesMissingUserAtStartup(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true

	_, err := e.resolveExecutionOptions()
	if err == nil {
		t.Fatal("resolveExecutionOptions() = nil error, want one for no uid configured")
	}
}

// TestResolveExecutionOptionsAllowsMissingUserExplicitly is the mirror of
// TestResolveExecutionOptionsAllowsSameUserExplicitly for the other shape
// checkLaunch refuses: allow_same_user lifts the no-uid refusal too, since
// it is the single escape hatch for both.
func TestResolveExecutionOptionsAllowsMissingUserExplicitly(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true
	e.config.Execution.Subprocess.AllowSameUser = true

	if _, err := e.resolveExecutionOptions(); err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
	}
}

// TestResolveExecutionOptionsSucceedsWhenUserConfigured is the negative
// case for the above: a properly configured uid must not be refused.
func TestResolveExecutionOptionsSucceedsWhenUserConfigured(t *testing.T) {
	e := New()
	e.config.Execution.Subprocess.Enabled = true
	e.config.Execution.Subprocess.User = 65532
	e.config.Execution.Subprocess.Group = 65532

	if _, err := e.resolveExecutionOptions(); err != nil {
		t.Fatalf("resolveExecutionOptions() = %v, want nil", err)
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

// TestMergeConfigurationsKeepsProgrammaticExecution is the regression
// test for the bug mergeConfigurations had: it merged Artifacts and
// Resources but never touched Execution at all, so a binary that called
// WithConfig(Config{Execution: ...}) to turn on the subprocess rung had
// that silently dropped the instant loadConfiguration found ANY YAML
// "dispatch" key — even one that says nothing whatsoever about
// execution. The deployment would then run every job in-process,
// believing it was sandboxed, with no error or warning anywhere.
//
// Reproduces the exact shape from the report: YAML sets an unrelated
// field (BasePath) and says nothing about execution at all.
func TestMergeConfigurationsKeepsProgrammaticExecution(t *testing.T) {
	e := New()

	yamlConfig := Config{BasePath: "/custom"}
	programmaticConfig := Config{
		Execution: ExecutionConfig{
			Subprocess: SubprocessConfig{
				Enabled: true,
				User:    65532,
				Group:   65532,
			},
		},
	}

	got := e.mergeConfigurations(yamlConfig, programmaticConfig)

	if !got.Execution.Subprocess.Enabled {
		t.Error("WithConfig's execution.subprocess.enabled was dropped by a config file that said nothing about execution")
	}
	if got.Execution.Subprocess.User != 65532 || got.Execution.Subprocess.Group != 65532 {
		t.Errorf("Execution.Subprocess.User/Group = %d/%d, want 65532/65532",
			got.Execution.Subprocess.User, got.Execution.Subprocess.Group)
	}
}

// TestMergeExecutionConfig covers mergeExecutionConfig's precedence
// rules directly, mirroring TestMergeResourceConfig in
// config_internal_test.go.
func TestMergeExecutionConfig(t *testing.T) {
	t.Run("programmatic enable survives silent yaml", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{},
			ExecutionConfig{Subprocess: SubprocessConfig{Enabled: true}},
		)
		if !got.Subprocess.Enabled {
			t.Error("enabled was dropped by a config file that said nothing")
		}
	})

	t.Run("yaml enabled survives silent programmatic config", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{Subprocess: SubprocessConfig{Enabled: true}},
			ExecutionConfig{},
		)
		if !got.Subprocess.Enabled {
			t.Error("yaml's enabled was lost")
		}
	})

	t.Run("yaml wins on scalars", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{Subprocess: SubprocessConfig{Binary: "/yaml/bin", ScratchDir: "/yaml/scratch"}},
			ExecutionConfig{Subprocess: SubprocessConfig{Binary: "/programmatic/bin", ScratchDir: "/programmatic/scratch"}},
		)
		if got.Subprocess.Binary != "/yaml/bin" {
			t.Errorf("Binary = %q, want yaml value", got.Subprocess.Binary)
		}
		if got.Subprocess.ScratchDir != "/yaml/scratch" {
			t.Errorf("ScratchDir = %q, want yaml value", got.Subprocess.ScratchDir)
		}
	})

	t.Run("programmatic fills gaps left by yaml", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{},
			ExecutionConfig{Subprocess: SubprocessConfig{Binary: "/programmatic/bin", ScratchDir: "/programmatic/scratch"}},
		)
		if got.Subprocess.Binary != "/programmatic/bin" {
			t.Errorf("Binary = %q, want programmatic value", got.Subprocess.Binary)
		}
		if got.Subprocess.ScratchDir != "/programmatic/scratch" {
			t.Errorf("ScratchDir = %q, want programmatic value", got.Subprocess.ScratchDir)
		}
	})

	t.Run("user and group merge as a pair, never independently", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{Subprocess: SubprocessConfig{User: 100, Group: 100}},
			ExecutionConfig{Subprocess: SubprocessConfig{User: 200, Group: 200}},
		)
		if got.Subprocess.User != 100 || got.Subprocess.Group != 100 {
			t.Errorf("User/Group = %d/%d, want yaml's 100/100", got.Subprocess.User, got.Subprocess.Group)
		}

		got = mergeExecutionConfig(
			ExecutionConfig{},
			ExecutionConfig{Subprocess: SubprocessConfig{User: 200, Group: 200}},
		)
		if got.Subprocess.User != 200 || got.Subprocess.Group != 200 {
			t.Errorf("User/Group = %d/%d, want programmatic's 200/200 filling an empty yaml pair",
				got.Subprocess.User, got.Subprocess.Group)
		}
	})

	t.Run("allow_same_user and strict_rlimits are an OR", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{},
			ExecutionConfig{Subprocess: SubprocessConfig{AllowSameUser: true, StrictRlimits: true}},
		)
		if !got.Subprocess.AllowSameUser {
			t.Error("programmatic AllowSameUser was dropped")
		}
		if !got.Subprocess.StrictRlimits {
			t.Error("programmatic StrictRlimits was dropped")
		}
	})

	t.Run("rlimits: yaml wins wholesale when it set any field", func(t *testing.T) {
		got := mergeExecutionConfig(
			ExecutionConfig{Subprocess: SubprocessConfig{Rlimits: RlimitsConfig{NoFile: 10}}},
			ExecutionConfig{Subprocess: SubprocessConfig{Rlimits: RlimitsConfig{NoFile: 20, NProc: 5}}},
		)
		if got.Subprocess.Rlimits != (RlimitsConfig{NoFile: 10}) {
			t.Errorf("Rlimits = %+v, want yaml's {NoFile: 10}", got.Subprocess.Rlimits)
		}
	})
}
