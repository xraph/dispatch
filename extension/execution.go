package extension

import (
	"errors"
	"fmt"
	"os"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec/subprocess"
)

// mergeExecutionConfig folds programmatic execution settings into what
// YAML supplied, following the same precedence rule mergeResourceConfig
// documents: YAML wins where it said something, programmatic options
// fill the gaps, and every enable-shaped flag is an OR rather than an
// override. Without this, a binary that called WithConfig to turn on
// the subprocess rung — the one thing standing between a malicious
// upload and the worker's own credentials — had that silently discarded
// the moment ANY YAML "dispatch" key existed, because loadConfiguration
// replaces e.config wholesale with mergeConfigurations' result and this
// block was the one Config field that function never touched. The
// deployment would then run every job in-process, believing it was
// sandboxed, with no error or warning anywhere.
func mergeExecutionConfig(yamlCfg, programmatic ExecutionConfig) ExecutionConfig {
	yamlCfg.Subprocess = mergeSubprocessConfig(yamlCfg.Subprocess, programmatic.Subprocess)

	return yamlCfg
}

// mergeSubprocessConfig applies mergeExecutionConfig's precedence rule
// field by field.
//
// User and Group are merged as a pair, not independently: buildSubprocessOptions
// already refuses a config that sets one without the other, so filling
// them from different sources here could silently manufacture exactly
// that invalid combination. YAML naming either one counts as YAML having
// spoken on the pair; only when YAML sets neither does the programmatic
// pair fill the gap.
func mergeSubprocessConfig(yamlCfg, programmatic SubprocessConfig) SubprocessConfig {
	if programmatic.Enabled {
		yamlCfg.Enabled = true
	}

	if yamlCfg.Binary == "" && programmatic.Binary != "" {
		yamlCfg.Binary = programmatic.Binary
	}

	if yamlCfg.User == 0 && yamlCfg.Group == 0 && (programmatic.User != 0 || programmatic.Group != 0) {
		yamlCfg.User = programmatic.User
		yamlCfg.Group = programmatic.Group
	}

	if programmatic.AllowSameUser {
		yamlCfg.AllowSameUser = true
	}

	if yamlCfg.ScratchDir == "" && programmatic.ScratchDir != "" {
		yamlCfg.ScratchDir = programmatic.ScratchDir
	}

	if yamlCfg.Rlimits == (RlimitsConfig{}) && programmatic.Rlimits != (RlimitsConfig{}) {
		yamlCfg.Rlimits = programmatic.Rlimits
	}

	if programmatic.StrictRlimits {
		yamlCfg.StrictRlimits = true
	}

	return yamlCfg
}

// resolveExecutionOptions turns the execution config block into engine
// options that register additional isolation rungs beyond the always-
// present in-process default.
//
// This mirrors resolveArtifactBackend's shape: nothing configured returns
// no options and no error, so a deployment that never asks for out-of-
// process isolation runs exactly as it did before this existed. Unlike
// resolveArtifactBackend, a request this deployment or this platform
// cannot actually satisfy is refused HERE, at startup — not deferred to
// engine.RegisterChecked finding no executor for a policy, and not
// deferred further still to a job's first launch failure. Failing early
// and once is the whole point: the alternative is a job that requeues
// itself every poll interval forever, discovering the same
// misconfiguration on every attempt.
func (e *Extension) resolveExecutionOptions() ([]engine.Option, error) {
	cfg := e.config.Execution.Subprocess

	if !cfg.Enabled {
		if cfg.ScratchDir != "" && e.Logger() != nil {
			e.Logger().Warn("dispatch: execution.subprocess.scratch_dir is set but " +
				"execution.subprocess.enabled is false; the value has no effect")
		}

		return nil, nil
	}

	// Available only answers a platform question — can this OS run the
	// rung at all — not a per-configuration one. It does NOT catch a
	// configured uid equal to the worker's own; that is checkLaunch's own
	// refusal, and on its own it would not run until the first job
	// actually launches. Caught once, at startup, instead of on every
	// job's first launch attempt once the deployment is already running.
	if err := subprocess.Available(); err != nil {
		return nil, fmt.Errorf("dispatch: execution.subprocess is enabled but %w", err)
	}

	// The uid question, asked here through the identical helpers
	// checkLaunch itself uses (subprocess.SameUserRefused, and hasUser via
	// cfg.User == 0), so this check and checkLaunch's cannot drift apart
	// silently. Without it, a config naming the worker's own uid — or
	// naming none at all — passes startup clean and then fails to launch
	// every single job forever — requeued, reaped off its expired lease,
	// and re-leased, until it exhausts the launch-attempt cap and DLQs,
	// which happens per job, in production, long after this function
	// returned nil.
	//
	// Both shapes — no uid configured, and a configured uid equal to the
	// worker's own — are refused unless allow_same_user opts in: one
	// switch for "I know this is unisolated," not a warning for one shape
	// and a hard refusal for the other. See checkLaunch
	// (exec/subprocess/limits_unix.go) for why they are treated alike.
	if cfg.User == 0 {
		if !cfg.AllowSameUser {
			return nil, errors.New(
				"dispatch: execution.subprocess is enabled with no user configured; the sandboxed " +
					"child would run as the worker's own uid, with the worker's own read access to " +
					"its credentials and filesystem — set execution.subprocess.user, or " +
					"execution.subprocess.allow_same_user to accept running unisolated")
		}
	} else if subprocess.SameUserRefused(cfg.User, cfg.AllowSameUser) {
		return nil, fmt.Errorf(
			"dispatch: execution.subprocess.user %d matches the worker's own uid; "+
				"running the child as the worker defeats this rung's isolation — set "+
				"execution.subprocess.allow_same_user to allow it", cfg.User)
	}

	opts, err := e.buildSubprocessOptions(cfg)
	if err != nil {
		return nil, err
	}

	engOpts := []engine.Option{engine.WithExecutor(subprocess.New(opts...))}

	// ScratchDir only does anything once the artifact plane is also
	// configured — see WithScratchRoot and the ScratchDir field's own
	// doc comment. Setting it unconditionally here is still correct: the
	// warning above already told the operator when it will be a no-op,
	// and engine.Build itself only reads eng.scratchRoot when the
	// artifact plane resolved to a non-nil service.
	if cfg.ScratchDir != "" {
		engOpts = append(engOpts, engine.WithScratchRoot(cfg.ScratchDir))

		if e.artifacts == nil && e.Logger() != nil {
			e.Logger().Warn("dispatch: execution.subprocess.scratch_dir is configured but " +
				"the artifact plane is disabled; the out-of-process rung still gets a " +
				"scratch working directory, but nothing it writes is committed and " +
				"PriorOutputs stays empty — see artifacts.enabled")
		}
	}

	return engOpts, nil
}

// buildSubprocessOptions translates a SubprocessConfig into the
// subprocess.Option values its Executor is constructed from.
func (e *Extension) buildSubprocessOptions(cfg SubprocessConfig) ([]subprocess.Option, error) {
	binary := cfg.Binary
	if binary == "" {
		resolved, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("dispatch: resolve the worker's own binary for the subprocess rung: %w", err)
		}
		binary = resolved
	}

	opts := []subprocess.Option{
		subprocess.WithBinary(binary),
	}

	// Only when a logger is actually available: subprocess.New defaults
	// to a safe no-op logger on its own, and passing through a nil
	// interface here — e.Logger() before Register runs, or an app whose
	// own Logger() returns nil — would silently replace that default
	// with a nil Logger that panics the moment the child's stdout or
	// stderr is streamed through it.
	if logger := e.Logger(); logger != nil {
		opts = append(opts, subprocess.WithLogger(logger))
	}

	// WithUser takes uid and gid together; a config declaring one without
	// the other is ambiguous about what it wants and is refused rather
	// than guessed at — silently running the child under the worker's
	// own primary group while only the uid was pinned would quietly
	// weaken the boundary the operator thought they configured.
	if (cfg.User == 0) != (cfg.Group == 0) {
		return nil, errors.New(
			"dispatch: execution.subprocess.user and execution.subprocess.group must both be set, or neither")
	}
	if cfg.User != 0 {
		opts = append(opts, subprocess.WithUser(cfg.User, cfg.Group))
	}

	// Deliberately unconditional on cfg.AllowSameUser alone: nothing in
	// this function ever adds WithAllowSameUser on the operator's behalf,
	// so a config that leaves it false keeps subprocess.checkLaunch's own
	// refusal — the worker's own uid is not an acceptable default here —
	// fully in force.
	if cfg.AllowSameUser {
		opts = append(opts, subprocess.WithAllowSameUser())
	}

	if cfg.ScratchDir != "" {
		opts = append(opts, subprocess.WithScratchDir(cfg.ScratchDir))
	}

	if cfg.Rlimits != (RlimitsConfig{}) {
		opts = append(opts, subprocess.WithRlimits(subprocess.Rlimits{
			AddressSpace: cfg.Rlimits.AddressSpace,
			NoFile:       cfg.Rlimits.NoFile,
			NProc:        cfg.Rlimits.NProc,
			Core:         cfg.Rlimits.Core,
			FSize:        cfg.Rlimits.FSize,
		}))
	}

	if cfg.StrictRlimits {
		opts = append(opts, subprocess.WithStrictRlimits())
	}

	return opts, nil
}
