package extension

import (
	"errors"
	"fmt"
	"os"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/exec/subprocess"
)

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

	// Available reports the identical condition Run's own checkLaunch
	// refuses on, but here it is caught once, at startup, instead of on
	// every job's first launch attempt once the deployment is already
	// running.
	if err := subprocess.Available(); err != nil {
		return nil, fmt.Errorf("dispatch: execution.subprocess is enabled but %w", err)
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
