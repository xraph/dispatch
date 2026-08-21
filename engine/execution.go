package engine

import (
	"fmt"

	"github.com/xraph/dispatch/exec"
	"github.com/xraph/dispatch/exec/inproc"
	"github.com/xraph/dispatch/job"
)

// WithExecutor registers an additional executor, making a stronger
// isolation level available to job definitions that ask for it.
//
// The in-process executor is always present as the default, so a
// deployment that adds nothing behaves exactly as it always has.
func WithExecutor(e exec.Executor) Option {
	return func(eng *Engine) {
		eng.extraExecutors = append(eng.extraExecutors, e)
	}
}

// WithScratchRoot sets the root directory an out-of-process attempt's
// scratch OutputDir is created under (worker.Runner.WithArtifacts).
//
// It only has an effect once the artifact plane is also configured
// (WithArtifacts): Build only ever passes this value to the Runner
// alongside eng.artifacts (see Build's own runner.WithArtifacts call),
// so with no artifact plane configured this sets a value Build never
// reads and the Runner's scratch directories fall back to os.TempDir()
// regardless. That is not because scratch-dir creation or its startup
// sweep are themselves gated on the artifact plane — they are not, and
// both run for any out-of-process attempt either way (see
// worker.Runner.prepareOutputDir and Reclaim) — it is purely this
// option's own value never reaching the Runner. The extension package's
// resolveExecutionOptions logs a startup warning for exactly this
// configuration (a configured scratch_dir with the artifact plane off),
// so that stays a deliberate, visible choice rather than a silent one.
//
// Leaving it unset defaults to os.TempDir(), exactly as worker.Runner
// does on its own.
func WithScratchRoot(dir string) Option {
	return func(eng *Engine) { eng.scratchRoot = dir }
}

// Executors returns the configured executor registry.
func (eng *Engine) Executors() *exec.Registry { return eng.executors }

// buildExecutors assembles the executor registry. It is called once during
// engine construction, before any definition is registered, because
// registration validates policies against it.
func (eng *Engine) buildExecutors() {
	r := exec.NewRegistry(inproc.New(eng.registry))
	for _, e := range eng.extraExecutors {
		r.Add(e)
	}
	eng.executors = r
}

// checkExecutionPolicy reports whether the deployment can satisfy a
// definition's declared isolation.
//
// This runs at registration rather than at execution deliberately. A
// definition that can never be satisfied should fail on a developer's
// machine, not on the first malicious upload in production.
func (eng *Engine) checkExecutionPolicy(name string, p exec.Policy) error {
	if eng.executors == nil {
		return nil
	}
	if _, err := eng.executors.Select(p); err != nil {
		return fmt.Errorf("dispatch/engine: job %q: %w", name, err)
	}

	return nil
}

// RegisterAll registers a set of definitions.
//
// It takes job.Registrable rather than a typed definition so a single
// handler list can be shared between the worker and an out-of-process
// entrypoint, which cannot be handed an engine.
func RegisterAll(eng *Engine, defs ...job.Registrable) error {
	// Validate every definition before registering any of them, so a
	// rejected set leaves the registry as it was rather than half
	// populated.
	for _, d := range defs {
		if err := eng.checkExecutionPolicy(d.JobName(), d.Policy()); err != nil {
			return err
		}
	}
	for _, d := range defs {
		d.Register(eng.registry)
	}

	return nil
}
