package workflow

import (
	"encoding/json"
	"fmt"
	"sync"
)

// RunnerFunc is a type-erased workflow runner that accepts raw JSON input.
// The typed Definition[T] is converted to a RunnerFunc at registration
// time by closing over JSON unmarshal + the typed handler.
type RunnerFunc func(wf *Workflow, input []byte) error

// versionedRunner holds a runner tagged with its version number.
type versionedRunner struct {
	version int
	runner  RunnerFunc
}

// Registry maps workflow names to versioned runner functions.
// Multiple versions of the same workflow can be registered; the latest
// version is used for new runs. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	versions map[string][]versionedRunner // name → list of versioned runners
}

// NewRegistry creates an empty workflow registry.
func NewRegistry() *Registry {
	return &Registry{
		versions: make(map[string][]versionedRunner),
	}
}

// RegisterDefinition registers a typed workflow definition. The generic
// handler is wrapped in a closure that JSON-unmarshals the input into T
// before calling the typed handler.
//
// If Version is 0 (default), it is treated as version 1.
// Multiple versions of the same workflow name can coexist.
//
// This is a package-level generic function because Go does not allow
// generic methods on non-generic receiver types.
func RegisterDefinition[T any](r *Registry, def *Definition[T]) {
	version := def.Version
	if version <= 0 {
		version = 1
	}

	runner := func(wf *Workflow, input []byte) error {
		var t T
		if len(input) > 0 {
			if err := json.Unmarshal(input, &t); err != nil {
				return fmt.Errorf("unmarshal input for workflow %q: %w", def.Name, err)
			}
		}
		return def.Handler(wf, t)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	vr := versionedRunner{version: version, runner: runner}
	existing := r.versions[def.Name]

	// Replace if same version already registered, else append.
	replaced := false
	for i, v := range existing {
		if v.version == version {
			existing[i] = vr
			replaced = true
			break
		}
	}
	if !replaced {
		existing = append(existing, vr)
	}
	r.versions[def.Name] = existing
}

// Get returns the latest-version runner for the given workflow name.
// Returns false if no runner is registered.
func (r *Registry) Get(name string) (RunnerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.versions[name]
	if len(versions) == 0 {
		return nil, false
	}
	// Find the highest version.
	best := versions[0]
	for _, v := range versions[1:] {
		if v.version > best.version {
			best = v
		}
	}
	return best.runner, true
}

// GetVersion returns the runner for a specific version of a workflow.
// If version <= 0, behaves like Get (returns latest).
func (r *Registry) GetVersion(name string, version int) (RunnerFunc, bool) {
	if version <= 0 {
		return r.Get(name)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, v := range r.versions[name] {
		if v.version == version {
			return v.runner, true
		}
	}
	return nil, false
}

// LatestVersion returns the highest registered version number for a workflow.
// Returns 0 if the workflow is not registered.
func (r *Registry) LatestVersion(name string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	best := 0
	for _, v := range r.versions[name] {
		if v.version > best {
			best = v.version
		}
	}
	return best
}

// Names returns all registered workflow names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.versions))
	for name := range r.versions {
		names = append(names, name)
	}
	return names
}
