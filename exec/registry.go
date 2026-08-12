package exec

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrNoExecutor marks a policy no configured executor can satisfy.
var ErrNoExecutor = errors.New("no executor satisfies the policy")

// Registry holds the executors a deployment has configured and matches
// job policies against them.
//
// It is safe for concurrent use, though in practice it is built once at
// startup and only read afterwards.
type Registry struct {
	mu     sync.RWMutex
	def    Executor
	byName map[string]Executor
}

// NewRegistry creates a registry with a default executor, which is the one
// used by any job that declares no isolation requirement.
func NewRegistry(def Executor) *Registry {
	r := &Registry{
		def:    def,
		byName: make(map[string]Executor),
	}
	if def != nil {
		r.byName[def.Name()] = def
	}

	return r
}

// Add registers an executor, replacing any existing one with the same name.
func (r *Registry) Add(e Executor) {
	if e == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[e.Name()] = e
}

// Default returns the executor used when a job declares no requirement.
func (r *Registry) Default() Executor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.def
}

// Executors returns every registered executor, ordered by name so callers
// and tests see a stable list.
func (r *Registry) Executors() []Executor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)

	out := make([]Executor, 0, len(names))
	for _, n := range names {
		out = append(out, r.byName[n])
	}

	return out
}

// Select returns the executor that should run a job with this policy.
//
// It picks the weakest executor that still satisfies the declared level,
// so a job needing a separate process is not handed a Kubernetes pod
// merely because one is configured. When nothing satisfies the policy the
// call fails rather than quietly running the handler with less isolation
// than it asked for — unless the policy opted into a downgrade.
func (r *Registry) Select(p Policy) (Executor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if p.Level == LevelNone {
		if r.def == nil {
			return nil, fmt.Errorf("%w: no default executor configured", ErrNoExecutor)
		}

		return r.def, nil
	}

	var best Executor
	for _, e := range r.byName {
		if e.Level() < p.Level {
			continue
		}
		if best == nil || e.Level() < best.Level() ||
			(e.Level() == best.Level() && e.Name() < best.Name()) {
			best = e
		}
	}
	if best != nil {
		return best, nil
	}

	if p.AllowDowngrade && r.def != nil {
		return r.def, nil
	}

	return nil, fmt.Errorf(
		"%w: policy requires level %s, configured executors are %s",
		ErrNoExecutor, p.Level, r.describeLocked(),
	)
}

// describeLocked renders the configured executors for an error message.
// The caller must hold at least a read lock.
func (r *Registry) describeLocked() string {
	if len(r.byName) == 0 {
		return "(none)"
	}

	names := make([]string, 0, len(r.byName))
	for n, e := range r.byName {
		names = append(names, fmt.Sprintf("%s(%s)", n, e.Level()))
	}
	sort.Strings(names)

	out := names[0]
	for _, n := range names[1:] {
		out += ", " + n
	}

	return out
}
