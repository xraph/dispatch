package queue

import (
	"sync"

	"golang.org/x/time/rate"
)

// Config defines per-queue behaviour such as rate limiting and concurrency.
type Config struct {
	// Name is the queue identifier (must match the job.Queue field).
	Name string

	// MaxConcurrency limits how many jobs from this queue may run
	// simultaneously across the local worker pool. Zero means no
	// queue-specific limit (pool-wide concurrency still applies).
	MaxConcurrency int

	// RateLimit is the maximum sustained jobs per second that may be
	// dequeued from this queue. Zero disables rate limiting.
	RateLimit float64

	// RateBurst is the burst size for the token-bucket rate limiter.
	// Defaults to 1 if RateLimit is set but RateBurst is zero.
	RateBurst int
}

// queueState tracks runtime state for a single queue.
type queueState struct {
	config  Config
	limiter *rate.Limiter
	active  int
}

// Manager controls per-queue and per-tenant rate limiting and concurrency.
// It is safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	queues  map[string]*queueState
	tenants map[string]*tenantState
}

// NewManager creates a Manager with the given queue configurations.
// Queues not listed here have no limits.
func NewManager(configs ...Config) *Manager {
	m := &Manager{
		queues:  make(map[string]*queueState, len(configs)),
		tenants: make(map[string]*tenantState),
	}
	for _, cfg := range configs {
		m.queues[cfg.Name] = newQueueState(cfg)
	}
	return m
}

func newQueueState(cfg Config) *queueState {
	qs := &queueState{config: cfg}
	if cfg.RateLimit > 0 {
		burst := cfg.RateBurst
		if burst <= 0 {
			burst = 1
		}
		qs.limiter = rate.NewLimiter(rate.Limit(cfg.RateLimit), burst)
	}
	return qs
}

// Acquire checks rate limits and concurrency for the given queue and
// tenant. If the job is allowed to proceed it increments the active
// counter and returns true. The caller MUST call Release when the job
// completes.
func (m *Manager) Acquire(queue, tenantID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check queue-level constraints.
	qs := m.queues[queue]
	if qs != nil {
		if qs.limiter != nil && !qs.limiter.Allow() {
			return false
		}
		if qs.config.MaxConcurrency > 0 && qs.active >= qs.config.MaxConcurrency {
			return false
		}
	}

	// Check tenant-level constraints.
	if tenantID != "" {
		ts := m.tenants[tenantKey(queue, tenantID)]
		if ts != nil {
			if ts.limiter != nil && !ts.limiter.Allow() {
				return false
			}
			if ts.maxConcurrency > 0 && ts.active >= ts.maxConcurrency {
				return false
			}
			ts.active++
		}
	}

	// Increment queue active count.
	if qs != nil {
		qs.active++
	}

	return true
}

// Release decrements the active job count for the queue and tenant.
func (m *Manager) Release(queue, tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if qs := m.queues[queue]; qs != nil && qs.active > 0 {
		qs.active--
	}

	if tenantID != "" {
		if ts := m.tenants[tenantKey(queue, tenantID)]; ts != nil && ts.active > 0 {
			ts.active--
		}
	}
}

// SetQueueConfig dynamically updates (or creates) a queue configuration.
func (m *Manager) SetQueueConfig(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.queues[cfg.Name]
	qs := newQueueState(cfg)

	// Preserve current active count if reconfiguring.
	if existing != nil {
		qs.active = existing.active
	}
	m.queues[cfg.Name] = qs
}

// ActiveCount returns the current number of active jobs for a queue.
func (m *Manager) ActiveCount(queue string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if qs := m.queues[queue]; qs != nil {
		return qs.active
	}
	return 0
}
