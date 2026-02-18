package queue

import (
	"fmt"

	"golang.org/x/time/rate"
)

// TenantConfig defines rate limits and concurrency for a specific tenant
// on a specific queue, identified by the job's ScopeOrgID.
type TenantConfig struct {
	// QueueName is the queue this config applies to.
	QueueName string

	// TenantID is the tenant identifier (typically job.ScopeOrgID).
	TenantID string

	// RateLimit is the sustained jobs per second for this tenant.
	RateLimit float64

	// RateBurst is the burst size for the tenant's rate limiter.
	RateBurst int

	// MaxConcurrency limits simultaneous jobs for this tenant on this
	// queue. Zero means no tenant-specific concurrency limit.
	MaxConcurrency int
}

// tenantState tracks runtime state for a single queue+tenant pair.
type tenantState struct {
	limiter        *rate.Limiter
	maxConcurrency int
	active         int
}

// tenantKey builds the map key for a queue+tenant pair.
func tenantKey(queue, tenantID string) string {
	return fmt.Sprintf("%s:%s", queue, tenantID)
}

// SetTenantConfig configures rate limits and concurrency for a specific
// tenant on a specific queue. Calling this multiple times for the same
// queue+tenant replaces the previous configuration.
func (m *Manager) SetTenantConfig(cfg TenantConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := tenantKey(cfg.QueueName, cfg.TenantID)
	existing := m.tenants[key]

	ts := &tenantState{
		maxConcurrency: cfg.MaxConcurrency,
	}
	if cfg.RateLimit > 0 {
		burst := cfg.RateBurst
		if burst <= 0 {
			burst = 1
		}
		ts.limiter = rate.NewLimiter(rate.Limit(cfg.RateLimit), burst)
	}

	// Preserve current active count if reconfiguring.
	if existing != nil {
		ts.active = existing.active
	}
	m.tenants[key] = ts
}

// TenantActiveCount returns the current number of active jobs for a
// queue+tenant pair.
func (m *Manager) TenantActiveCount(queue, tenantID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ts := m.tenants[tenantKey(queue, tenantID)]; ts != nil {
		return ts.active
	}
	return 0
}
