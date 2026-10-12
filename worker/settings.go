package worker

import (
	"slices"
	"time"
)

// Settings is the effective configuration of this process's worker pool.
type Settings struct {
	Concurrency                                                         int
	Queues                                                              []string
	PollInterval, MaxPollInterval, HeartbeatInterval, StaleJobThreshold time.Duration
	ReapInterval, DefaultLeaseTTL, StoreCallTimeout                     time.Duration
	StoreCallsBounded, ReapingEnabled, LeasesEnabled                    bool
}

// Settings returns a detached view, without worker state or credentials.
func (p *Pool) Settings() Settings {
	timeout := p.storeCallTimeout
	if timeout == 0 {
		timeout = defaultStoreCallTimeout
	}
	return Settings{
		Concurrency: p.concurrency, Queues: slices.Clone(p.queues),
		PollInterval: p.pollInterval, MaxPollInterval: p.maxPollInterval,
		HeartbeatInterval: p.heartbeatInterval, StaleJobThreshold: p.staleJobThreshold,
		ReapInterval: p.resolvedReapInterval(), DefaultLeaseTTL: p.leaseTTLFor(nil),
		StoreCallTimeout: timeout, StoreCallsBounded: timeout > 0,
		ReapingEnabled: p.staleJobThreshold > 0, LeasesEnabled: p.leaseStore != nil,
	}
}
