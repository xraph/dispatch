package cron

import "time"

// Settings is the configuration of this process's scheduler.
type Settings struct {
	TickInterval, LeaderTTL, RefreshInterval, LockTTL, StoreCallTimeout time.Duration
	StoreCallsBounded                                                   bool
}

// Settings reports the effective scheduler timings.
func (s *Scheduler) Settings() Settings {
	timeout := s.storeCallTimeout
	if timeout == 0 {
		timeout = defaultStoreCallTimeout
	}
	return Settings{
		TickInterval: s.tickInterval, LeaderTTL: s.leaderTTL,
		RefreshInterval: s.cronRefreshInterval, LockTTL: s.lockTTL,
		StoreCallTimeout: timeout, StoreCallsBounded: timeout > 0,
	}
}
