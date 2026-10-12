package engine

import (
	"os"
	"slices"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/worker"
)

// Inspection describes the serving process. It makes no fleet-wide health claim.
type Inspection struct {
	Config                                        dispatch.Config
	Pool                                          worker.Settings
	Cron                                          cron.Settings
	ResourceDefaults                              resource.Set
	QueueResources                                map[string]resource.Set
	WorkerCapacity                                resource.Set
	WorkerCustomKeys                              []string
	ResourceManagerEnabled, EstimatorConfigured   bool
	ScratchRoot                                   string
	WakeNotifierSupported                         bool
	WorkerHeartbeatInterval, WorkerStaleThreshold time.Duration
}

// Inspect returns detached configuration and effective subsystem settings.
func (eng *Engine) Inspect() Inspection {
	cfg := eng.d.Config()
	cfg.Queues = slices.Clone(cfg.Queues)
	queues := make(map[string]resource.Set, len(eng.queueResources))
	for name, resources := range eng.queueResources {
		queues[name] = resources.Clone()
	}
	scratch := os.TempDir()
	if eng.artifacts != nil && eng.scratchRoot != "" {
		scratch = eng.scratchRoot
	}
	_, wakeSupported := eng.jobStore.(store.WakeNotifier)
	return Inspection{
		Config: cfg, Pool: eng.pool.Settings(), Cron: eng.scheduler.Settings(),
		ResourceDefaults: eng.resourceDefault.Clone(), QueueResources: queues,
		WorkerCapacity: eng.workerCapacity.Clone(), WorkerCustomKeys: slices.Clone(eng.workerCustomKeys),
		ResourceManagerEnabled: eng.resources != nil, EstimatorConfigured: eng.estimator != nil,
		ScratchRoot: scratch, WakeNotifierSupported: wakeSupported,
		WorkerHeartbeatInterval: eng.workerHeartbeatInterval(), WorkerStaleThreshold: eng.staleWorkerThreshold(),
	}
}
