package contract

import (
	"context"
	"slices"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/exec/subprocess"
	"github.com/xraph/dispatch/resource"
)

type PoolConfig struct {
	Concurrency             int      `json:"concurrency"`
	Queues                  []string `json:"queues"`
	PollInterval            Duration `json:"pollInterval"`
	MaxPollInterval         Duration `json:"maxPollInterval"`
	JobHeartbeatInterval    Duration `json:"jobHeartbeatInterval"`
	WorkerHeartbeatInterval Duration `json:"workerHeartbeatInterval"`
	WorkerStaleThreshold    Duration `json:"workerStaleThreshold"`
	StaleJobThreshold       Duration `json:"staleJobThreshold"`
	ReapInterval            Duration `json:"reapInterval"`
	DefaultLeaseTTL         Duration `json:"defaultLeaseTtl"`
	StoreCallTimeout        Duration `json:"storeCallTimeout"`
	ShutdownTimeout         Duration `json:"shutdownTimeout"`
	StoreCallsBounded       bool     `json:"storeCallsBounded"`
	ReapingEnabled          bool     `json:"reapingEnabled"`
	LeasesEnabled           bool     `json:"leasesEnabled"`
}
type SchedulerConfig struct {
	TickInterval      Duration `json:"tickInterval"`
	LeaderTTL         Duration `json:"leaderTtl"`
	RefreshInterval   Duration `json:"refreshInterval"`
	LockTTL           Duration `json:"lockTtl"`
	StoreCallTimeout  Duration `json:"storeCallTimeout"`
	StoreCallsBounded bool     `json:"storeCallsBounded"`
}
type QueueConfigRow struct {
	Name     string        `json:"name"`
	Settings QueueSettings `json:"settings"`
}
type RequestedRlimits struct {
	AddressSpace int64 `json:"addressSpace"`
	NoFile       int64 `json:"noFile"`
	NProc        int64 `json:"nProc"`
	Core         int64 `json:"core"`
	FSize        int64 `json:"fSize"`
}
type SubprocessConfig struct {
	UserConfigured  bool             `json:"userConfigured"`
	UID             *int             `json:"uid"`
	GID             *int             `json:"gid"`
	AllowSameUser   bool             `json:"allowSameUser"`
	HasRlimits      bool             `json:"hasRlimits"`
	StrictRlimits   bool             `json:"strictRlimits"`
	RequestedLimits RequestedRlimits `json:"requestedLimits"`
	ScratchDir      *string          `json:"scratchDir"`
}
type ExecutorRow struct {
	Name       string            `json:"name"`
	Level      string            `json:"level"`
	Default    bool              `json:"default"`
	Subprocess *SubprocessConfig `json:"subprocess"`
}
type ResourceConfig struct {
	Enabled                  bool                    `json:"enabled"`
	Defaults                 resource.Set            `json:"defaults"`
	Queues                   map[string]resource.Set `json:"queues"`
	AdvertisedWorkerCapacity resource.Set            `json:"advertisedWorkerCapacity"`
	CustomKeys               []string                `json:"customKeys"`
	EstimatorConfigured      bool                    `json:"estimatorConfigured"`
}
type ArtifactCacheConfig struct {
	Directory   string `json:"directory"`
	BudgetBytes int64  `json:"budgetBytes"`
	UsedBytes   int64  `json:"usedBytes"`
}
type ArtifactConfig struct {
	Enabled       bool                 `json:"enabled"`
	Backend       *string              `json:"backend"`
	DefaultBucket *string              `json:"defaultBucket"`
	Cache         *ArtifactCacheConfig `json:"cache"`
}
type EngineConfig struct {
	WorkerID              string           `json:"workerId"`
	Pool                  PoolConfig       `json:"pool"`
	Scheduler             SchedulerConfig  `json:"scheduler"`
	Queues                []QueueConfigRow `json:"queues"`
	Executors             []ExecutorRow    `json:"executors"`
	Resources             ResourceConfig   `json:"resources"`
	Artifacts             ArtifactConfig   `json:"artifacts"`
	ScratchRoot           string           `json:"scratchRoot"`
	WakeNotifierSupported bool             `json:"wakeNotifierSupported"`
	AsOf                  string           `json:"asOf"`
}

func engineConfigHandler(deps Deps) func(context.Context, EmptyInput, fc.Principal) (EngineConfig, error) {
	return handle(deps, "engine.config", false, func(_ context.Context, _ EmptyInput, _ fc.Principal) (EngineConfig, error) {
		inspected := deps.Engine.Inspect()
		pool, cron := inspected.Pool, inspected.Cron
		out := EngineConfig{WorkerID: deps.Engine.WorkerID().String(), Queues: []QueueConfigRow{}, Executors: []ExecutorRow{},
			ScratchRoot: inspected.ScratchRoot, WakeNotifierSupported: inspected.WakeNotifierSupported, AsOf: time.Now().UTC().Format(time.RFC3339Nano)}
		out.Pool = PoolConfig{Concurrency: pool.Concurrency, Queues: append([]string{}, pool.Queues...), PollInterval: duration(pool.PollInterval),
			MaxPollInterval: duration(pool.MaxPollInterval), JobHeartbeatInterval: duration(pool.HeartbeatInterval),
			WorkerHeartbeatInterval: duration(inspected.WorkerHeartbeatInterval), WorkerStaleThreshold: duration(inspected.WorkerStaleThreshold),
			StaleJobThreshold: duration(pool.StaleJobThreshold), ReapInterval: duration(pool.ReapInterval), DefaultLeaseTTL: duration(pool.DefaultLeaseTTL),
			StoreCallTimeout: duration(pool.StoreCallTimeout), ShutdownTimeout: duration(inspected.Config.ShutdownTimeout),
			StoreCallsBounded: pool.StoreCallsBounded, ReapingEnabled: pool.ReapingEnabled, LeasesEnabled: pool.LeasesEnabled}
		out.Scheduler = SchedulerConfig{TickInterval: duration(cron.TickInterval), LeaderTTL: duration(cron.LeaderTTL), RefreshInterval: duration(cron.RefreshInterval),
			LockTTL: duration(cron.LockTTL), StoreCallTimeout: duration(cron.StoreCallTimeout), StoreCallsBounded: cron.StoreCallsBounded}
		names := []string{}
		if manager := deps.Engine.QueueManager(); manager != nil {
			names = manager.QueueNames()
		}
		slices.Sort(names)
		for _, name := range names {
			row := projectQueueLocal(deps, name)
			if row.LocalSettings != nil {
				out.Queues = append(out.Queues, QueueConfigRow{Name: name, Settings: *row.LocalSettings})
			}
		}
		registry := deps.Engine.Executors()
		defaultExecutor := registry.Default()
		for _, executor := range registry.Executors() {
			row := ExecutorRow{Name: executor.Name(), Level: executor.Level().String(), Default: defaultExecutor != nil && executor.Name() == defaultExecutor.Name()}
			if process, ok := executor.(interface{ Settings() subprocess.Settings }); ok {
				settings := process.Settings()
				limits := settings.Rlimits
				row.Subprocess = &SubprocessConfig{UserConfigured: settings.UserConfigured, AllowSameUser: settings.AllowSameUser, HasRlimits: settings.HasRlimits,
					StrictRlimits: settings.StrictRlimits, RequestedLimits: RequestedRlimits{AddressSpace: limits.AddressSpace, NoFile: limits.NoFile, NProc: limits.NProc, Core: limits.Core, FSize: limits.FSize},
					ScratchDir: nullable(settings.ScratchDir)}
				if settings.UserConfigured {
					uid, gid := settings.UID, settings.GID
					row.Subprocess.UID = &uid
					row.Subprocess.GID = &gid
				}
			}
			out.Executors = append(out.Executors, row)
		}
		queues := make(map[string]resource.Set, len(inspected.QueueResources))
		for name, values := range inspected.QueueResources {
			queues[name] = resourceValues(values)
		}
		out.Resources = ResourceConfig{Enabled: inspected.ResourceManagerEnabled, Defaults: resourceValues(inspected.ResourceDefaults), Queues: queues,
			AdvertisedWorkerCapacity: resourceValues(inspected.WorkerCapacity), CustomKeys: append([]string{}, inspected.WorkerCustomKeys...), EstimatorConfigured: inspected.EstimatorConfigured}
		service := deps.Engine.Artifacts()
		if service.Enabled() {
			out.Artifacts.Enabled = true
			out.Artifacts.Backend = nullable(service.Backend().Name())
			out.Artifacts.DefaultBucket = nullable(service.DefaultBucket())
			if cache := deps.Engine.ArtifactCache(); cache != nil {
				out.Artifacts.Cache = &ArtifactCacheConfig{Directory: cache.Dir(), BudgetBytes: cache.Budget(), UsedBytes: cache.Used()}
			}
		}
		return out, nil
	})
}
