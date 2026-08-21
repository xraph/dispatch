package extension

import (
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/resource"
)

// Config holds configuration for the Dispatch Forge extension.
// Fields can be set programmatically via Option functions or loaded from
// YAML configuration files (under "extensions.dispatch" or "dispatch" keys).
type Config struct {
	// BasePath is the URL prefix for all dispatch API routes.
	BasePath string `default:"/dispatch" json:"base_path" mapstructure:"base_path" yaml:"base_path"`

	// DisableRoutes disables the registration of HTTP routes.
	// Useful when embedding Dispatch for background processing only.
	DisableRoutes bool `default:"false" json:"disable_routes" mapstructure:"disable_routes" yaml:"disable_routes"`

	// DisableMigrate disables auto-migration on start.
	DisableMigrate bool `default:"false" json:"disable_migrate" mapstructure:"disable_migrate" yaml:"disable_migrate"`

	// Dispatch is parsed from YAML (extensions.dispatch.dispatch.* or
	// dispatch.dispatch.*) but not currently applied anywhere: nothing in
	// this package reads e.config.Dispatch, and dispatch.Config's own
	// fields carry no yaml/mapstructure/json struct tags of their own, so
	// even a populated value here binds by Go field name at best. Setting
	// concurrency, queues, poll interval, or any other dispatch.Config
	// field under this key parses without error and has no effect.
	//
	// The dispatcher itself IS configurable through this extension —
	// just not through this field. WithConcurrency, WithQueues,
	// WithPollInterval, WithMaxPollInterval, WithHeartbeatInterval,
	// WithStaleJobThreshold, WithWorkerStoreCallTimeout, and the
	// WithCron* options (options.go) each translate one dispatch.Config
	// field into the matching dispatch.With* functional option; use
	// those from Go, not this field from YAML.
	Dispatch dispatch.Config `json:"dispatch" mapstructure:"dispatch" yaml:"dispatch"`

	// GroveDatabase is the name of a grove.DB registered in the DI container.
	// When set, the extension resolves this named database and auto-constructs
	// the appropriate store based on the driver type (pg/sqlite/mongo).
	// When empty and WithGroveDatabase was called, the default (unnamed) DB is used.
	GroveDatabase string `json:"grove_database" mapstructure:"grove_database" yaml:"grove_database"`

	// GroveKV is the name of a grove kv.Store registered in the DI container.
	// When set, the extension resolves this named KV store and auto-constructs
	// a Redis-backed store. When empty and WithGroveKV was called, the default
	// (unnamed) kv.Store is used.
	GroveKV string `json:"grove_kv" mapstructure:"grove_kv" yaml:"grove_kv"`

	// Artifacts configures the artifact plane.
	Artifacts ArtifactConfig `json:"artifacts" mapstructure:"artifacts" yaml:"artifacts"`

	// Resources configures the worker's resource model.
	Resources ResourceConfig `json:"resources" mapstructure:"resources" yaml:"resources"`

	// Execution configures which isolation rungs beyond the in-process
	// default are available to job definitions that declare a stronger
	// minimum via job.WithExecution.
	Execution ExecutionConfig `json:"execution" mapstructure:"execution" yaml:"execution"`

	// EnableDWP enables the Dispatch Wire Protocol for real-time
	// client communication (WebSocket, SSE, HTTP RPC).
	EnableDWP bool `default:"false" json:"enable_dwp" mapstructure:"enable_dwp" yaml:"enable_dwp"`

	// DWPBasePath is the URL prefix for DWP endpoints.
	// Default is "/dwp".
	DWPBasePath string `json:"dwp_base_path" mapstructure:"dwp_base_path" yaml:"dwp_base_path"`

	// RequireConfig requires config to be present in YAML files.
	// If true and no config is found, Register returns an error.
	RequireConfig bool `json:"-" yaml:"-"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		BasePath: "/dispatch",
	}
}

// ArtifactConfig configures the artifact plane — Dispatch's tracked
// object storage for job inputs and outputs.
//
// The plane is entirely opt-in. With no backend resolved, Dispatch
// behaves exactly as it did before artifacts existed.
type ArtifactConfig struct {
	// Enabled turns the artifact plane on. When false, no backend is
	// resolved even if a Trove instance is present in the container.
	Enabled bool `default:"false" json:"enabled" mapstructure:"enabled" yaml:"enabled"`

	// TroveStore is the name of a *trove.Trove registered in the DI
	// container. Empty resolves the default (unnamed) instance, which is
	// what a single-store Trove extension provides.
	TroveStore string `json:"trove_store" mapstructure:"trove_store" yaml:"trove_store"`

	// Bucket is where Dispatch writes the ephemeral artifacts it owns.
	Bucket string `default:"dispatch-artifacts" json:"bucket" mapstructure:"bucket" yaml:"bucket"`

	// EphemeralPrefix is the key prefix for Dispatch-owned objects.
	EphemeralPrefix string `default:"ephemeral" json:"ephemeral_prefix" mapstructure:"ephemeral_prefix" yaml:"ephemeral_prefix"`

	// Retention is how long an ephemeral artifact survives after every
	// owner reaches a terminal state.
	Retention time.Duration `default:"168h" json:"retention" mapstructure:"retention" yaml:"retention"`

	// PurgeGrace is how long a soft-deleted artifact's bytes survive
	// before the purge pass removes them. It is the window in which a
	// mistaken sweep can still be caught.
	PurgeGrace time.Duration `default:"24h" json:"purge_grace" mapstructure:"purge_grace" yaml:"purge_grace"`

	// Cache configures the worker-local staging cache.
	Cache ArtifactCacheConfig `json:"cache" mapstructure:"cache" yaml:"cache"`
}

// ArtifactCacheConfig configures the worker-local staging cache.
type ArtifactCacheConfig struct {
	// Dir is where staged artifacts are held on local disk.
	Dir string `default:"/var/lib/dispatch/cache" json:"dir" mapstructure:"dir" yaml:"dir"`

	// Budget caps the bytes the cache may hold. A job needing more
	// staging space than is free waits rather than filling the volume.
	//
	// With the resource model on this becomes the shared ledger's disk
	// capacity rather than a private ceiling inside the cache, so it is
	// the same number a job's disk requirement is admitted against.
	Budget int64 `json:"budget" mapstructure:"budget" yaml:"budget"`
}

// ExecutionConfig configures which execution rungs beyond the always-
// present in-process default a deployment makes available.
//
// A job definition declares the isolation it needs with
// job.WithExecution(exec.Isolate(...)); this block decides which rungs
// EXIST to satisfy that declaration — it never changes what any
// definition asks for, and it never lets a declaration that cannot be
// satisfied run anyway. engine.RegisterChecked already refuses a policy
// nothing configured here can satisfy (exec.ErrNoExecutor), unless the
// definition itself opted into exec.AllowDowngrade — that per-definition
// choice is deliberately not something this block can override, because a
// config-wide override would be exactly the silent downgrade
// RegisterChecked exists to prevent.
//
// The whole block is additive and opt-in: a deployment that configures
// none of it registers no extra executor, and every job keeps running
// in-process exactly as it does today.
type ExecutionConfig struct {
	// Subprocess configures the out-of-process rung (exec.LevelProcess) —
	// the handler runs in a re-exec'd child process instead of the
	// worker's own address space. A zero value (Enabled: false, the
	// default) registers nothing.
	Subprocess SubprocessConfig `json:"subprocess" mapstructure:"subprocess" yaml:"subprocess"`
}

// SubprocessConfig configures exec/subprocess.Executor.
//
// This rung refuses to launch outside Unix (exec/subprocess's checkLaunch
// and Available); the extension checks Available itself at startup, so
// enabling this on an unsupported platform fails registration once,
// loudly, instead of failing every job's first launch attempt at
// runtime.
type SubprocessConfig struct {
	// Enabled registers the subprocess executor with the engine. Without
	// it, nothing else in this struct has any effect.
	Enabled bool `default:"false" json:"enabled" mapstructure:"enabled" yaml:"enabled"`

	// Binary overrides the path to the binary the executor re-execs for
	// every attempt. Empty resolves os.Executable() — the worker's own
	// binary — which is correct for every deployment that has not split
	// the sandboxed handlers into a separate build.
	Binary string `json:"binary" mapstructure:"binary" yaml:"binary"`

	// User and Group are the uid/gid the child process runs as
	// (subprocess.WithUser). Both must be set together, or neither: a
	// uid with no configured gid is rejected at startup rather than
	// silently running the child under the worker's own primary group.
	//
	// Zero means "not configured" rather than uid/gid 0 — this config
	// surface has no way to request running the child as root, which is
	// deliberate: it is never the isolation this rung exists to provide.
	User  int `json:"user" mapstructure:"user" yaml:"user"`
	Group int `json:"group" mapstructure:"group" yaml:"group"`

	// AllowSameUser is the single opt-out for running this rung unisolated
	// on the uid boundary (subprocess.WithAllowSameUser): it permits User
	// to name the worker's own uid, and it permits leaving User unset
	// entirely. Without it, either shape refuses at startup —
	// resolveExecutionOptions rejects it during Register, before this
	// worker ever starts processing jobs — rather than passing cleanly
	// and only then failing every attempt's launch, forever, once the
	// deployment is already running. That is a deliberate security
	// default (see WithAllowSameUser and checkLaunch, which enforces the
	// same rule again at Run() for callers that build subprocess.Executor
	// directly instead of through this config) that this config surface
	// passes through rather than working around: nothing here defaults it
	// to true, so a configuration mistake cannot silently defeat it.
	AllowSameUser bool `default:"false" json:"allow_same_user" mapstructure:"allow_same_user" yaml:"allow_same_user"`

	// ScratchDir is the root directory both the child process's working
	// directory (subprocess.WithScratchDir) and, when the artifact plane
	// is also enabled, the Runner's scratch OutputDir
	// (engine.WithScratchRoot) are created under. Empty means
	// os.TempDir() for both, their own independent defaults.
	//
	// Configuring this with the artifact plane OFF is not an error: the
	// child still gets a scratch working directory, but nothing commits
	// its outputs and PriorOutputs stays empty, exactly as
	// worker.Runner.WithArtifacts documents for a nil service — the
	// extension logs a warning at startup so that is a deliberate choice,
	// not a silent one.
	ScratchDir string `json:"scratch_dir" mapstructure:"scratch_dir" yaml:"scratch_dir"`

	// Rlimits configures POSIX resource limits applied to the child
	// (subprocess.WithRlimits). Fields are in bytes (AddressSpace, FSize,
	// Core) or counts (NoFile, NProc); zero leaves that limit at whatever
	// the worker itself runs with — except Core, which buildEnv forces
	// to zero unconditionally regardless of this value, so a configured
	// Core is always ignored (see RlimitsConfig.Core below). There is no
	// unit-suffixed string parsing here (no "16GB") — this repo takes no
	// new dependency to provide one, and every other byte-valued config
	// field (ArtifactCacheConfig.Budget, resource.Set) is already a plain
	// integer for the same reason.
	Rlimits RlimitsConfig `json:"rlimits" mapstructure:"rlimits" yaml:"rlimits"`

	// StrictRlimits makes a configured rlimit that did not actually take
	// effect a launch failure instead of a silently ignored warning (see
	// subprocess.WithStrictRlimits).
	StrictRlimits bool `default:"false" json:"strict_rlimits" mapstructure:"strict_rlimits" yaml:"strict_rlimits"`
}

// RlimitsConfig configures the child process's POSIX resource limits. See
// subprocess.Rlimits for what each field does; the field names and units
// here mirror it exactly.
type RlimitsConfig struct {
	// AddressSpace caps RLIMIT_AS in bytes.
	AddressSpace int64 `json:"address_space" mapstructure:"address_space" yaml:"address_space"`
	// NoFile caps RLIMIT_NOFILE, the open file descriptor count.
	NoFile int64 `json:"nofile" mapstructure:"nofile" yaml:"nofile"`
	// NProc caps RLIMIT_NPROC, the number of processes the child's uid
	// may run.
	NProc int64 `json:"nproc" mapstructure:"nproc" yaml:"nproc"`
	// Core caps RLIMIT_CORE. Accepted for API symmetry; subprocess forces
	// the child's actual core limit to zero unconditionally regardless of
	// this value — see subprocess.Rlimits.Core.
	Core int64 `json:"core" mapstructure:"core" yaml:"core"`
	// FSize caps RLIMIT_FSIZE in bytes.
	FSize int64 `json:"fsize" mapstructure:"fsize" yaml:"fsize"`
}

// ResourceConfig configures how this worker's capacity is derived and
// whether jobs are admitted against it at all.
//
// Off by default, and off means the RUNTIME does nothing new: no manager
// is constructed, the pool offers no dequeue budget, every store backend
// skips its fit predicate, and the staging cache keeps the private disk
// budget it has always had. A deployment that does not set this behaves
// at runtime as it did before the resource model existed.
//
// Two things happen regardless, and neither is under this flag. The
// resource columns are added by migration on every upgrade, because a
// job row has to be readable by every worker in a mixed-version fleet;
// on Postgres that includes an index build, which is why it is done
// CONCURRENTLY. And every enqueue writes those columns, at their zero
// values when nothing declares a requirement.
type ResourceConfig struct {
	// Enabled turns on capacity detection and resource-aware admission.
	Enabled bool `default:"false" json:"enabled" mapstructure:"enabled" yaml:"enabled"`

	// CPUOvercommit multiplies the detected core count. CPU is
	// compressible — exceeding it makes jobs slow rather than dead — so
	// values above 1.0 are a legitimate throughput trade. Zero means 1.0.
	//
	// There is deliberately no memory equivalent. Overcommitting memory
	// is how a box enters the OOM cascade this model exists to prevent.
	CPUOvercommit float64 `default:"1.0" json:"cpu_overcommit" mapstructure:"cpu_overcommit" yaml:"cpu_overcommit"`

	// MemoryFraction is the share of the detected memory limit to
	// advertise, leaving the rest for the Go runtime, the page cache, and
	// everything else on the box. Zero means 0.8.
	MemoryFraction float64 `default:"0.8" json:"memory_fraction" mapstructure:"memory_fraction" yaml:"memory_fraction"`

	// Explicit overrides detection per key, and is the ONLY way to
	// declare a custom resource: there is no detection for "fpga".
	// Quantities are canonical units — cpu in millicores, memory and disk
	// in bytes, gpu in milli-devices, custom keys in whatever integer the
	// declaring job means by them.
	Explicit resource.Set `json:"explicit" mapstructure:"explicit" yaml:"explicit"`

	// CustomKeys narrows the custom resource keys this worker advertises
	// at dequeue. Empty advertises every custom key in the detected
	// capacity, which is the honest default.
	CustomKeys []string `json:"custom_keys" mapstructure:"custom_keys" yaml:"custom_keys"`
}
