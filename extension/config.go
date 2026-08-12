package extension

import (
	"time"

	"github.com/xraph/dispatch"
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

	// Dispatch holds the core dispatcher configuration.
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
	Budget int64 `json:"budget" mapstructure:"budget" yaml:"budget"`
}
