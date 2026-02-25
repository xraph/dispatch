package extension

import "github.com/xraph/dispatch"

// Config holds configuration for the Dispatch Forge extension.
// Fields can be set programmatically via Option functions or loaded from
// YAML configuration files (under "extensions.dispatch" or "dispatch" keys).
type Config struct {
	// BasePath is the URL prefix for all dispatch API routes.
	BasePath string `default:"/api/dispatch" json:"base_path" mapstructure:"base_path" yaml:"base_path"`

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

	// RequireConfig requires config to be present in YAML files.
	// If true and no config is found, Register returns an error.
	RequireConfig bool `json:"-" yaml:"-"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		BasePath: "/api/dispatch",
	}
}
