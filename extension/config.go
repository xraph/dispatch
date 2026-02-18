package extension

import "github.com/xraph/dispatch"

// Config holds configuration for the Dispatch Forge extension.
type Config struct {
	// BasePath is the URL prefix for all dispatch API routes.
	BasePath string `default:"/api/dispatch" json:"base_path"`

	// DisableRoutes disables the registration of HTTP routes.
	// Useful when embedding Dispatch for background processing only.
	DisableRoutes bool `default:"false" json:"disable_routes"`

	// DisableMigrate disables auto-migration on start.
	DisableMigrate bool `default:"false" json:"disable_migrate"`

	// Dispatch holds the core dispatcher configuration.
	Dispatch dispatch.Config `json:"dispatch"`
}
