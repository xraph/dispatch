// Package authority supplies the shared real Authsome bootstrap for qualification.
package authority

import (
	"github.com/xraph/authsome"
	"github.com/xraph/authsome/bridge"
	ap "github.com/xraph/authsome/plugins/apikey"
	"github.com/xraph/authsome/store"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/warden"
)

// New uses the same engine configuration for browser regressions and machine
// hosts. The caller migrates its store and starts the engine before issuance.
// Memory Chronicle is bootstrap telemetry only, never sink-delivery evidence.
func New(st store.Store, w *warden.Engine, appID string, registry auth.Registry, options ...authsome.Option) (*authsome.Engine, error) {
	options = append([]authsome.Option{authsome.WithStore(st), authsome.WithWarden(w), authsome.WithChronicle(bridge.NewMemoryChronicle()), authsome.WithDisableMigrate(), authsome.WithAppID(appID), authsome.WithPlugin(ap.New())}, options...)
	e, err := authsome.NewEngine(options...)
	if err != nil {
		return nil, err
	}
	if registry != nil {
		e.SetAuthRegistry(registry)
	}
	return e, nil
}

func NewEncrypted(st store.Store, w *warden.Engine, appID string, registry auth.Registry, key []byte) (*authsome.Engine, error) {
	encryptor, err := bridge.NewAESGCMEncryptor(key)
	if err != nil {
		return nil, err
	}
	return New(st, w, appID, registry, authsome.WithTokenEncryptor(encryptor))
}
