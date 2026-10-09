package extension

import (
	"github.com/xraph/vessel"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
)

// WithDurableOperators enables namespace-aware inspection. Cursor keys belong to
// the host; store, installation and audit activation come from the extension.
func WithDurableOperators(keys operator.CursorKeys, authorizer operator.Authorizer) ExtOption {
	clone := operator.CursorKeys{Active: keys.Active, Keys: map[string][]byte{}}
	for version, key := range keys.Keys {
		clone.Keys[version] = append([]byte(nil), key...)
	}
	return func(e *Extension) { e.operatorOptions = &operator.Options{CursorKeys: clone, Authorizer: authorizer} }
}
func (e *Extension) operatorService() (*operator.Service, error) {
	if e.operatorOptions == nil {
		return nil, nil
	}
	opts := *e.operatorOptions
	store := e.eng.Dispatcher().Store()
	var ok bool
	opts.Store, ok = store.(durable.Store)
	if !ok {
		return nil, durable.ErrInvalid
	}
	opts.Reads, ok = store.(durable.ReadStore)
	if !ok {
		return nil, durable.ErrInvalid
	}
	opts.Catalog, ok = store.(durable.NamespaceStore)
	if !ok {
		return nil, durable.ErrInvalid
	}
	opts.InstallationID = e.boundary.Resource.InstallationID
	opts.Audit = *e.boundary
	if opts.Authorizer == nil {
		opts.Authorizer = &operator.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return vessel.Inject[*warden.Engine](e.App().Container()) }}
	}
	// Inspection alone does not claim query runtime availability. A later command
	// resolver must prove the persisted namespace/build is served before invocation.
	return operator.New(opts)
}
