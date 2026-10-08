package extension

import (
	"fmt"

	"github.com/xraph/forge"
	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	dispatchcontract "github.com/xraph/dispatch/extension/contract"
	"github.com/xraph/dispatch/store"
)

// RegisterContractContributor exposes the initialized engine to the dashboard.
func (e *Extension) RegisterContractContributor(d *dispatcher.Dispatcher, reg fc.Registry, wreg fc.WardenRegistry) error {
	logger := e.Logger()
	if logger == nil {
		logger = forge.NewNoopLogger()
	}
	if e.eng == nil {
		logger.Warn("dispatch: engine not initialized; skipping contract contributor registration")
		return nil
	}
	s, ok := e.eng.Dispatcher().Store().(store.Store)
	if !ok {
		return fmt.Errorf("dispatch: dashboard requires cursor-capable stores")
	}
	return dispatchcontract.Register(d, reg, wreg, dispatchcontract.Deps{Engine: e.eng, Store: s, Logger: logger})
}
