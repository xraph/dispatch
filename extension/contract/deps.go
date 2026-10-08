// Package contract serves the operator-wide Dispatch dashboard.
package contract

import (
	"fmt"

	"github.com/xraph/forge"

	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/store"
)

// Deps contains the running engine and the stores its intents inspect.
type Deps struct {
	Engine *engine.Engine
	Store  store.Store
	Logger forge.Logger
}

func (d Deps) validate() error {
	if d.Engine == nil {
		return fmt.Errorf("dispatch/contract: Engine is required")
	}
	if d.Store == nil {
		return fmt.Errorf("dispatch/contract: Store is required")
	}
	return nil
}
