package extension

import (
	"fmt"
	"maps"
	"time"

	"github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/engine"
)

// DurableConfig contains serializable worker routing and timing. Handlers are Go
// registrations supplied through WithDurableWorkflows.
type DurableConfig struct {
	Enabled       bool          `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Namespace     string        `json:"namespace" yaml:"namespace" mapstructure:"namespace"`
	Queue         string        `json:"queue" yaml:"queue" mapstructure:"queue"`
	BuildID       string        `json:"build_id" yaml:"build_id" mapstructure:"build_id"`
	Owner         string        `json:"owner" yaml:"owner" mapstructure:"owner"`
	LeaseDuration time.Duration `json:"lease_duration" yaml:"lease_duration" mapstructure:"lease_duration"`
	PollInterval  time.Duration `json:"poll_interval" yaml:"poll_interval" mapstructure:"poll_interval"`
	StoreTimeout  time.Duration `json:"store_timeout" yaml:"store_timeout" mapstructure:"store_timeout"`
	Concurrency   int           `json:"concurrency" yaml:"concurrency" mapstructure:"concurrency"`
}

// WithDurableWorkflows captures immutable handler registrations immediately.
// Programmatic options replace YAML worker routing and timing as one unit.
func WithDurableWorkflows(options runtime.Options) ExtOption {
	options.Workflows = maps.Clone(options.Workflows)
	options.Activities = maps.Clone(options.Activities)
	return func(e *Extension) {
		clone := options
		clone.Workflows = maps.Clone(options.Workflows)
		clone.Activities = maps.Clone(options.Activities)
		e.durable = &clone
	}
}

// WithDurableHandlers binds Go functions to YAML-configured worker routing.
// Registrations are captured when you create the option and copied per extension.
func WithDurableHandlers(workflows map[string]runtime.WorkflowFunc, activities map[string]runtime.ActivityFunc) ExtOption {
	workflows = maps.Clone(workflows)
	activities = maps.Clone(activities)
	return func(e *Extension) {
		e.durableHandlers = runtime.Options{Workflows: maps.Clone(workflows), Activities: maps.Clone(activities)}
	}
}

func (e *Extension) durableOption() ([]engine.Option, error) {
	if e.durable != nil {
		if len(e.durable.Workflows)+len(e.durable.Activities) == 0 {
			return nil, fmt.Errorf("dispatch: durable handlers are required")
		}
		return []engine.Option{engine.WithDurableWorkflows(*e.durable)}, nil
	}
	cfg := e.config.Durable
	if !cfg.Enabled {
		return nil, nil
	}
	if len(e.durableHandlers.Workflows)+len(e.durableHandlers.Activities) == 0 {
		return nil, fmt.Errorf("dispatch: durable handlers are required")
	}
	return []engine.Option{engine.WithDurableWorkflows(runtime.Options{Namespace: cfg.Namespace, Queue: cfg.Queue, BuildID: cfg.BuildID, Owner: cfg.Owner, LeaseDuration: cfg.LeaseDuration, PollInterval: cfg.PollInterval, StoreTimeout: cfg.StoreTimeout, Concurrency: cfg.Concurrency, Workflows: maps.Clone(e.durableHandlers.Workflows), Activities: maps.Clone(e.durableHandlers.Activities)})}, nil
}
