package contract

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"

	fc "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/loader"
)

// ContributorName joins the Go contributor and the React plugin.
const ContributorName = "dispatch"

//go:embed manifest.yaml
var manifestYAML []byte

type binding struct {
	intent string
	bind   func(*dispatcher.Dispatcher) error
}

func query[I, O any](intent string, fn func(context.Context, I, fc.Principal) (O, error)) binding {
	return binding{intent: intent, bind: func(d *dispatcher.Dispatcher) error {
		return dispatcher.RegisterQuery(d, ContributorName, intent, 1, fn)
	}}
}
func command[I, O any](intent string, fn func(context.Context, I, fc.Principal) (O, error)) binding {
	return binding{intent: intent, bind: func(d *dispatcher.Dispatcher) error {
		return dispatcher.RegisterCommand(d, ContributorName, intent, 1, fn)
	}}
}

func bindings(deps Deps) []binding {
	return []binding{
		query("artifacts.list", artifactsListHandler(deps)),
		query("artifacts.get", artifactsGetHandler(deps)),
		query("artifacts.forJob", artifactsForJobHandler(deps)),
		query("artifacts.presign", artifactsPresignHandler(deps)),
		query("workers.list", workersListHandler(deps)),
		query("workers.get", workersGetHandler(deps)),
		query("queues.list", queuesListHandler(deps)),
		query("queues.get", queuesGetHandler(deps)),
		query("handlers.list", handlersListHandler(deps)),
		query("handlers.get", handlersGetHandler(deps)),
		query("engine.config", engineConfigHandler(deps)),
		query("overview.summary", overviewSummaryHandler(deps)),
		query("workflows.list", workflowsListHandler(deps)),
		query("workflows.get", workflowsGetHandler(deps)),
		query("workflows.replayPreview", workflowsReplayPreviewHandler(deps)),
		command("workflows.replayFrom", workflowsReplayFromHandler(deps)),
		query("crons.list", cronsListHandler(deps)),
		query("crons.get", cronsGetHandler(deps)),
		command("crons.enable", cronToggleHandler(deps, true)),
		command("crons.disable", cronToggleHandler(deps, false)),
		command("crons.delete", cronsDeleteHandler(deps)),
		command("crons.runNow", cronsRunNowHandler(deps)),
		query("dlq.list", dlqListHandler(deps)),
		query("dlq.get", dlqGetHandler(deps)),
		query("dlq.counts", dlqCountsHandler(deps)),
		query("dlq.purgePreview", dlqPurgeHandler(deps, true)),
		command("dlq.replay", dlqReplayHandler(deps)),
		command("dlq.replayAll", dlqReplayAllHandler(deps)),
		command("dlq.delete", dlqDeleteHandler(deps)),
		command("dlq.purge", dlqPurgeHandler(deps, false)),
		query("jobs.list", jobsListHandler(deps)),
		query("jobs.get", jobsGetHandler(deps)),
		query("jobs.counts", jobsCountsHandler(deps)),
		command("jobs.cancel", jobActionHandler(deps, "jobs.cancel", deps.Engine.CancelJob)),
		command("jobs.retry", jobActionHandler(deps, "jobs.retry", deps.Engine.RetryJob)),
	}
}

// Register validates the manifest and binds only implemented intents.
func Register(d *dispatcher.Dispatcher, reg fc.Registry, wreg fc.WardenRegistry, deps Deps) error {
	if err := deps.validate(); err != nil {
		return err
	}
	if d == nil || reg == nil || wreg == nil {
		return fmt.Errorf("dispatch/contract: dispatcher and registries are required")
	}
	if err := wreg.Register(WardenName, operatorWarden{deps: deps}); err != nil {
		return err
	}
	manifest, err := loader.Load(bytes.NewReader(manifestYAML), "dispatch/contract/manifest.yaml")
	if err != nil {
		return fmt.Errorf("dispatch/contract: load manifest: %w", err)
	}
	if err := loader.Validate(manifest, wreg); err != nil {
		return fmt.Errorf("dispatch/contract: validate manifest: %w", err)
	}
	declared := make(map[string]bool, len(manifest.Intents))
	for _, intent := range manifest.Intents {
		declared[intent.Name] = true
	}
	handlers := bindings(deps)
	seen := make(map[string]bool, len(handlers))
	for _, binding := range handlers {
		if !declared[binding.intent] || seen[binding.intent] {
			return fmt.Errorf("dispatch/contract: undeclared or repeated binding %s", binding.intent)
		}
		seen[binding.intent] = true
	}
	for intent := range declared {
		if !seen[intent] {
			return fmt.Errorf("dispatch/contract: intent %s has no binding", intent)
		}
	}
	if err := reg.Register(manifest); err != nil {
		return fmt.Errorf("dispatch/contract: register manifest: %w", err)
	}
	for _, binding := range handlers {
		if err := binding.bind(d); err != nil {
			return fmt.Errorf("dispatch/contract: bind %s: %w", binding.intent, err)
		}
	}
	return nil
}
