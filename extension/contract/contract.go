package contract

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/xraph/dispatch/security"

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
		return dispatcher.RegisterQuery(d, ContributorName, intent, 1, fn, dispatcher.RequireKind(fc.KindQuery))
	}}
}
func command[I, O any](deps Deps, intent string, fn func(context.Context, I, fc.Principal) (O, error)) binding {
	return binding{intent: intent, bind: func(d *dispatcher.Dispatcher) error {
		options := []dispatcher.RegisterOption{dispatcher.RequireKind(fc.KindCommand)}
		if durableKind(intent) == fc.KindCommand {
			options = append(options, dispatcher.BypassIdempotency())
		} else {
			options = append(options, dispatcher.BeforeDispatch(func(ctx context.Context, request fc.Request, principal fc.Principal) error {
				var input I
				if len(request.Params) > 0 {
					raw, err := json.Marshal(request.Params)
					if err != nil || json.Unmarshal(raw, &input) != nil {
						return fc.ErrBadRequest
					}
				}
				if len(request.Payload) > 0 && string(request.Payload) != "null" {
					if json.Unmarshal(request.Payload, &input) != nil {
						return fc.ErrBadRequest
					}
				}
				op := security.ContractOperation(intent)
				target, targetErr := auditInputTarget(intent, input)
				op.Target = target
				if err := deps.authorizeOperation(ctx, principal, op); err != nil {
					return err
				}
				if targetErr != nil {
					return fc.ErrBadRequest
				}
				return nil
			}), dispatcher.IdempotencyScope(func(context.Context, fc.Request, fc.Principal) ([]byte, error) {
				return json.Marshal([]string{"dispatch.installation.v1", deps.Security.Resource.InstallationID, deps.Security.Resource.PolicyTenant})
			}))
		}
		return dispatcher.RegisterCommand(d, ContributorName, intent, 1, fn, options...)
	}}
}

func bindings(deps Deps) []binding {
	return append([]binding{
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
		command(deps, "workflows.replayFrom", workflowsReplayFromHandler(deps)),
		query("crons.list", cronsListHandler(deps)),
		query("crons.get", cronsGetHandler(deps)),
		command(deps, "crons.enable", cronToggleHandler(deps, true)),
		command(deps, "crons.disable", cronToggleHandler(deps, false)),
		command(deps, "crons.delete", cronsDeleteHandler(deps)),
		command(deps, "crons.runNow", cronsRunNowHandler(deps)),
		query("dlq.list", dlqListHandler(deps)),
		query("dlq.get", dlqGetHandler(deps)),
		query("dlq.counts", dlqCountsHandler(deps)),
		query("dlq.purgePreview", dlqPurgeHandler(deps, true)),
		command(deps, "dlq.replay", dlqReplayHandler(deps)),
		command(deps, "dlq.replayAll", dlqReplayAllHandler(deps)),
		command(deps, "dlq.delete", dlqDeleteHandler(deps)),
		command(deps, "dlq.purge", dlqPurgeHandler(deps, false)),
		query("jobs.list", jobsListHandler(deps)),
		query("jobs.get", jobsGetHandler(deps)),
		query("jobs.counts", jobsCountsHandler(deps)),
		command(deps, "jobs.cancel", jobActionHandler(deps, "jobs.cancel", deps.Engine.CancelJob)),
		command(deps, "jobs.retry", jobActionHandler(deps, "jobs.retry", deps.Engine.RetryJob)),
	}, durableBindings(deps)...)
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
	if err := wreg.Register(DurableWardenName, durableWarden{deps: deps}); err != nil {
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
