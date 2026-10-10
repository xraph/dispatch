package operatorhost

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xraph/authsome/environment"
	aid "github.com/xraph/authsome/id"
	"github.com/xraph/forge/extensions/auth"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
)

type commandRuntime struct {
	mu      sync.Mutex
	workers map[string]*drt.Worker
	handles map[durable.Key][]drt.AsyncActivityHandle
}

func (h *Host) configureCommands(ctx context.Context, appID aid.AppID, registry auth.Registry) error {
	h.runtime = commandRuntime{workers: map[string]*drt.Worker{}, handles: map[durable.Key][]drt.AsyncActivityHandle{}}
	for _, build := range []string{"operator-v1", "operator-v2"} {
		worker, err := drt.NewWorker(h.Store, drt.Options{Namespace: "production", BuildID: build, Queue: "operator", Owner: "operator-fixture", Workflows: map[string]drt.WorkflowFunc{
			"operator": func(w *drt.Workflow, input []byte) ([]byte, error) {
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
				w.SetQueryHandler("mutate", func([]byte) ([]byte, error) { w.Activity("forbidden", "forbidden", "", nil); return nil, nil })
				return w.ReceiveSignal("finish", "finish").Get()
			},
			"continue": func(w *drt.Workflow, input []byte) ([]byte, error) {
				w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
				if string(input) == "successor" {
					return w.ReceiveSignal("finish", "finish").Get()
				}
				return nil, w.ContinueAsNew([]byte("successor"), drt.ContinueOptions{})
			},
			"async": asyncWorkflow(time.Minute), "async-expiry": asyncWorkflow(250 * time.Millisecond),
		}, Activities: map[string]drt.ActivityFunc{"callback": func(c context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
			handle, err := info.DeferCompletion(c)
			if err == nil {
				h.runtime.mu.Lock()
				h.runtime.handles[handle.Key] = append(h.runtime.handles[handle.Key], handle)
				h.runtime.mu.Unlock()
			}
			return nil, err
		}}})
		if err != nil {
			return err
		}
		h.runtime.workers[build] = worker
	}
	env := &environment.Environment{ID: aid.NewEnvironmentID(), AppID: appID, Name: "Operator", Slug: "operator", Type: environment.TypeDevelopment}
	if err := h.Auth.CreateEnvironment(ctx, env); err != nil {
		return err
	}
	account, err := h.Auth.CreateServiceAccountInEnvironment(ctx, appID, env.ID, "operator-callback", "", []string{operator.CompleteActivity, operator.HeartbeatActivity})
	if err != nil {
		return err
	}
	key, secret, err := h.Auth.CreateServiceAccountAPIKey(ctx, account.ID, "operator-callback", account.Scopes, nil)
	if err != nil {
		return err
	}
	h.Machine = authority.Credential{KeyID: key.ID, AccountID: account.ID, Secret: secret}
	h.Credentials["machine"] = Credential{Subject: account.ID.String(), Token: secret}
	provider := &authority.Provider{Engine: h.Auth, EnvironmentID: env.ID.String(), Binding: ecosystem.Binding{InstallationID: "operator-host", AppID: appID.String(), TenantID: "tenant-production", Namespace: "production", Producer: "dispatch"}, Credentials: []authority.Credential{h.Machine}}
	h.machineProvider = provider
	if err := registry.Register(provider); err != nil {
		return err
	}
	h.CommandPolicies = map[string]wid.PolicyID{}
	for _, action := range []string{operator.StartWorkflow, operator.SignalWorkflow, operator.SignalStartWorkflow, operator.CancelWorkflow, operator.QueryWorkflow, operator.CompleteActivity, operator.HeartbeatActivity} {
		role, kind := "commander", "user"
		if action == operator.CompleteActivity || action == operator.HeartbeatActivity {
			role, kind = "machine", "service_acct"
		}
		p := &policy.Policy{ID: wid.NewPolicyID(), AppID: appID.String(), TenantID: "tenant-production", NamespacePath: "production", Name: action, Effect: policy.EffectAllow, IsActive: true, Subjects: []policy.SubjectMatch{{Kind: kind, ID: h.Credentials[role].Subject}}, Actions: []string{action}, Resources: []string{"dispatch_namespace:production"}, Conditions: []policy.Condition{{Field: "resource.installation_id", Operator: policy.OpEquals, Value: "operator-host"}, {Field: "resource.app_id", Operator: policy.OpEquals, Value: appID.String()}, {Field: "resource.tenant_id", Operator: policy.OpEquals, Value: "tenant-production"}}}
		if action == operator.StartWorkflow {
			p.Conditions = append(p.Conditions, policy.Condition{Field: "resource.workflow_type", Operator: policy.OpIn, Value: []string{"operator", "continue", "async", "async-expiry"}})
		}
		if err := h.Policies.CreatePolicy(ctx, p); err != nil {
			return err
		}
		h.CommandPolicies[action] = p.ID
	}
	legacy := &policy.Policy{ID: wid.NewPolicyID(), TenantID: "tenant-audit", Name: "legacy-command", Effect: policy.EffectAllow, IsActive: true, Subjects: []policy.SubjectMatch{{Kind: "user", ID: h.Credentials["commander"].Subject}}, Actions: []string{security.OperatorRead, security.OperatorWrite}, Resources: []string{"dispatch_installation:operator-host"}}
	if err := h.Policies.CreatePolicy(ctx, legacy); err != nil {
		return err
	}
	h.LegacyPolicy = legacy.ID
	return nil
}
func asyncWorkflow(deadline time.Duration) drt.WorkflowFunc {
	return func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ActivityWithOptions("callback", "callback", "", nil, drt.ActivityOptions{StartToCloseTimeout: deadline, HeartbeatTimeout: deadline, RetryPolicy: &drt.RetryPolicy{MaximumAttempts: 2, InitialInterval: time.Millisecond}}).Get()
	}
}
func (h *Host) worker(namespace, build string) (*drt.Worker, error) {
	if namespace != "production" {
		return nil, errors.New("operator fixture: runtime unavailable")
	}
	worker := h.runtime.workers[build]
	if worker == nil {
		return nil, errors.New("operator fixture: runtime unavailable")
	}
	return worker, nil
}

// StartWorkers runs only fixture-owned runtimes. Stop them before closing the host.
func (h *Host) StartWorkers(ctx context.Context) func() error {
	ctx, cancel := context.WithCancel(ctx)
	var group sync.WaitGroup
	failures := make(chan error, len(h.runtime.workers))
	for _, worker := range h.runtime.workers {
		group.Go(func() {
			if err := worker.Run(ctx); err != nil {
				failures <- err
			}
		})
	}
	return func() error {
		cancel()
		group.Wait()
		close(failures)
		var result error
		for err := range failures {
			result = errors.Join(result, err)
		}
		return result
	}
}
