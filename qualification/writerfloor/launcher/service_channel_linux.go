//go:build linux

package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"syscall"
	"time"

	aid "github.com/xraph/authsome/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
)

// serviceAuthenticationError exposes only a fixed stage to internal diagnostics.
// The underlying error remains available for cancellation and refusal handling.
type serviceAuthenticationError struct {
	class string
	cause error
}

func (e *serviceAuthenticationError) Error() string {
	return "launcher: service authentication refused"
}
func (e *serviceAuthenticationError) Unwrap() error           { return e.cause }
func (e *serviceAuthenticationError) DiagnosticClass() string { return e.class }

func serviceAuthenticationFailure(class string, cause error) error {
	if cause == nil {
		cause = ErrRefused
	}

	return &serviceAuthenticationError{class: class, cause: cause}
}

func serviceCreationMatches(plan ServicePlan, command docker.ManagedDockerCommand, created ServiceCreation) bool {
	return command.Kind == managed.StepCreateContainer && command.ResourceID == "" && hexadecimal(command.NetworkID) && command.Service != nil && reflect.DeepEqual(*command.Service, plan.Service) && created.Target == plan.Target && created.ProviderOperationID == plan.ProviderOperationID && reflect.DeepEqual(created.Command, command) && hexadecimal(created.Custody.ResourceID) && hexadecimal(created.Custody.Incarnation) && created.Custody.OriginalProcess == nil && len(created.Custody.Attestation) > 0 && len(created.Custody.Attestation) <= managed.MaxProviderEffectAttestation
}

// BindCreated consumes only the original signed create result. It cannot adopt an
// inspected container, select another parent, or create a second resource.
func (s *ServiceEnrollment) BindCreated(ctx context.Context, id string, command docker.ManagedDockerCommand) (ServiceBinding, error) {
	if e := ctx.Err(); e != nil {
		return ServiceBinding{}, e
	}
	rawCommand, e := json.Marshal(command)
	if e != nil || len(rawCommand) > MaxRecord {
		return ServiceBinding{}, ErrRefused
	}
	var ownedCommand docker.ManagedDockerCommand
	if operationErr := json.Unmarshal(rawCommand, &ownedCommand); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	command = ownedCommand
	p, e := s.plan(id)
	if e != nil {
		return ServiceBinding{}, e
	}
	lock, e := openJournal(s.root, id)
	if e != nil {
		return ServiceBinding{}, e
	}
	defer lock.close()
	record, e := s.read(id)
	if e != nil {
		return ServiceBinding{}, e
	}
	if record.Phase != "prepared" && record.Phase != "bound" {
		return ServiceBinding{}, ErrUncertain
	}
	created, e := s.reader.ReadServiceCreation(ctx, command)
	if e != nil {
		return ServiceBinding{}, e
	}
	if !serviceCreationMatches(p, command, created) {
		return ServiceBinding{}, ErrRefused
	}
	if record.Creation != nil {
		if record.Phase != "bound" || !reflect.DeepEqual(*record.Creation, created) {
			return ServiceBinding{}, ErrRefused
		}
		return serviceBinding(record)
	}
	raw, e := json.Marshal(created)
	if e != nil || len(raw) > MaxRecord {
		return ServiceBinding{}, ErrRefused
	}
	var owned ServiceCreation
	if operationErr := json.Unmarshal(raw, &owned); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	record.Creation = &owned
	record.Phase = "bound"
	if operationErr := ctx.Err(); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	if operationErr := s.save(record, false); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	return serviceBinding(record)
}
func serviceBinding(record serviceRecord) (ServiceBinding, error) {
	if record.Phase != "bound" || record.Creation == nil {
		return ServiceBinding{}, ErrUncertain
	}
	raw, e := json.Marshal(record.Creation)
	if e != nil {
		return ServiceBinding{}, e
	}
	nonce := ""
	if record.Ready != nil {
		nonce = record.Ready.BootNonce
	}
	return ServiceBinding{BootNonce: nonce, BootstrapID: record.ID, PlanDigest: record.PlanDigest, ResourceID: record.Creation.Custody.ResourceID, Incarnation: record.Creation.Custody.Incarnation, CreationDigest: managed.Digest(raw), Account: record.Account, Key: record.Key, PeerDigest: record.Peer}, nil
}
func (s *ServiceEnrollment) authenticate(ctx context.Context, request *http.Request, record serviceRecord, action string) error {
	if request == nil || record.Phase != "bound" || record.Creation == nil {
		return serviceAuthenticationFailure("channel_record", ErrRefused)
	}
	p, e := s.plan(record.ID)
	if e != nil {
		return serviceAuthenticationFailure("channel_plan", e)
	}
	created, e := s.reader.ReadServiceCreation(ctx, record.Creation.Command)
	if e != nil {
		return serviceAuthenticationFailure("creation_read", e)
	}
	if !serviceCreationMatches(p, record.Creation.Command, created) || !reflect.DeepEqual(created, *record.Creation) {
		return serviceAuthenticationFailure("creation_match", ErrRefused)
	}
	peer, e := readyPeer(request)
	if e != nil || peer != record.Peer {
		return serviceAuthenticationFailure("tls_peer", errors.Join(ErrRefused, e))
	}
	account, e := aid.ParseServiceAccountID(record.Account)
	if e != nil {
		return serviceAuthenticationFailure("machine_account", e)
	}
	key, e := aid.ParseAPIKeyID(record.Key)
	if e != nil {
		return serviceAuthenticationFailure("machine_key", e)
	}
	provider := authority.Provider{Engine: s.host.engine, EnvironmentID: s.host.environment.String(), Binding: ecosystem.Binding{AppID: s.host.app.String(), TenantID: s.host.tenant}, Credentials: []authority.Credential{{KeyID: key, AccountID: account}}}
	principal, e := provider.Authenticate(ctx, request)
	if e != nil || principal == nil || principal.Subject != record.Account || !slices.Contains(principal.Scopes, action) {
		stage := "machine_provider"
		var classified interface{ DiagnosticClass() string }
		if errors.As(e, &classified) {
			stage += "_" + classified.DiagnosticClass()
		}
		return serviceAuthenticationFailure(stage, errors.Join(ErrRefused, e))
	}
	checkCtx := warden.WithNamespace(warden.WithTenant(ctx, s.host.app.String(), s.host.tenant), "")
	result, e := s.policy.Check(checkCtx, &warden.CheckRequest{Subject: warden.Subject{Kind: warden.SubjectServiceAcct, ID: record.Account}, Action: warden.Action{Name: action}, Resource: warden.Resource{Type: "managed_service", ID: record.ID}, TenantID: s.host.tenant})
	if e != nil || result == nil || !result.Allowed || len(result.Obligations) != 0 {
		return serviceAuthenticationFailure("warden", errors.Join(ErrRefused, e))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return serviceAuthenticationFailure("authentication_context", contextErr)
	}

	return nil
}

// AuthenticateReady binds the exact configured identity and a one-process nonce
// after real machine/channel authentication. It does not claim a live native probe.
func (s *ServiceEnrollment) AuthenticateReady(ctx context.Context, request *http.Request, ready ServiceReady) (ServiceBinding, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	p, e := s.plan(ready.BootstrapID)
	if e != nil || !serviceReadyMatches(p, ready) {
		return ServiceBinding{}, ErrRefused
	}
	lock, e := openJournal(s.root, ready.BootstrapID)
	if e != nil {
		return ServiceBinding{}, e
	}
	defer lock.close()
	record, e := s.read(ready.BootstrapID)
	if e != nil {
		return ServiceBinding{}, e
	}
	if operationErr := s.authenticate(ctx, request, record, "managed-service.ready"); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	if record.Ready != nil {
		if *record.Ready != ready {
			return ServiceBinding{}, ErrRefused
		}
		return serviceBinding(record)
	}
	record.Ready = &ready
	if operationErr := ctx.Err(); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	if operationErr := s.save(record, false); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	return serviceBinding(record)
}

// AuthenticateChannel requires the saved process nonce in addition to the fixed
// Authsome identity, Warden action, verified TLS peer and current creation custody.
func (s *ServiceEnrollment) AuthenticateChannel(ctx context.Context, request *http.Request, id string) (ServiceBinding, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lock, e := openServiceJournal(ctx, s.root, id)
	if e != nil {
		return ServiceBinding{}, serviceAuthenticationFailure("journal_lock", e)
	}
	defer lock.close()
	record, e := s.read(id)
	if e != nil {
		return ServiceBinding{}, serviceAuthenticationFailure("journal_read", e)
	}
	if request == nil || record.Ready == nil || len(request.Header.Values("X-Writerfloor-Boot")) != 1 || request.Header.Get("X-Writerfloor-Boot") != record.Ready.BootNonce {
		return ServiceBinding{}, serviceAuthenticationFailure("boot_nonce", ErrRefused)
	}
	if operationErr := s.authenticate(ctx, request, record, "managed-service.channel"); operationErr != nil {
		return ServiceBinding{}, operationErr
	}
	binding, bindingErr := serviceBinding(record)
	if bindingErr != nil {
		return ServiceBinding{}, serviceAuthenticationFailure("channel_binding", bindingErr)
	}

	return binding, nil
}

// Close revokes only this machine channel. It never reports a provider effect or
// native guard closed, and never deletes a resource. Closed records cannot reopen.
func (s *ServiceEnrollment) Close(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lock, e := openJournal(s.root, id)
	if e != nil {
		return e
	}
	defer lock.close()
	record, e := s.read(id)
	if e != nil {
		return e
	}
	record.Phase = "closed"
	if operationErr := s.save(record, false); operationErr != nil {
		return operationErr
	}
	keyID, e := aid.ParseAPIKeyID(record.Key)
	if e != nil {
		return ErrUncertain
	}
	key, e := s.host.engine.APIKeyStore().GetAPIKey(ctx, keyID)
	if e != nil || key == nil || key.ServiceAccountID.String() != record.Account || key.AppID != s.host.app || key.EnvID != s.host.environment {
		return ErrRefused
	}
	if key.Revoked {
		return nil
	}
	key.Revoked = true
	return s.host.engine.APIKeyStore().UpdateAPIKey(ctx, key)
}

// openServiceJournal waits only for transient contention within the authentication
// deadline. Credential and custody failures are never retried.
func openServiceJournal(ctx context.Context, root, id string) (*journal, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lock, err := openJournal(root, id)
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return lock, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
