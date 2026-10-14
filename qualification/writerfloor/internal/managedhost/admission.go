package managedhost

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// ProvisionAuthority issues only the original accepted finite provision step.
type ProvisionAuthority interface {
	AuthorizeManagedProviderStep(context.Context, managed.ProviderStepAuthorization) (managed.ProviderStepGrant, error)
}

// CreationAdmission connects fresh provision grants to the existing authenticated
// admission Runtime. The volatile grant is lost on replacement, never reconstructed
// from a receipt. The durable ledger and creation journal independently prevent resend.
type CreationAdmission struct {
	authority ProvisionAuthority
	custody   *CreationTransport
	mu        sync.Mutex
	grants    map[string]admission.Invocation
}

// NewCreationAdmission installs the verifier and delegate for one fixed custodian.
// Pass this same object to admission.NewRuntime, then expose only host.Boundary.
func NewCreationAdmission(authority ProvisionAuthority, custody *CreationTransport) (*CreationAdmission, error) {
	if nilInterface(authority) || custody == nil {
		return nil, managed.ErrUnsupported
	}
	return &CreationAdmission{authority: authority, custody: custody, grants: map[string]admission.Invocation{}}, nil
}

func creationInvocation(record creation, key string) (admission.Invocation, error) {
	raw, err := bounded(record.Permission)
	if err != nil {
		return admission.Invocation{}, err
	}
	p := record.Permission
	slot := string(p.Step.Kind)
	if p.Step.Service != "" {
		slot += ":" + p.Step.Service
	}
	return admission.Invocation{ID: p.CommandID.String(), Tenant: p.Step.TenantID, Target: p.Step.InstanceID.String(), Operation: p.Step.ProviderOperationID.String(), Attempt: "provision", Generation: p.Physical.Generation, Permission: managed.Digest(raw), Command: key, Kind: "provider", Slot: slot}, nil
}

// Authorize captures the fresh grant and retains exact creation inputs. The returned
// invocation is sent through the existing mTLS/Authsome/Warden admission boundary.
// Returning an error after issuance leaves a possible-effect obligation in Ctrlplane.
func (a *CreationAdmission) Authorize(ctx context.Context, request managed.ProviderStepAuthorization, command docker.ManagedDockerCommand) (admission.Invocation, error) {
	var empty admission.Invocation
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	// Dropping an unverifiable grant preserves its possible-effect obligation. It
	// never permits a replacement grant to be recovered from historical acceptance.
	for key, invocation := range a.grants {
		if _, err := a.verify(ctx, invocation); err != nil {
			delete(a.grants, key)
		}
	}
	if len(a.grants) >= managed.MaxMembers*2+1 {
		return empty, managed.ErrCapacity
	}
	grant, err := a.authority.AuthorizeManagedProviderStep(ctx, request)
	if err != nil {
		return empty, err
	}
	if !grant.NewlyIssued {
		return empty, managed.ErrUnavailable
	}
	if operationErr := a.custody.PrepareCreation(ctx, request, command); operationErr != nil {
		return empty, operationErr
	}
	key, err := creationKey(a.custody.backend, command)
	if err != nil {
		return empty, err
	}
	raw, err := a.custody.journal.Load(ctx, key)
	if err != nil {
		return empty, err
	}
	var record creation
	if operationErr := exactDecode(raw, &record); operationErr != nil {
		return empty, operationErr
	}
	if operationErr := a.custody.verify(ctx, record); operationErr != nil {
		return empty, operationErr
	}
	if record.State != "prepared" || !reflect.DeepEqual(record.Permission, managed.ProviderPermissionWire(grant.Permission)) {
		return empty, managed.ErrAuthority
	}
	invocation, err := creationInvocation(record, key)
	if err != nil {
		return empty, err
	}
	if _, exists := a.grants[invocation.ID]; exists {
		return empty, managed.ErrConflict
	}
	a.grants[invocation.ID] = invocation
	return invocation, nil
}

func (a *CreationAdmission) verify(ctx context.Context, invocation admission.Invocation) (creation, error) {
	var empty creation
	expected, exists := a.grants[invocation.ID]
	if !exists || expected != invocation || !objectID(invocation.Command) {
		return empty, managed.ErrAuthority
	}
	raw, err := a.custody.journal.Load(ctx, invocation.Command)
	if err != nil {
		return empty, err
	}
	var record creation
	if operationErr := exactDecode(raw, &record); operationErr != nil {
		return empty, operationErr
	}
	if operationErr := a.custody.verify(ctx, record); operationErr != nil {
		return empty, operationErr
	}
	key, err := creationKey(a.custody.backend, record.Command)
	if err != nil {
		return empty, err
	}
	exact, err := creationInvocation(record, key)
	if err != nil || exact != invocation || record.State != "prepared" {
		return empty, managed.ErrAuthority
	}
	target, err := a.custody.acceptances.ReadManagedTarget(ctx, record.Authorization.Step.Key)
	if err != nil {
		return empty, err
	}
	revision, err := record.Permission.Revision.Uint64()
	if err != nil || target == nil || target.Revision != revision || target.LeaseOwner != record.Authorization.Owner || target.LeaseEpoch != record.Authorization.LeaseEpoch || !target.LeaseUntil.After(time.Now().UTC()) {
		return empty, managed.ErrAuthority
	}
	return record, nil
}

// Verify checks the exact fresh grant while the existing admission ledger holds its
// floor/incarnation transaction. Reading historical permission cannot recreate it.
func (a *CreationAdmission) Verify(ctx context.Context, invocation admission.Invocation) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.verify(ctx, invocation)
	return err
}

// Dispatch consumes the grant before the first provider call. Only the configured
// admission Runtime calls this after its independent delegation record is durable.
func (a *CreationAdmission) Dispatch(ctx context.Context, delegation admission.Delegation) error {
	a.mu.Lock()
	expected, exists := a.grants[delegation.Invocation.ID]
	if !exists || expected != delegation.Invocation {
		a.mu.Unlock()
		return managed.ErrAuthority
	}
	record, err := a.verify(ctx, delegation.Invocation)
	delete(a.grants, delegation.Invocation.ID)
	a.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = a.custody.Execute(ctx, record.Command)
	return err
}

var (
	_ admission.Verifier = (*CreationAdmission)(nil)
	_ admission.Delegate = (*CreationAdmission)(nil)
)
