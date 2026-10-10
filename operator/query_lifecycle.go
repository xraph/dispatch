package operator

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

const (
	ReadQueryRuntime     = "dispatch.query_runtime.read"
	RegisterQueryRuntime = "dispatch.query_runtime.register"
	VerifyQueryRuntime   = "dispatch.query_runtime.verify"
	RemoveQueryRuntime   = "dispatch.query_runtime.remove"
	FinishQueryRuntime   = "dispatch.query_runtime.finish"
	AbortQueryRuntime    = "dispatch.query_runtime.abort"
)

func queryLifecycleAction(action string) bool {
	switch action {
	case ReadQueryRuntime, RegisterQueryRuntime, VerifyQueryRuntime, RemoveQueryRuntime, FinishQueryRuntime, AbortQueryRuntime:
		return true
	}
	return false
}

// QueryRuntimeHost resolves actual deployed identity and probes retained history.
// ResolveBinding is read-only. Verify runs the configured named historical probes.
type QueryRuntimeHost interface {
	ResolveBinding(context.Context, durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error)
	Verify(context.Context, durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error)
}

// QueryRemovalCompletionVerifier confirms removal of the exact reserved binding.
// It must resolve unknown issued operations before claiming completion.
type QueryRemovalCompletionVerifier interface {
	VerifyRemoved(context.Context, durable.QueryRemovalFence) error
}

// QueryRegistrationPolicy authorizes new issuance against a managed instance.
// Accepted command replay skips this gate and never reactivates a binding.
type QueryRegistrationPolicy func(context.Context, durable.RegisterQueryRuntimeRequest) error

type QueryRuntimeInput struct {
	BuildInput
	RuntimeID string `json:"runtime_id"`
}
type QueryRuntimeCommand struct {
	QueryRuntimeInput
	RequestID       string `json:"request_id"`
	ExpectedVersion string `json:"expected_version"`
}

func (s *Service) queryTarget(in QueryRuntimeInput) (durable.QueryRuntimeTarget, error) {
	build, err := s.buildTarget(in.BuildInput)
	if err != nil {
		return durable.QueryRuntimeTarget{}, err
	}
	target := durable.QueryRuntimeTarget{BuildTarget: build, RuntimeID: in.RuntimeID}
	return target, target.Validate()
}
func (s *Service) queryStore() (durable.QueryRuntimeStore, error) {
	store, ok := s.store.(durable.QueryRuntimeStore)
	if !ok {
		return nil, security.ErrUnavailable
	}
	return store, nil
}
func (s *Service) queryBinding(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeBinding, error) {
	store, err := s.queryStore()
	if err != nil {
		return durable.QueryRuntimeBinding{}, err
	}
	after := ""
	for ctx.Err() == nil {
		page, readErr := store.ListQueryRuntimes(ctx, durable.QueryRuntimeList{NamespaceTarget: target.NamespaceTarget, BuildID: target.BuildID, After: after, Limit: durable.MaxReadPage})
		if readErr != nil {
			return durable.QueryRuntimeBinding{}, commandError(readErr)
		}
		for _, binding := range page.Items {
			if binding.RuntimeID == target.RuntimeID {
				if binding.QueryRuntimeTarget != target || binding.Validate() != nil {
					return durable.QueryRuntimeBinding{}, security.ErrUnavailable
				}
				return binding, nil
			}
		}
		if page.Next == "" {
			return durable.QueryRuntimeBinding{}, durable.ErrNotFound
		}
		if page.Next <= after {
			return durable.QueryRuntimeBinding{}, security.ErrUnavailable
		}
		after = page.Next
	}
	return durable.QueryRuntimeBinding{}, security.ErrUnavailable
}
func (s *Service) authorizeQuery(ctx context.Context, p security.Principal, action string, identity durable.QueryRuntimeIdentity) error {
	if identity.Validate() != nil || identity.InstallationID != s.installation {
		return security.ErrUnavailable
	}
	return s.checkResourceFacts(ctx, p, action, durable.Key{Namespace: identity.Namespace}, "", identity.BuildID, identity.RuntimeID, identity.InstanceID, "")
}

func (s *Service) queryFailure(ctx context.Context, p security.Principal, action string, target durable.QueryRuntimeTarget, cause error) error {
	if err := s.checkResourceFacts(ctx, p, action, durable.Key{Namespace: target.Namespace}, "", target.BuildID, target.RuntimeID, "", ""); err != nil {
		return err
	}
	return commandError(cause)
}
func (s *Service) queryLookup(ctx context.Context, p security.Principal, action string, target durable.QueryRuntimeTarget, operation durable.LifecycleOperation, requestID, digest string) (durable.LifecycleReceipt, error) {
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: operation, RequestID: requestID, CommandDigest: digest}
	return s.lookupQueryReceipt(ctx, p, action, target, lookup)
}

func (s *Service) lookupQueryReceipt(ctx context.Context, p security.Principal, action string, target durable.QueryRuntimeTarget, lookup durable.LifecycleReceiptLookup) (durable.LifecycleReceipt, error) {
	life, err := s.lifecycleStore()
	if err != nil {
		return durable.LifecycleReceipt{}, err
	}
	receipt, err := life.LookupLifecycleReceipt(ctx, lookup)
	if err == nil {
		if receipt.Match(lookup) != nil || receipt.QueryRuntime == nil || receipt.QueryRuntime.QueryRuntimeTarget != target {
			return durable.LifecycleReceipt{}, security.ErrUnavailable
		}
		if authErr := s.authorizeQuery(ctx, p, action, receipt.QueryRuntime.QueryRuntimeIdentity); authErr != nil {
			return durable.LifecycleReceipt{}, authErr
		}
		return receipt, nil
	}
	if errors.Is(err, durable.ErrNotFound) {
		return durable.LifecycleReceipt{}, err
	}
	if authErr := s.checkResourceFacts(ctx, p, action, durable.Key{Namespace: target.Namespace}, "", target.BuildID, target.RuntimeID, "", ""); authErr != nil {
		return durable.LifecycleReceipt{}, authErr
	}
	return durable.LifecycleReceipt{}, commandError(err)
}

type QueryRuntime struct {
	QueryRuntimeInput
	InstanceID         string    `json:"instance_id"`
	IdentityVersion    string    `json:"identity_version"`
	State              string    `json:"state"`
	Version            string    `json:"version"`
	RemovalEpoch       string    `json:"removal_epoch"`
	ProofID            string    `json:"proof_id,omitempty"`
	ProbePolicyID      string    `json:"probe_policy_id"`
	ProbePolicyVersion string    `json:"probe_policy_version"`
	VerifiedAt         time.Time `json:"verified_at"`
	ValidUntil         time.Time `json:"valid_until"`
}

func queryRuntime(b durable.QueryRuntimeBinding) QueryRuntime {
	return QueryRuntime{QueryRuntimeInput: QueryRuntimeInput{BuildInput: BuildInput{Namespace: b.Namespace, BuildID: b.BuildID}, RuntimeID: b.RuntimeID}, InstanceID: b.InstanceID, IdentityVersion: strconv.FormatInt(b.IdentityVersion, 10), State: b.State, Version: strconv.FormatInt(b.Version, 10), RemovalEpoch: strconv.FormatInt(b.RemovalEpoch, 10), ProofID: b.Verification.ProofID, ProbePolicyID: b.BuildIdentity.ProbePolicyID, ProbePolicyVersion: strconv.FormatInt(b.BuildIdentity.ProbePolicyVersion, 10), VerifiedAt: b.Verification.VerifiedAt, ValidUntil: b.Verification.ValidUntil}
}

type QueryRuntimeAcceptance struct {
	LifecycleAcceptance
	Binding QueryRuntime `json:"binding"`
}

func queryAcceptance(r durable.LifecycleReceipt) QueryRuntimeAcceptance {
	out := QueryRuntimeAcceptance{LifecycleAcceptance: lifecycleAcceptance(r)}
	if r.QueryRuntime != nil {
		out.Binding = queryRuntime(*r.QueryRuntime)
	}
	return out
}

func checkedQueryReceipt(r durable.LifecycleReceipt, err error, target durable.QueryRuntimeTarget, operation durable.LifecycleOperation, id string, request any) (durable.LifecycleReceipt, error) {
	if err != nil {
		return durable.LifecycleReceipt{}, commandError(err)
	}
	digest, digestErr := durable.Fingerprint(string(operation), request)
	lookup := durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: operation, RequestID: id, RequestDigest: digest}
	if digestErr != nil || r.Match(lookup) != nil || r.QueryRuntime == nil || r.QueryRuntime.QueryRuntimeTarget != target {
		return durable.LifecycleReceipt{}, security.ErrUnavailable
	}
	return r, nil
}
func (s *Service) QueryRuntime(ctx context.Context, p security.Principal, in QueryRuntimeInput) (QueryRuntime, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.queryTarget(in)
	if err != nil {
		return QueryRuntime{}, err
	}
	binding, err := s.queryBinding(ctx, target)
	if err != nil {
		if authErr := s.checkResourceFacts(ctx, p, ReadQueryRuntime, durable.Key{Namespace: target.Namespace}, "", target.BuildID, target.RuntimeID, "", ""); authErr != nil {
			return QueryRuntime{}, authErr
		}
		return QueryRuntime{}, err
	}
	if authErr := s.authorizeQuery(ctx, p, ReadQueryRuntime, binding.QueryRuntimeIdentity); authErr != nil {
		return QueryRuntime{}, authErr
	}
	if err = s.audit.RecordDurableRead(ctx, p, ReadQueryRuntime, "allowed", target.Namespace); err != nil {
		return QueryRuntime{}, security.ErrUnavailable
	}
	return queryRuntime(binding), nil
}
func (s *Service) RegisterQueryRuntime(ctx context.Context, p security.Principal, in QueryRuntimeCommand) (QueryRuntimeAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.queryTarget(in.QueryRuntimeInput)
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	if !durable.DeliveryIdentifier(in.RequestID) || in.ExpectedVersion != "0" {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	if p.Validate() != nil {
		return QueryRuntimeAcceptance{}, s.denied(ctx, p, RegisterQueryRuntime, security.ErrUnauthenticated)
	}
	digest, err := durable.Fingerprint("operator.query_runtime.register.v1", in)
	if err != nil {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	saved, err := s.queryLookup(ctx, p, RegisterQueryRuntime, target, durable.OperationRegisterQueryRuntime, in.RequestID, digest)
	if !errors.Is(err, durable.ErrNotFound) {
		return queryAcceptance(saved), err
	}
	if s.queryHost == nil {
		return QueryRuntimeAcceptance{}, s.queryFailure(ctx, p, RegisterQueryRuntime, target, ErrRuntimeUnavailable)
	}
	identity, err := s.queryHost.ResolveBinding(ctx, target)
	if err != nil {
		return QueryRuntimeAcceptance{}, s.queryFailure(ctx, p, RegisterQueryRuntime, target, err)
	}
	if identity.QueryRuntimeTarget != target || identity.Validate() != nil {
		return QueryRuntimeAcceptance{}, security.ErrUnavailable
	}
	if authErr := s.authorizeQuery(ctx, p, RegisterQueryRuntime, identity); authErr != nil {
		return QueryRuntimeAcceptance{}, authErr
	}
	store, err := s.queryStore()
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	request := durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: in.RequestID, CommandDigest: digest}
	if s.queryRegistrationPolicy != nil {
		if policyErr := s.queryRegistrationPolicy(ctx, request); policyErr != nil {
			return QueryRuntimeAcceptance{}, commandError(policyErr)
		}
	}
	receipt, err := store.RegisterQueryRuntime(commandContext(ctx, p, in.RequestID), request)
	receipt, err = checkedQueryReceipt(receipt, err, target, durable.OperationRegisterQueryRuntime, in.RequestID, request)
	return queryAcceptance(receipt), commandError(err)
}
func (s *Service) VerifyQueryRuntime(ctx context.Context, p security.Principal, in QueryRuntimeCommand) (QueryRuntimeAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.queryTarget(in.QueryRuntimeInput)
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	version, err := lifecycleVersion(in.ExpectedVersion, false)
	if err != nil || !durable.DeliveryIdentifier(in.RequestID) {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	if p.Validate() != nil {
		return QueryRuntimeAcceptance{}, s.denied(ctx, p, VerifyQueryRuntime, security.ErrUnauthenticated)
	}
	digest, err := durable.Fingerprint("operator.query_runtime.verify.v1", in)
	if err != nil {
		return QueryRuntimeAcceptance{}, durable.ErrInvalid
	}
	saved, err := s.queryLookup(ctx, p, VerifyQueryRuntime, target, durable.OperationVerifyQueryRuntime, in.RequestID, digest)
	if !errors.Is(err, durable.ErrNotFound) {
		return queryAcceptance(saved), err
	}
	binding, err := s.queryBinding(ctx, target)
	if err != nil {
		return QueryRuntimeAcceptance{}, s.queryFailure(ctx, p, VerifyQueryRuntime, target, err)
	}
	if authErr := s.authorizeQuery(ctx, p, VerifyQueryRuntime, binding.QueryRuntimeIdentity); authErr != nil {
		return QueryRuntimeAcceptance{}, authErr
	}
	if s.queryHost == nil {
		return QueryRuntimeAcceptance{}, ErrRuntimeUnavailable
	}
	proof, err := s.queryHost.Verify(ctx, binding)
	if err != nil {
		s.observeQueryRejection(ctx, target, in.RequestID, durable.OperationVerifyQueryRuntime, err)
		return QueryRuntimeAcceptance{}, commandError(err)
	}
	store, err := s.queryStore()
	if err != nil {
		return QueryRuntimeAcceptance{}, err
	}
	request := durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: target, RequestID: in.RequestID, ExpectedVersion: version, CommandDigest: digest, Verification: proof}
	receipt, err := store.RecordQueryRuntimeVerification(commandContext(ctx, p, in.RequestID), request)
	s.observeQueryRejection(ctx, target, in.RequestID, durable.OperationVerifyQueryRuntime, err)
	receipt, err = checkedQueryReceipt(receipt, err, target, durable.OperationVerifyQueryRuntime, in.RequestID, request)
	return queryAcceptance(receipt), commandError(err)
}
