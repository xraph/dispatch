package operator

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

// LifecycleHost keeps deployment evidence and process control inside the host.
// Its resolvers must bind actual immutable resources, never caller-supplied proof.
type LifecycleHost struct {
	QueryRuntime            QueryRuntimeHost
	QueryRegistrationPolicy QueryRegistrationPolicy
	BuildIdentity           func(context.Context, durable.BuildTarget) (durable.BuildQueryIdentity, error)
	WorkerControl           func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error)
}

const (
	ReadBuild        = "dispatch.build.read"
	EnrollRetirement = "dispatch.retirement.enroll"
	RegisterBuild    = "dispatch.build.register"
	RetireBuild      = "dispatch.build.retire"
	FinalizeBuild    = "dispatch.build.finalize"
	ResumeBuild      = "dispatch.build.resume"
	ReadWorker       = "dispatch.worker.read"
	DrainWorker      = "dispatch.worker.drain"
)

func lifecycleAction(action string) bool {
	if queryLifecycleAction(action) {
		return true
	}
	switch action {
	case ReadBuild, EnrollRetirement, RegisterBuild, RetireBuild, FinalizeBuild, ResumeBuild, ReadWorker, DrainWorker:
		return true
	}
	return false
}

type BuildInput struct {
	Namespace string `json:"namespace"`
	BuildID   string `json:"build_id"`
}

func (s *Service) buildTarget(in BuildInput) (durable.BuildTarget, error) {
	target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: s.installation, Namespace: in.Namespace}, BuildID: in.BuildID}
	return target, target.Validate()
}
func lifecycleVersion(text string, zero bool) (int64, error) {
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < 0 || (!zero && n == 0) || strconv.FormatInt(n, 10) != text {
		return 0, durable.ErrInvalid
	}
	return n, nil
}
func (s *Service) lifecycleStore() (durable.LifecycleStore, error) {
	store, ok := s.store.(durable.LifecycleStore)
	if !ok {
		return nil, security.ErrUnavailable
	}
	return store, nil
}
func (s *Service) authorizeBuild(ctx context.Context, p security.Principal, action string, target durable.BuildTarget, existing bool) (durable.BuildLifecycleFacts, error) {
	if target.Validate() != nil || target.InstallationID != s.installation {
		return durable.BuildLifecycleFacts{}, durable.ErrInvalid
	}
	if err := s.checkFacts(ctx, p, action, durable.Key{Namespace: target.Namespace}, "", target.BuildID); err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	if !existing {
		return durable.BuildLifecycleFacts{}, nil
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return durable.BuildLifecycleFacts{}, err
	}
	facts, err := life.InspectBuildLifecycle(ctx, target)
	if err != nil {
		return facts, commandError(err)
	}
	if facts.Admission.BuildTarget != target {
		return durable.BuildLifecycleFacts{}, security.ErrUnavailable
	}
	return facts, nil
}

type Compatibility struct {
	Namespace          string    `json:"namespace"`
	Enrolled           bool      `json:"enrolled"`
	SchemaVersion      string    `json:"schema_version"`
	WriterProtocol     string    `json:"writer_protocol"`
	QuerySchemaVersion string    `json:"query_schema_version"`
	DrainSchemaVersion string    `json:"drain_schema_version"`
	Version            string    `json:"version"`
	ObservedAt         time.Time `json:"observed_at"`
}

func compatibility(f durable.CompatibilityFacts) Compatibility {
	return Compatibility{Namespace: f.Namespace, Enrolled: f.Enrolled, SchemaVersion: strconv.Itoa(f.SchemaVersion), WriterProtocol: strconv.Itoa(f.WriterProtocol), QuerySchemaVersion: strconv.Itoa(f.QueryRetentionSchemaVersion), DrainSchemaVersion: strconv.Itoa(f.WorkerDrainSchemaVersion), Version: strconv.FormatInt(f.Version, 10), ObservedAt: f.ObservedAt}
}

type NamespaceLifecycleInput struct {
	Namespace string `json:"namespace"`
}

func (s *Service) Compatibility(ctx context.Context, p security.Principal, in NamespaceLifecycleInput) (Compatibility, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if err := s.check(ctx, p, ReadBuild, durable.Key{Namespace: in.Namespace}); err != nil {
		return Compatibility{}, err
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return Compatibility{}, err
	}
	facts, err := life.InspectCompatibility(ctx, durable.NamespaceTarget{InstallationID: s.installation, Namespace: in.Namespace})
	if err != nil {
		return Compatibility{}, commandError(err)
	}
	if facts.InstallationID != s.installation || facts.Namespace != in.Namespace {
		return Compatibility{}, security.ErrUnavailable
	}
	if err = s.audit.RecordDurableRead(ctx, p, ReadBuild, "allowed", in.Namespace); err != nil {
		return Compatibility{}, security.ErrUnavailable
	}
	return compatibility(facts), nil
}

type BuildLifecycle struct {
	BuildInput
	State                 string            `json:"state"`
	Epoch                 string            `json:"epoch"`
	Version               string            `json:"version"`
	CompatibilityVersion  string            `json:"compatibility_version"`
	ObservedAt            time.Time         `json:"observed_at"`
	HasBlockers           bool              `json:"has_blockers"`
	Blockers              map[string]string `json:"blockers"`
	RetainedExecutions    string            `json:"retained_executions"`
	VerifiedQueryRuntimes string            `json:"verified_query_runtimes"`
}

func buildLifecycle(f durable.BuildLifecycleFacts) BuildLifecycle {
	b := f.Blockers
	return BuildLifecycle{BuildInput: BuildInput{Namespace: f.Admission.Namespace, BuildID: f.Admission.BuildID}, State: f.Admission.State, Epoch: strconv.FormatInt(f.Admission.Epoch, 10), Version: strconv.FormatInt(f.Admission.Version, 10), CompatibilityVersion: strconv.FormatInt(f.ObservationVersion.CompatibilityVersion, 10), ObservedAt: f.ObservationVersion.ObservedAt, HasBlockers: !b.Empty(), Blockers: map[string]string{"open_executions": strconv.FormatInt(b.OpenExecutions, 10), "pending_tasks": strconv.FormatInt(b.PendingTasks, 10), "async_callbacks": strconv.FormatInt(b.AsyncCallbacks, 10), "delayed_runs": strconv.FormatInt(b.DelayedRuns, 10), "pending_child_deliveries": strconv.FormatInt(b.PendingChildDeliveries, 10), "child_obligations": strconv.FormatInt(b.ChildObligations, 10)}, RetainedExecutions: strconv.FormatInt(f.QueryRetention.RetainedExecutions, 10), VerifiedQueryRuntimes: strconv.FormatInt(f.QueryRetention.VerifiedBindings, 10)}
}
func (s *Service) BuildLifecycle(ctx context.Context, p security.Principal, in BuildInput) (BuildLifecycle, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.buildTarget(in)
	if err != nil {
		return BuildLifecycle{}, err
	}
	facts, err := s.authorizeBuild(ctx, p, ReadBuild, target, true)
	if err != nil {
		return BuildLifecycle{}, err
	}
	if err = s.audit.RecordDurableRead(ctx, p, ReadBuild, "allowed", in.Namespace); err != nil {
		return BuildLifecycle{}, security.ErrUnavailable
	}
	return buildLifecycle(facts), nil
}

type LifecycleAcceptance struct {
	Namespace  string    `json:"namespace"`
	RequestID  string    `json:"request_id"`
	Operation  string    `json:"operation"`
	Status     string    `json:"status"`
	AcceptedAt time.Time `json:"accepted_at"`
	BuildID    string    `json:"build_id,omitempty"`
	State      string    `json:"state,omitempty"`
	Version    string    `json:"version,omitempty"`
	Epoch      string    `json:"epoch,omitempty"`
}

func lifecycleAcceptance(r durable.LifecycleReceipt) LifecycleAcceptance {
	out := LifecycleAcceptance{Namespace: r.Namespace, RequestID: r.RequestID, Operation: string(r.Operation), Status: "accepted", AcceptedAt: r.AcceptedAt}
	if r.Build != nil {
		out.BuildID = r.Build.BuildID
		out.State = r.Build.State
		out.Version = strconv.FormatInt(r.Build.Version, 10)
		out.Epoch = strconv.FormatInt(r.Build.Epoch, 10)
	}
	return out
}

type EnrollmentInput struct {
	NamespaceLifecycleInput
	RequestID string `json:"request_id"`
}

func (s *Service) EnrollRetirement(ctx context.Context, p security.Principal, in EnrollmentInput) (LifecycleAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	if err := s.check(ctx, p, EnrollRetirement, durable.Key{Namespace: in.Namespace}); err != nil {
		return LifecycleAcceptance{}, err
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	r, err := life.EnrollRetirement(commandContext(ctx, p, in.RequestID), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: s.installation, Namespace: in.Namespace}, RequestID: in.RequestID, SchemaVersion: durable.RetirementSchemaVersion, WriterProtocol: durable.RetirementWriterProtocol})
	return lifecycleAcceptance(r), commandError(err)
}

type RegisterBuildInput struct {
	BuildInput
	RequestID       string `json:"request_id"`
	ExpectedVersion string `json:"expected_version"`
}

// RegisterBuild captures configured host identity only after accepted-command recovery.
func (s *Service) RegisterBuild(ctx context.Context, p security.Principal, in RegisterBuildInput) (LifecycleAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.buildTarget(in.BuildInput)
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	version, err := lifecycleVersion(in.ExpectedVersion, true)
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	if !durable.DeliveryIdentifier(in.RequestID) {
		return LifecycleAcceptance{}, durable.ErrInvalid
	}
	if _, err = s.authorizeBuild(ctx, p, RegisterBuild, target, false); err != nil {
		return LifecycleAcceptance{}, err
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	digest, err := durable.Fingerprint("operator.build.register.v1", in)
	if err != nil {
		return LifecycleAcceptance{}, durable.ErrInvalid
	}
	saved, err := life.LookupLifecycleReceipt(ctx, durable.LifecycleReceiptLookup{NamespaceTarget: target.NamespaceTarget, Operation: durable.OperationRegisterBuild, RequestID: in.RequestID, CommandDigest: digest})
	if !errors.Is(err, durable.ErrNotFound) {
		return lifecycleAcceptance(saved), commandError(err)
	}
	request := durable.RegisterBuildRequest{BuildTarget: target, RequestID: in.RequestID, ExpectedVersion: version, CommandDigest: digest}
	if s.buildIdentity != nil {
		identity, resolveErr := s.buildIdentity(ctx, target)
		if resolveErr != nil {
			return LifecycleAcceptance{}, commandError(resolveErr)
		}
		if identity.Validate() != nil {
			return LifecycleAcceptance{}, security.ErrUnavailable
		}
		request.Identity = &identity
	}
	receipt, err := life.RegisterBuild(commandContext(ctx, p, in.RequestID), request)
	return lifecycleAcceptance(receipt), commandError(err)
}

type BuildRetirementInput struct {
	BuildInput
	RequestID       string `json:"request_id"`
	ExpectedVersion string `json:"expected_version"`
	ExpectedEpoch   string `json:"expected_epoch"`
}

func (s *Service) changeBuild(ctx context.Context, p security.Principal, in BuildRetirementInput, action string) (LifecycleAcceptance, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	target, err := s.buildTarget(in.BuildInput)
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	version, err := lifecycleVersion(in.ExpectedVersion, false)
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	epoch, err := lifecycleVersion(in.ExpectedEpoch, false)
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	if _, err = s.authorizeBuild(ctx, p, action, target, true); err != nil {
		return LifecycleAcceptance{}, err
	}
	life, err := s.lifecycleStore()
	if err != nil {
		return LifecycleAcceptance{}, err
	}
	request := durable.BuildRetirementRequest{BuildTarget: target, RequestID: in.RequestID, ExpectedVersion: version, ExpectedEpoch: epoch}
	ctx = commandContext(ctx, p, in.RequestID)
	var receipt durable.LifecycleReceipt
	switch action {
	case RetireBuild:
		receipt, err = life.BeginBuildRetirement(ctx, request)
	case FinalizeBuild:
		receipt, err = life.FinalizeBuildRetirement(ctx, request)
	case ResumeBuild:
		receipt, err = life.AbortBuildRetirement(ctx, request)
	default:
		return LifecycleAcceptance{}, durable.ErrInvalid
	}
	return lifecycleAcceptance(receipt), commandError(err)
}
func (s *Service) RetireBuild(ctx context.Context, p security.Principal, in BuildRetirementInput) (LifecycleAcceptance, error) {
	return s.changeBuild(ctx, p, in, RetireBuild)
}
func (s *Service) FinalizeBuild(ctx context.Context, p security.Principal, in BuildRetirementInput) (LifecycleAcceptance, error) {
	return s.changeBuild(ctx, p, in, FinalizeBuild)
}
func (s *Service) ResumeBuild(ctx context.Context, p security.Principal, in BuildRetirementInput) (LifecycleAcceptance, error) {
	return s.changeBuild(ctx, p, in, ResumeBuild)
}
