package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type readinessStore struct {
	*memory.Store
	inspections, claims atomic.Int64
	failure             error
	waitForContext      bool
	alter               func(durable.CompatibilityFacts) durable.CompatibilityFacts
}

func (s *readinessStore) InspectCompatibility(ctx context.Context, target durable.NamespaceTarget) (durable.CompatibilityFacts, error) {
	s.inspections.Add(1)
	if s.waitForContext {
		<-ctx.Done()
		return durable.CompatibilityFacts{}, ctx.Err()
	}
	if s.failure != nil {
		return durable.CompatibilityFacts{}, s.failure
	}
	f, err := s.Store.InspectCompatibility(ctx, target)
	if s.alter != nil {
		f = s.alter(f)
	}
	return f, err
}
func (s *readinessStore) ClaimTask(context.Context, durable.ClaimRequest) (*durable.Task, error) {
	s.claims.Add(1)
	return nil, nil
}
func (s *readinessStore) ClaimTimeoutTask(context.Context, durable.TimeoutClaimRequest) (*durable.Task, error) {
	s.claims.Add(1)
	return nil, nil
}
func (s *readinessStore) ClaimChildDelivery(context.Context, durable.ChildDeliveryClaimRequest) (*durable.ChildDelivery, error) {
	s.claims.Add(1)
	return nil, nil
}
func (s *readinessStore) ClaimExecutionTimeout(context.Context, durable.ExecutionTimeoutClaimRequest) (*durable.ExecutionTimeoutTask, error) {
	s.claims.Add(1)
	return nil, nil
}

type readinessBaseOnly struct{ durable.Store }

func readinessFixture(t *testing.T) (*readinessStore, *drt.Worker, drt.Options) {
	t.Helper()
	s := &readinessStore{Store: memory.New()}
	if _, err := s.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: "n", AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	options := drt.Options{Namespace: "n", Queue: "q", BuildID: "b", Owner: "o", Retirement: &drt.RetirementOptions{InstallationID: "i", WriterProtocol: 1}}
	w, err := drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	return s, w, options
}
func TestRetirementReadinessGatesAllClaimPaths(t *testing.T) {
	s, w, _ := readinessFixture(t)
	kinds := []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskTimer, drt.TaskTimeout, drt.TaskChildDelivery, drt.TaskExecutionTimeout}
	for _, kind := range kinds {
		if worked, err := w.RunOnce(t.Context(), kind); worked || !errors.Is(err, durable.ErrWriterCompatibility) {
			t.Fatalf("unenrolled %s: %v %v", kind, worked, err)
		}
	}
	if s.claims.Load() != 0 || s.inspections.Load() != 6 {
		t.Fatalf("claim bypass: %d %d", s.claims.Load(), s.inspections.Load())
	}
	r, err := w.Readiness(t.Context())
	if !errors.Is(err, durable.ErrWriterCompatibility) || r.Ready || r.RetirementStatus != "unenrolled" || r.Worker.State != drt.WorkerNotStarted {
		t.Fatalf("readiness %+v %v", r, err)
	}
	target := durable.NamespaceTarget{InstallationID: "i", Namespace: "n"}
	if _, err = s.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	r, err = w.Readiness(t.Context())
	if err != nil || r.Ready || r.RetirementStatus != "ready" || !r.QueryRuntimeCapability || r.Compatibility.QueryRetentionSchemaVersion != 1 {
		t.Fatalf("enrolled not started: %+v %v", r, err)
	}
	before := s.inspections.Load()
	for _, kind := range kinds {
		if _, err = w.RunOnce(t.Context(), kind); err != nil {
			t.Fatalf("enrolled %s: %v", kind, err)
		}
	}
	if s.claims.Load() != 6 || s.inspections.Load()-before != 6 {
		t.Fatalf("claim check missing: %d %d", s.claims.Load(), s.inspections.Load()-before)
	}
	for _, alter := range []func(durable.CompatibilityFacts) durable.CompatibilityFacts{
		func(f durable.CompatibilityFacts) durable.CompatibilityFacts { f.WriterProtocol++; return f },
		func(f durable.CompatibilityFacts) durable.CompatibilityFacts { f.SchemaVersion++; return f },
		func(f durable.CompatibilityFacts) durable.CompatibilityFacts {
			f.QueryRetentionSchemaVersion = 0
			return f
		},
		func(f durable.CompatibilityFacts) durable.CompatibilityFacts {
			f.WorkerDrainSchemaVersion = 0
			return f
		},
	} {
		s.alter = alter
		for _, kind := range kinds {
			if _, err = w.RunOnce(t.Context(), kind); !errors.Is(err, durable.ErrWriterCompatibility) {
				t.Fatalf("incompatible %s: %v", kind, err)
			}
		}
		r, err = w.Readiness(t.Context())
		if r.RetirementStatus != "incompatible" || !errors.Is(err, durable.ErrWriterCompatibility) {
			t.Fatalf("floor readiness: %+v %v", r, err)
		}
	}
	s.alter = nil
	s.failure = errors.New("private storage diagnostic")
	r, err = w.Readiness(t.Context())
	if err == nil || r.RetirementStatus != "unavailable" || r.Ready {
		t.Fatalf("unavailable readiness: %+v %v", r, err)
	}
	if s.claims.Load() != 6 {
		t.Fatal("incompatible writer reached claims")
	}
}
func TestRetirementConfigurationCapturesInputsAndOptionalCapabilities(t *testing.T) {
	s, w, options := readinessFixture(t)
	options.Retirement.InstallationID = "changed"
	options.Retirement.WriterProtocol = 9
	r, err := w.Readiness(t.Context())
	if !errors.Is(err, durable.ErrWriterCompatibility) || r.Compatibility.InstallationID != "i" {
		t.Fatalf("retained caller pointer: %+v %v", r, err)
	}
	options.Retirement = &drt.RetirementOptions{InstallationID: "i", WriterProtocol: 1}
	if _, err = drt.NewWorker(&readinessBaseOnly{Store: s}, options); !errors.Is(err, durable.ErrWriterCompatibility) {
		t.Fatalf("missing capability accepted: %v", err)
	}
	options.Retirement = nil
	ordinary, err := drt.NewWorker(&readinessBaseOnly{Store: s}, options)
	if err != nil {
		t.Fatal(err)
	}
	r, err = ordinary.Readiness(t.Context())
	if err != nil || r.RetirementEnabled || r.RetirementStatus != "unenrolled" {
		t.Fatalf("ordinary worker: %+v %v", r, err)
	}
	if _, err = ordinary.RunOnce(t.Context(), durable.TaskWorkflow); err != nil {
		t.Fatal(err)
	}
}
func TestRetirementRunFailsBeforeClaimsAndDrainRemainsAuthoritative(t *testing.T) {
	s, w, _ := readinessFixture(t)
	if err := w.Run(t.Context()); !errors.Is(err, durable.ErrWriterCompatibility) {
		t.Fatalf("unenrolled Run: %v", err)
	}
	if status := w.Status(); status.State != drt.WorkerFailed || status.Ready || status.InFlight != 0 || s.claims.Load() != 0 {
		t.Fatalf("failed readiness: %+v", status)
	}
	_, other, options := readinessFixture(t)
	handle, err := other.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "d", Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.RunOnce(t.Context(), durable.TaskWorkflow); !errors.Is(err, drt.ErrWorkerDraining) {
		t.Fatal(err)
	}
	if result, waitErr := other.WaitDrain(t.Context(), handle); waitErr != nil || !result.Complete {
		t.Fatalf("prestart drain %+v %v", result, waitErr)
	}
	options.Retirement.WriterProtocol = 0
	if _, err = drt.NewWorker(s, options); !errors.Is(err, durable.ErrWriterCompatibility) {
		t.Fatalf("older writer accepted: %v", err)
	}
}

func TestRetirementReadinessBoundsStorageObservation(t *testing.T) {
	s, _, options := readinessFixture(t)
	s.waitForContext = true
	options.StoreTimeout = 10 * time.Millisecond
	w, err := drt.NewWorker(s, options)
	if err != nil {
		t.Fatal(err)
	}
	if worked, runErr := w.RunOnce(t.Context(), durable.TaskWorkflow); worked || !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("readiness deadline: %v %v", worked, runErr)
	}
	if status := w.Status(); status.InFlight != 0 || status.UnknownClaims != 0 || s.claims.Load() != 0 {
		t.Fatalf("readiness observation became claim: %+v", status)
	}
	r, err := w.Readiness(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) || r.Ready || r.RetirementStatus != "unavailable" {
		t.Fatalf("bounded readiness: %+v %v", r, err)
	}
}
