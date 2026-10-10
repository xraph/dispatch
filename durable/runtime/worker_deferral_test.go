package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

type deferralLossStore struct {
	*memory.Store
	mu           sync.Mutex
	requests     []durable.WorkflowTaskDeferralRequest
	renewals     atomic.Int64
	entered      chan struct{}
	release      chan struct{}
	never        bool
	afterUnknown error
}

func (s *deferralLossStore) RenewTask(ctx context.Context, k durable.Key, token durable.TaskToken, d time.Duration) (time.Time, error) {
	s.renewals.Add(1)
	return s.Store.RenewTask(ctx, k, token, d)
}
func (s *deferralLossStore) DeferWorkflowTask(ctx context.Context, r durable.WorkflowTaskDeferralRequest) (durable.WorkflowTaskDeferralReceipt, error) {
	s.mu.Lock()
	s.requests = append(s.requests, r)
	call := len(s.requests)
	s.mu.Unlock()
	if call > 1 && s.afterUnknown != nil {
		return durable.WorkflowTaskDeferralReceipt{}, s.afterUnknown
	}
	if s.never {
		return durable.WorkflowTaskDeferralReceipt{}, errors.New("injected unavailable")
	}
	if call == 1 {
		if _, err := s.Store.DeferWorkflowTask(ctx, r); err != nil {
			return durable.WorkflowTaskDeferralReceipt{}, err
		}
		return durable.WorkflowTaskDeferralReceipt{}, errors.New("injected lost response")
	}
	if call == 2 {
		close(s.entered)
		<-s.release
	}
	return s.Store.DeferWorkflowTask(ctx, r)
}
func deferralWorker(t *testing.T, s *deferralLossStore) (*drt.Worker, durable.Key) {
	t.Helper()
	o := workerOptions(t)
	o.LeaseDuration = 90 * time.Millisecond
	o.StoreTimeout = 30 * time.Millisecond
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{BuildID: "missing", Queue: "children"}).Get()
	}
	if _, err := s.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: o.Namespace, AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}); err != nil {
		t.Fatal(err)
	}
	w := newWorker(t, s, o)
	key := startWorkerRun(t, w, o)
	if _, err := s.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: o.Namespace}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	return w, key
}
func TestWorkflowDeferralLostResponseSuspendsRenewalAndDrains(t *testing.T) {
	s := &deferralLossStore{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	w, key := deferralWorker(t, s)
	finished := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), durable.TaskWorkflow); finished <- err }()
	awaitDrainSignal(t, s.entered)
	handle, err := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	observer, cancel := context.WithTimeout(t.Context(), 120*time.Millisecond)
	_, err = w.WaitDrain(observer, handle)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("observer: %v", err)
	}
	if s.renewals.Load() != 0 {
		t.Fatalf("renewed uncertain transfer %d times", s.renewals.Load())
	}
	close(s.release)
	if err = <-finished; err != nil {
		t.Fatal(err)
	}
	outcome, err := w.WaitDrain(t.Context(), handle)
	if err != nil || !outcome.Complete {
		t.Fatalf("drain: %+v %v", outcome, err)
	}
	s.mu.Lock()
	requests := append([]durable.WorkflowTaskDeferralRequest(nil), s.requests...)
	s.mu.Unlock()
	if len(requests) != 3 || !reflect.DeepEqual(requests[0], requests[1]) || !reflect.DeepEqual(requests[0], requests[2]) {
		t.Fatalf("reconciliation changed request: %+v", requests)
	}
	e, err := s.GetExecution(t.Context(), key)
	if err != nil || e.Revision != 1 || e.LastSequence != 1 {
		t.Fatalf("history changed: %+v %v", e, err)
	}
	d, err := s.GetWorkflowTaskDeferral(t.Context(), key, requests[0].Token.TaskID)
	if err != nil || !d.Active || d.DeferralCount != 1 {
		t.Fatalf("deferral: %+v %v", d, err)
	}
}
func TestWorkflowDeferralUnresolvedOutcomeCannotCertifyDrain(t *testing.T) {
	s := &deferralLossStore{Store: memory.New(), never: true}
	w, _ := deferralWorker(t, s)
	if _, err := w.RunOnce(t.Context(), durable.TaskWorkflow); !errors.Is(err, drt.ErrDeferralUnknown) {
		t.Fatalf("unknown: %v", err)
	}
	handle, err := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := w.WaitDrain(t.Context(), handle)
	if !errors.Is(err, drt.ErrDrainIncomplete) || outcome.Complete {
		t.Fatalf("unknown certified: %+v %v", outcome, err)
	}
	if s.renewals.Load() != 0 || len(s.requests) != 3 {
		t.Fatalf("unbounded reconciliation/renewal: %d/%d", len(s.requests), s.renewals.Load())
	}
}

func TestWorkflowDeferralDrainDeadlinePreservesUnknownOutcome(t *testing.T) {
	s := &deferralLossStore{Store: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	w, _ := deferralWorker(t, s)
	finished := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), durable.TaskWorkflow); finished <- err }()
	awaitDrainSignal(t, s.entered)
	handle, err := w.BeginDrain(t.Context(), drt.DrainRequest{OperationID: "deadline", Deadline: time.Now().Add(40 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := w.WaitDrain(t.Context(), handle)
	if !errors.Is(err, drt.ErrDrainIncomplete) || outcome.Complete || outcome.Quiescent {
		t.Fatalf("deadline abandoned live call: %+v %v", outcome, err)
	}
	close(s.release)
	if err = <-finished; err == nil {
		t.Fatal("unknown canceled transfer reported success")
	}
	outcome, err = w.WaitDrain(t.Context(), handle)
	if !errors.Is(err, drt.ErrDrainIncomplete) || outcome.Complete || !outcome.Quiescent {
		t.Fatalf("deadline result changed: %+v %v", outcome, err)
	}
	if s.renewals.Load() != 0 {
		t.Fatal("renewal restarted after unknown cancellation")
	}
}

type registrationDuringDeferralStore struct {
	*memory.Store
	calls int
}

func (s *registrationDuringDeferralStore) DeferWorkflowTask(ctx context.Context, r durable.WorkflowTaskDeferralRequest) (durable.WorkflowTaskDeferralReceipt, error) {
	s.calls++
	if _, err := s.RegisterBuild(ctx, durable.RegisterBuildRequest{BuildTarget: durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: r.Namespace}, BuildID: r.TargetBuildID}, RequestID: "register-during-deferral"}); err != nil {
		return durable.WorkflowTaskDeferralReceipt{}, err
	}
	return s.Store.DeferWorkflowTask(ctx, r)
}
func TestWorkflowDeferralAdmissionChangedReevaluatesWithOwnedToken(t *testing.T) {
	base := memory.New()
	store := &registrationDuringDeferralStore{Store: base}
	o := workerOptions(t)
	o.Workflows["order"] = func(w *drt.Workflow, _ []byte) ([]byte, error) {
		return w.ChildWorkflow("child", "child", nil, drt.ChildOptions{BuildID: "new", Queue: "children"}).Get()
	}
	if _, err := base.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: o.Namespace, AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}); err != nil {
		t.Fatal(err)
	}
	w := newWorker(t, store, o)
	key := startWorkerRun(t, w, o)
	if _, err := base.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: o.Namespace}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	runTask(t, w, durable.TaskWorkflow)
	e, err := base.GetExecution(t.Context(), key)
	if err != nil || e.Revision != 2 || store.calls != 1 {
		t.Fatalf("did not reevaluate after registration: %+v %v calls=%d", e, err, store.calls)
	}
}

func TestWorkflowDeferralRefusalAfterUnknownDoesNotResumeRenewal(t *testing.T) {
	for _, refusal := range []error{durable.ErrLifecycleBusy, durable.ErrWriterCompatibility, durable.ErrInvalid, durable.ErrNotFound} {
		t.Run(refusal.Error(), func(t *testing.T) {
			s := &deferralLossStore{Store: memory.New(), afterUnknown: refusal}
			w, key := deferralWorker(t, s)
			if _, err := w.RunOnce(t.Context(), durable.TaskWorkflow); !errors.Is(err, drt.ErrDeferralUnknown) {
				t.Fatalf("unknown acceptance downgraded: %v", err)
			}
			d, err := s.GetWorkflowTaskDeferral(t.Context(), key, "workflow:1")
			if err != nil || !d.Active || s.renewals.Load() != 0 || len(s.requests) != 3 {
				t.Fatalf("accepted deferral lost: %+v %v renewals=%d calls=%d", d, err, s.renewals.Load(), len(s.requests))
			}
		})
	}
}
