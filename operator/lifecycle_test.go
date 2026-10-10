package operator

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

func lifecycleFixture(t *testing.T) (*Service, *memory.Store, *drt.Worker, *map[string]bool) {
	t.Helper()
	s, store, grants := fixture(t)
	if _, err := s.EnrollRetirement(t.Context(), reader(), EnrollmentInput{NamespaceLifecycleInput: NamespaceLifecycleInput{Namespace: "allowed"}, RequestID: "enroll"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterBuild(t.Context(), reader(), RegisterBuildInput{BuildInput: BuildInput{Namespace: "allowed", BuildID: "build"}, RequestID: "register", ExpectedVersion: "0"}); err != nil {
		t.Fatal(err)
	}
	w, err := drt.NewWorker(store, drt.Options{Namespace: "allowed", Queue: "queue", BuildID: "build", Owner: "owner", RuntimeID: "runtime", InstanceID: "instance", Retirement: &drt.RetirementOptions{InstallationID: "install", WriterProtocol: 1}})
	if err != nil {
		t.Fatal(err)
	}
	s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) {
		return LocalWorkerControl{Worker: w}, nil
	}
	return s, store, w, grants
}
func drainInput() WorkerDrainInput {
	return WorkerDrainInput{WorkerInput: WorkerInput{BuildInput: BuildInput{Namespace: "allowed", BuildID: "build"}, RuntimeID: "runtime"}, RequestID: "request", OperationID: "drain", Deadline: durable.Timestamp(time.Now().Add(time.Minute))}
}
func TestLifecycleBuildAuthorizationAndCapturedIdentity(t *testing.T) {
	s, store, grants := fixture(t)
	if _, err := s.EnrollRetirement(t.Context(), reader(), EnrollmentInput{NamespaceLifecycleInput: NamespaceLifecycleInput{Namespace: "allowed"}, RequestID: "enroll"}); err != nil {
		t.Fatal(err)
	}
	probes := 0
	s.buildIdentity = func(context.Context, durable.BuildTarget) (durable.BuildQueryIdentity, error) {
		probes++
		return durabletest.QueryIdentityFixture(), nil
	}
	in := RegisterBuildInput{BuildInput: BuildInput{Namespace: "allowed", BuildID: "build"}, RequestID: "register", ExpectedVersion: "0"}
	accepted, err := s.RegisterBuild(t.Context(), reader(), in)
	if err != nil || accepted.Version != "1" {
		t.Fatalf("registration %+v %v", accepted, err)
	}
	s.buildIdentity = func(context.Context, durable.BuildTarget) (durable.BuildQueryIdentity, error) {
		probes++
		return durable.BuildQueryIdentity{}, errors.New("private host diagnostic")
	}
	if replay, replayErr := s.RegisterBuild(t.Context(), reader(), in); replayErr != nil || !reflect.DeepEqual(replay, accepted) || probes != 1 {
		t.Fatalf("registration recaptured evidence: %+v %v %d", replay, replayErr, probes)
	}
	retire := BuildRetirementInput{BuildInput: in.BuildInput, RequestID: "retire", ExpectedVersion: "1", ExpectedEpoch: "1"}
	retired, err := s.RetireBuild(t.Context(), reader(), retire)
	if err != nil || retired.State != durable.BuildRetiring {
		t.Fatalf("retirement %+v %v", retired, err)
	}
	if replay, replayErr := s.RetireBuild(t.Context(), reader(), retire); replayErr != nil || !reflect.DeepEqual(replay, retired) {
		t.Fatalf("retirement replay %+v %v", replay, replayErr)
	}
	facts, err := s.BuildLifecycle(t.Context(), reader(), in.BuildInput)
	if err != nil || facts.Epoch != "2" || facts.HasBlockers {
		t.Fatalf("facts %+v %v", facts, err)
	}
	(*grants)["allowed"] = false
	if _, err = s.RegisterBuild(t.Context(), reader(), in); !errors.Is(err, security.ErrForbidden) || probes != 1 {
		t.Fatalf("revoked registration replay: %v", err)
	}
	if _, err = s.RetireBuild(t.Context(), reader(), retire); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("revoked retirement replay: %v", err)
	}
	f, err := store.InspectBuildLifecycle(t.Context(), durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "install", Namespace: "allowed"}, BuildID: "build"})
	if err != nil || f.Admission.Version != 2 {
		t.Fatalf("denial mutated build %+v %v", f, err)
	}
}

type lostDrainReceiptStore struct {
	*memory.Store
	lose bool
}

func (s *lostDrainReceiptStore) RequestWorkerDrain(ctx context.Context, r durable.WorkerDrainRequest) (durable.LifecycleReceipt, error) {
	receipt, err := s.Store.RequestWorkerDrain(ctx, r)
	if err == nil && s.lose {
		s.lose = false
		return durable.LifecycleReceipt{}, errors.New("lost acceptance reply")
	}
	return receipt, err
}

type lostDrainControl struct {
	LocalWorkerControl
	lose        bool
	invocations int
}

func (c *lostDrainControl) BeginDrain(ctx context.Context, r drt.DrainRequest) (drt.DrainHandle, error) {
	c.invocations++
	h, err := c.LocalWorkerControl.BeginDrain(ctx, r)
	if err == nil && c.lose {
		c.lose = false
		return drt.DrainHandle{}, errors.New("lost invocation reply")
	}
	return h, err
}
func TestWorkerDrainReceiptBeforeInvocationAndCurrentAuthorization(t *testing.T) {
	s, store, w, grants := lifecycleFixture(t)
	lost := &lostDrainReceiptStore{Store: store, lose: true}
	s.store = lost
	input := drainInput()
	if _, err := s.RequestWorkerDrain(t.Context(), reader(), input); !errors.Is(err, security.ErrUnavailable) {
		t.Fatalf("lost receipt reply: %v", err)
	}
	if w.Status().AdmissionClosed {
		t.Fatal("process invoked without acceptance reply")
	}
	receipt, err := s.WorkerDrainReceipt(t.Context(), reader(), input)
	if err != nil || receipt.Status != "requested" || receipt.Complete || w.Status().AdmissionClosed {
		t.Fatalf("acceptance became completion: %+v %v", receipt, err)
	}
	(*grants)["allowed"] = false
	if _, err = s.RequestWorkerDrain(t.Context(), reader(), input); !errors.Is(err, security.ErrForbidden) || w.Status().AdmissionClosed {
		t.Fatalf("revoked accepted command invoked: %v", err)
	}
	if _, err = s.WorkerDrainReceipt(t.Context(), reader(), input); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("revoked receipt read: %v", err)
	}
	(*grants)["allowed"] = true
	completed, err := s.RequestWorkerDrain(t.Context(), reader(), input)
	if err != nil || !completed.Complete || completed.Process != "complete" || !completed.Quiescent {
		t.Fatalf("recovered drain: %+v %v", completed, err)
	}
	if err = w.Run(t.Context()); !errors.Is(err, drt.ErrWorkerDraining) {
		t.Fatalf("prestart drain allowed later startup: %v", err)
	}
	changed := input
	changed.Deadline = changed.Deadline.Add(time.Minute)
	if _, err = s.RequestWorkerDrain(t.Context(), reader(), changed); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatalf("extended accepted deadline: %v", err)
	}
}
func TestWorkerDrainLostInvocationAndReplacement(t *testing.T) {
	s, _, w, _ := lifecycleFixture(t)
	control := &lostDrainControl{LocalWorkerControl: LocalWorkerControl{Worker: w}, lose: true}
	s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) { return control, nil }
	input := drainInput()
	accepted, err := s.RequestWorkerDrain(t.Context(), reader(), input)
	if err != nil || accepted.Process != "unknown" || accepted.Complete || !w.Status().AdmissionClosed {
		t.Fatalf("lost invocation became completion %+v %v", accepted, err)
	}
	completed, err := s.RequestWorkerDrain(t.Context(), reader(), input)
	if err != nil || !completed.Complete || control.invocations != 2 {
		t.Fatalf("same incarnation replay %+v %v", completed, err)
	}
	if completed.AcceptedAt != accepted.AcceptedAt || completed.Deadline != accepted.Deadline {
		t.Fatal("replay changed accepted operation")
	}
	replacement, createErr := drt.NewWorker(s.store, drt.Options{Namespace: "allowed", Queue: "queue", BuildID: "build", Owner: "owner", RuntimeID: "replacement", InstanceID: "instance"})
	if createErr != nil {
		t.Fatal(createErr)
	}
	s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) {
		return LocalWorkerControl{Worker: replacement}, nil
	}
	recovered, err := s.RequestWorkerDrain(t.Context(), reader(), input)
	if err != nil || recovered.Process != "unknown" || recovered.Complete || replacement.Status().AdmissionClosed {
		t.Fatalf("old request redirected %+v %v", recovered, err)
	}
}
func TestWorkerDrainExpiredAcceptanceNeverInvokes(t *testing.T) {
	s, store, w, _ := lifecycleFixture(t)
	s.store = &lostDrainReceiptStore{Store: store, lose: true}
	in := drainInput()
	in.Deadline = durable.Timestamp(time.Now().Add(30 * time.Millisecond))
	if _, err := s.RequestWorkerDrain(t.Context(), reader(), in); err == nil {
		t.Fatal("expected lost acceptance reply")
	}
	<-time.After(time.Until(in.Deadline) + time.Millisecond)
	result, err := s.RequestWorkerDrain(t.Context(), reader(), in)
	if err != nil || result.Complete || result.Process != "incomplete" || !result.DeadlineExpired || w.Status().AdmissionClosed {
		t.Fatalf("expired request invoked %+v %v", result, err)
	}
}

func TestWorkerDrainIncompleteRemainsIncompleteAfterQuiescence(t *testing.T) {
	s, store, _, _ := lifecycleFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	w, err := drt.NewWorker(store, drt.Options{Namespace: "allowed", Queue: "queue", BuildID: "build", Owner: "owner", RuntimeID: "runtime", InstanceID: "instance", Workflows: map[string]drt.WorkflowFunc{"wf": func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Activity("a", "a", "", nil).Get() }}, Activities: map[string]drt.ActivityFunc{"a": func(context.Context, drt.ActivityInfo, []byte) ([]byte, error) {
		close(entered)
		<-release
		return []byte("done"), nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) {
		return LocalWorkerControl{Worker: w}, nil
	}
	if _, err = w.StartExecution(t.Context(), durable.StartRequest{Key: durable.Key{Namespace: "allowed", WorkflowID: "workflow", RunID: "run"}, RequestID: "start", BuildID: "build", Queue: "queue", WorkflowType: "wf"}); err != nil {
		t.Fatal(err)
	}
	if _, err = w.RunOnce(t.Context(), durable.TaskWorkflow); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _ = w.RunOnce(t.Context(), durable.TaskActivity); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("activity did not enter")
	}
	input := drainInput()
	input.Deadline = durable.Timestamp(time.Now().Add(50 * time.Millisecond))
	result, err := s.RequestWorkerDrain(t.Context(), reader(), input)
	if err != nil || result.Complete || result.Quiescent || result.Process != "incomplete" {
		t.Fatalf("live handler became completed %+v %v", result, err)
	}
	close(release)
	<-done
	replay, err := s.RequestWorkerDrain(t.Context(), reader(), input)
	if err != nil || replay.Complete || !replay.Quiescent || replay.Process != "incomplete" || !replay.DeadlineExpired || replay.AcceptedAt != result.AcceptedAt {
		t.Fatalf("late quiescence rewrote drain %+v %v", replay, err)
	}
}
func TestLifecycleWireVersionsRejectLossyValues(t *testing.T) {
	for _, value := range []string{"", "-1", "01", "1.0", "1e2", "9223372036854775808"} {
		if _, err := lifecycleVersion(value, true); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("accepted %q: %v", value, err)
		}
	}
	n, err := lifecycleVersion("9007199254740993", false)
	if err != nil || n != 9007199254740993 {
		t.Fatalf("lost version precision %d %v", n, err)
	}
}

type missingDrainResponseStore struct{ *memory.Store }

func (s missingDrainResponseStore) RequestWorkerDrain(ctx context.Context, r durable.WorkerDrainRequest) (durable.LifecycleReceipt, error) {
	receipt, err := s.Store.RequestWorkerDrain(ctx, r)
	receipt.WorkerDrain = nil
	return receipt, err
}
func TestWorkerDrainMissingAcceptedResponseFailsClosed(t *testing.T) {
	s, store, w, _ := lifecycleFixture(t)
	s.store = missingDrainResponseStore{Store: store}
	if _, err := s.RequestWorkerDrain(t.Context(), reader(), drainInput()); !errors.Is(err, security.ErrUnavailable) || w.Status().AdmissionClosed {
		t.Fatalf("missing accepted response invoked process: %v", err)
	}
}

func TestWorkerDrainCompletedOutcomeSurvivesDeadline(t *testing.T) {
	s, _, _, _ := lifecycleFixture(t)
	in := drainInput()
	in.Deadline = durable.Timestamp(time.Now().Add(100 * time.Millisecond))
	accepted, err := s.RequestWorkerDrain(t.Context(), reader(), in)
	if err != nil || !accepted.Complete || accepted.DeadlineExpired {
		t.Fatalf("initial drain %+v %v", accepted, err)
	}
	<-time.After(time.Until(in.Deadline) + time.Millisecond)
	replay, err := s.RequestWorkerDrain(t.Context(), reader(), in)
	if err != nil || !replay.Complete || replay.DeadlineExpired || replay.Process != "complete" || replay.AcceptedAt != accepted.AcceptedAt {
		t.Fatalf("expired observation changed completed outcome %+v %v", replay, err)
	}
}
