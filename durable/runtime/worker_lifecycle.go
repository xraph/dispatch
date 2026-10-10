package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xraph/dispatch/durable"
)

// WorkerState describes polling lifecycle independently of database liveness.
type WorkerState string

const (
	WorkerNotStarted WorkerState = "not_started"
	WorkerRunning    WorkerState = "running"
	WorkerDraining   WorkerState = "draining"
	WorkerStopped    WorkerState = "stopped"
	WorkerFailed     WorkerState = "failed"
)

var (
	ErrWorkerStarted   = errors.New("durable runtime: worker already started")
	ErrWorkerDraining  = errors.New("durable runtime: worker claim admission closed")
	ErrDrainIncomplete = errors.New("durable runtime: drain incomplete")
)

// DrainRequest fixes the operation deadline independently of any observer.
// Retrying an operation must preserve both fields.
type DrainRequest struct {
	OperationID string
	Deadline    time.Time
}

// DrainHandle binds an accepted operation to one immutable Worker incarnation.
type DrainHandle struct {
	RuntimeID string
	DrainRequest
}

// ClaimStatus counts local calls, including claims awaiting a database response.
type ClaimStatus struct {
	Kind       durable.TaskKind
	Claiming   int64
	Processing int64
}

// WorkerStatus reports process facts. Zero local work does not prove a build
// has no persisted runs, callback grants, timers or child obligations.
type WorkerStatus struct {
	State                        WorkerState
	Namespace, Queue, BuildID    string
	Owner, RuntimeID, InstanceID string
	Ready, AdmissionClosed       bool
	InFlight                     int64
	Claims                       []ClaimStatus
	UnknownClaims                int64
	Failure                      string
	Drain                        *DrainHandle
	ObservedAt                   time.Time
}

// DrainResult separates the immutable operation outcome from current quiescence.
// Deadline expiry and unknown claims never become a successful drain on replay.
type DrainResult struct {
	Handle          DrainHandle
	Complete        bool
	DeadlineExpired bool
	Quiescent       bool
	InFlight        int64
	UnknownClaims   int64
	ObservedAt      time.Time
}

type claimOperation struct {
	kind     durable.TaskKind
	claiming bool
	cancel   context.CancelCauseFunc
}

type drainOperation struct {
	handle     DrainHandle
	done       chan struct{}
	ended      bool
	expired    bool
	incomplete bool
}

type workerLifecycle struct {
	mu        sync.Mutex
	state     WorkerState
	started   bool
	closed    bool
	admission chan struct{}
	changed   chan struct{}
	active    map[*claimOperation]struct{}
	unknown   int64
	failure   string
	drain     *drainOperation
}

var claimKinds = [...]durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity, durable.TaskTimer, TaskTimeout, TaskChildDelivery, TaskExecutionTimeout}

func newWorkerLifecycle() workerLifecycle {
	return workerLifecycle{state: WorkerNotStarted, admission: make(chan struct{}), changed: make(chan struct{}), active: make(map[*claimOperation]struct{})}
}

// Status returns a private snapshot and never calls storage or user code.
func (w *Worker) Status() WorkerStatus {
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	s := WorkerStatus{State: l.state, Namespace: w.options.Namespace, Queue: w.options.Queue, BuildID: w.options.BuildID,
		Owner: w.options.Owner, RuntimeID: w.options.RuntimeID, InstanceID: w.options.InstanceID,
		Ready: l.state == WorkerRunning && !l.closed, AdmissionClosed: l.closed, InFlight: int64(len(l.active)),
		UnknownClaims: l.unknown, Failure: l.failure, ObservedAt: time.Now().UTC()}
	for _, kind := range claimKinds {
		entry := ClaimStatus{Kind: kind}
		for op := range l.active {
			if op.kind == kind {
				if op.claiming {
					entry.Claiming++
				} else {
					entry.Processing++
				}
			}
		}
		s.Claims = append(s.Claims, entry)
	}
	if l.drain != nil {
		handle := l.drain.handle
		s.Drain = &handle
	}
	return s
}

// BeginDrain closes claim admission permanently. Its context only governs
// acceptance; the explicit operation deadline governs cancellation of work.
func (w *Worker) BeginDrain(ctx context.Context, r DrainRequest) (DrainHandle, error) {
	if !validID(r.OperationID) || r.Deadline.IsZero() {
		return DrainHandle{}, durable.ErrInvalid
	}
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return DrainHandle{}, err
	}
	if l.drain != nil {
		if l.drain.handle.OperationID != r.OperationID || !l.drain.handle.Deadline.Equal(r.Deadline) {
			return l.drain.handle, durable.ErrRequestConflict
		}
		return l.drain.handle, nil
	}
	d := &drainOperation{handle: DrainHandle{RuntimeID: w.options.RuntimeID, DrainRequest: r}, done: make(chan struct{})}
	l.drain = d
	l.closeAdmission()
	if !r.Deadline.After(time.Now()) {
		l.expireDrain()
	} else {
		l.finishIdle()
		if !d.ended {
			go w.drainDeadline(d)
		}
	}
	return d.handle, nil
}

func (w *Worker) drainDeadline(d *drainOperation) {
	timer := time.NewTimer(time.Until(d.handle.Deadline))
	defer timer.Stop()
	select {
	case <-d.done:
		return
	case <-timer.C:
	}
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	if !d.ended {
		l.expireDrain()
	}
}

func (l *workerLifecycle) expireDrain() {
	l.drain.expired = true
	l.drain.incomplete = true
	for op := range l.active {
		op.cancel(ErrDrainIncomplete)
	}
	l.endDrain()
	l.finishIdle()
}

// WaitDrain waits without changing the operation's deadline or cancellation.
func (w *Worker) WaitDrain(ctx context.Context, handle DrainHandle) (DrainResult, error) {
	l := &w.lifecycle
	l.mu.Lock()
	if l.drain == nil || handle.RuntimeID != w.options.RuntimeID || handle.OperationID != l.drain.handle.OperationID || !handle.Deadline.Equal(l.drain.handle.Deadline) {
		l.mu.Unlock()
		return DrainResult{}, durable.ErrRequestConflict
	}
	done := l.drain.done
	l.mu.Unlock()
	select {
	case <-ctx.Done():
		return w.drainResult(), ctx.Err()
	case <-done:
	}
	result := w.drainResult()
	if !result.Complete {
		return result, ErrDrainIncomplete
	}
	return result, nil
}

func (w *Worker) drainResult() DrainResult {
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	return DrainResult{Handle: l.drain.handle, Complete: l.drain.ended && !l.drain.incomplete && l.failure == "" && l.unknown == 0 && len(l.active) == 0,
		DeadlineExpired: l.drain.expired, Quiescent: len(l.active) == 0, InFlight: int64(len(l.active)), UnknownClaims: l.unknown, ObservedAt: time.Now().UTC()}
}

// Stop closes admission, cancels work and waits for actual call completion.
// A timed-out caller can wait again; it cannot force an uncooperative handler out.
func (w *Worker) Stop(ctx context.Context) error {
	l := &w.lifecycle
	l.mu.Lock()
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return err
	}
	l.closeAdmission()
	for op := range l.active {
		op.cancel(ErrWorkerDraining)
	}
	// Explicit force-stop cannot masquerade as a successful graceful drain.
	if l.drain != nil && !l.drain.ended && len(l.active) != 0 {
		l.drain.incomplete = true
		l.endDrain()
	}
	l.finishIdle()
	for len(l.active) != 0 {
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		l.mu.Lock()
	}
	l.mu.Unlock()
	return nil
}

func (l *workerLifecycle) closeAdmission() {
	if !l.closed {
		l.closed = true
		close(l.admission)
	}
	if l.state != WorkerFailed && l.state != WorkerStopped {
		l.state = WorkerDraining
	}
}
func (l *workerLifecycle) endDrain() {
	if l.drain != nil && !l.drain.ended {
		l.drain.ended = true
		close(l.drain.done)
	}
}
func (l *workerLifecycle) finishIdle() {
	if l.closed && len(l.active) == 0 {
		if l.state != WorkerFailed {
			l.state = WorkerStopped
		}
		if l.drain != nil && !l.drain.ended && !l.drain.handle.Deadline.After(time.Now()) {
			l.drain.expired, l.drain.incomplete = true, true
		}
		l.endDrain()
	}
}

func (w *Worker) startRun(ctx context.Context) error {
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.closed {
		return ErrWorkerDraining
	}
	if l.started {
		return ErrWorkerStarted
	}
	l.started, l.state = true, WorkerRunning
	return nil
}
func (w *Worker) finishRun(err error) {
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closeAdmission()
	if err != nil {
		l.state, l.failure = WorkerFailed, "processing_failed"
	}
	l.finishIdle()
}

func (w *Worker) enterClaim(ctx context.Context, kind durable.TaskKind) (context.Context, *claimOperation, error) {
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if l.closed {
		return nil, nil, ErrWorkerDraining
	}
	workCtx, cancel := context.WithCancelCause(ctx)
	op := &claimOperation{kind: kind, claiming: true, cancel: cancel}
	l.active[op] = struct{}{}
	return workCtx, op, nil
}
func (w *Worker) claimReturned(op *claimOperation, err error) {
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	op.claiming = false
	if err != nil && !definitiveCommitError(err) {
		l.unknown++
	}
}
func (w *Worker) leaveClaim(op *claimOperation, err error) {
	op.cancel(nil)
	l := &w.lifecycle
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.drain != nil && err != nil {
		l.drain.incomplete = true
	}
	delete(l.active, op)
	close(l.changed)
	l.changed = make(chan struct{})
	l.finishIdle()
}

func (w *Worker) pollWait(ctx context.Context) error {
	timer := time.NewTimer(w.options.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.lifecycle.admission:
		return ErrWorkerDraining
	case <-timer.C:
		return nil
	}
}
