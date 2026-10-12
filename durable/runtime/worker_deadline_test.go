package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStrictDrainAcceptanceChecksDeadlineUnderLock(t *testing.T) {
	w := &Worker{options: Options{RuntimeID: "runtime"}, lifecycle: newWorkerLifecycle()}
	work, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	w.lifecycle.active[&claimOperation{cancel: cancel}] = struct{}{}
	request := DrainRequest{OperationID: "drain", Deadline: time.Now().Add(30 * time.Millisecond)}
	w.lifecycle.mu.Lock()
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() { close(entered); _, err := w.BeginDrainBeforeDeadline(t.Context(), request); done <- err }()
	<-entered
	// Keep acceptance synchronization held until the actual operation deadline.
	timer := time.NewTimer(time.Until(request.Deadline))
	<-timer.C
	w.lifecycle.mu.Unlock()
	if err := <-done; !errors.Is(err, ErrDrainDeadline) {
		t.Fatalf("late acceptance: %v", err)
	}
	if w.Status().AdmissionClosed || w.Status().Drain != nil || work.Err() != nil {
		t.Fatal("late first acceptance mutated admission or work")
	}
}

func TestStrictDrainReplayAndDirectExpiredCompatibility(t *testing.T) {
	w := &Worker{options: Options{RuntimeID: "runtime"}, lifecycle: newWorkerLifecycle()}
	r := DrainRequest{OperationID: "drain", Deadline: time.Now().Add(20 * time.Millisecond)}
	h, err := w.BeginDrainBeforeDeadline(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(r.Deadline))
	<-timer.C
	if replay, replayErr := w.BeginDrainBeforeDeadline(t.Context(), r); replayErr != nil || replay != h {
		t.Fatalf("expired accepted replay %+v %v", replay, replayErr)
	}
	if result, waitErr := w.WaitDrain(t.Context(), h); waitErr != nil || !result.Complete {
		t.Fatalf("completed result changed %+v %v", result, waitErr)
	}
	direct := &Worker{options: Options{RuntimeID: "direct"}, lifecycle: newWorkerLifecycle()}
	h, err = direct.BeginDrain(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if result, waitErr := direct.WaitDrain(t.Context(), h); !errors.Is(waitErr, ErrDrainIncomplete) || !result.DeadlineExpired || result.Complete {
		t.Fatalf("direct expired request changed %+v %v", result, waitErr)
	}
}
