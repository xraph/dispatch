package operator

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/security"
)

func TestWorkerDrainResolutionCrossesDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _, w, _ := lifecycleFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		calls := 0
		s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) {
			calls++
			if calls == 2 {
				close(entered)
				<-release
			}
			return LocalWorkerControl{Worker: w}, nil
		}
		input := drainInput()
		input.Deadline = time.Now().Add(time.Second)
		done := make(chan WorkerDrainAcceptance, 1)
		go func() {
			out, err := s.RequestWorkerDrain(t.Context(), reader(), input)
			if err != nil {
				t.Error(err)
			}
			done <- out
		}()
		<-entered
		time.Sleep(2 * time.Second)
		close(release)
		out := <-done
		if w.Status().AdmissionClosed || w.Status().Drain != nil {
			t.Fatal("first drain mutated process after resolution crossed deadline")
		}
		if out.Process != "incomplete" || !out.DeadlineExpired || out.Deadline != input.Deadline {
			t.Fatalf("missed first invocation %+v", out)
		}
	})
}

func TestWorkerDrainLostReplyAfterExpiryRemainsUnknown(t *testing.T) {
	for _, lostAcceptance := range []bool{false, true} {
		for _, replacement := range []bool{false, true} {
			synctest.Test(t, func(t *testing.T) {
				s, store, w, grants := lifecycleFixture(t)
				input := drainInput()
				input.Deadline = time.Now().Add(time.Second)
				if lostAcceptance {
					s.store = &lostDrainReceiptStore{Store: store, lose: true}
				} else {
					control := &lostDrainControl{LocalWorkerControl: LocalWorkerControl{Worker: w}, lose: true}
					s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) { return control, nil }
				}
				_, err := s.RequestWorkerDrain(t.Context(), reader(), input)
				if lostAcceptance && !errors.Is(err, security.ErrUnavailable) {
					t.Fatal(err)
				}
				if !lostAcceptance {
					status := w.Status()
					if status.Drain == nil {
						t.Fatal("original invocation missing")
					}
					result, waitErr := w.WaitDrain(t.Context(), *status.Drain)
					if waitErr != nil || !result.Complete {
						t.Fatalf("original did not complete %+v %v", result, waitErr)
					}
				}
				other, createErr := drt.NewWorker(store, drt.Options{Namespace: "allowed", Queue: "queue", BuildID: "build", Owner: "owner", RuntimeID: "replacement", InstanceID: "instance"})
				if createErr != nil {
					t.Fatal(createErr)
				}
				s.workerControl = func(context.Context, durable.QueryRuntimeTarget) (WorkerControl, error) {
					if replacement {
						return LocalWorkerControl{Worker: other}, nil
					}
					return nil, ErrRuntimeUnavailable
				}
				time.Sleep(2 * time.Second)
				out, err := s.RequestWorkerDrain(t.Context(), reader(), input)
				if err != nil || out.Process != "unknown" || !out.DeadlineExpired || out.Complete || out.Deadline != input.Deadline || other.Status().AdmissionClosed {
					t.Fatalf("unobserved incarnation became terminal: %+v %v", out, err)
				}
				(*grants)["allowed"] = false
				if _, err = s.RequestWorkerDrain(t.Context(), reader(), input); !errors.Is(err, security.ErrForbidden) {
					t.Fatalf("revoked unknown replay: %v", err)
				}
			})
		}
	}
}
