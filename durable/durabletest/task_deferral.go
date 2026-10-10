package durabletest

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func RunWorkflowTaskDeferral(t *testing.T, s durable.Store) {
	t.Helper()
	store, ok := s.(durable.WorkflowTaskDeferralStore)
	if !ok {
		t.Fatal("deferral capability missing")
	}
	life, catalog := lifecycleCapabilities(t, s)
	for _, reference := range []string{"child", "continuation"} {
		for _, state := range []durable.DeferralTargetState{durable.DeferralUnregistered, durable.DeferralRetiring, durable.DeferralRetired} {
			t.Run(reference+"/"+string(state), func(t *testing.T) {
				ns := fmt.Sprintf("defer-%d", time.Now().UnixNano())
				target := durable.BuildTarget{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, BuildID: "target"}
				if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", RequireAudit: true, SchemaVersion: 1}); err != nil {
					t.Fatal(err)
				}
				start := lifecycleStart(t, s, ns, "source", "source")
				task := lifecycleClaim(t, s, start)
				if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: target.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
					t.Fatal(err)
				}
				epoch := int64(0)
				if state != durable.DeferralUnregistered {
					if _, err := life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "register"}); err != nil {
						t.Fatal(err)
					}
					if _, err := life.BeginBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "begin", ExpectedEpoch: 1, ExpectedVersion: 1}); err != nil {
						t.Fatal(err)
					}
					epoch = 2
					if state == durable.DeferralRetired {
						if _, err := life.FinalizeBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}); err != nil {
							t.Fatal(err)
						}
					}
				}
				before, err := s.GetExecution(t.Context(), start.Key)
				if err != nil {
					t.Fatal(err)
				}
				history, err := s.ReadHistory(t.Context(), start.Key, 0, 100)
				if err != nil {
					t.Fatal(err)
				}
				child := start
				child.WorkflowID = "child"
				child.BuildID = "target"

				decision := durable.CommitRequest{Key: start.Key, RequestID: "decision", ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "decision"}}}
				if reference == "child" {
					decision.Children = []durable.ChildStartSpec{{CommandID: "child", Start: child, ParentQueue: start.Queue, ParentClosePolicy: durable.ParentCloseAbandon}}
				} else {
					decision.State = durable.StateContinuedAsNew
					decision.Continuation = &durable.ContinueSpec{RunID: "next", WorkflowType: start.WorkflowType, BuildID: "target", Queue: start.Queue}
				}
				_, err = s.CommitTransition(t.Context(), decision)
				var refusal *durable.BuildAdmissionError
				if !errors.As(err, &refusal) || refusal.State != string(state) || refusal.RetirementEpoch != epoch {
					t.Fatalf("refusal: %+v %v", refusal, err)
				}
				request, err := durable.NewWorkflowTaskDeferralRequest(task, 1, refusal)
				if err != nil {
					t.Fatal(err)
				}
				metadata := durable.AuditMetadata{ActorKind: "worker", ActorID: "qualified-worker", CorrelationID: "decision-correlation"}
				accepted, err := store.DeferWorkflowTask(durable.WithAuditMetadata(t.Context(), metadata), request)
				if err != nil {
					t.Fatal(err)
				}
				if accepted.DeferralCount != 1 || accepted.RetryAt.Sub(accepted.RecordedAt) != time.Second || accepted.FirstSequence != 0 || accepted.LastSequence != 0 || accepted.Revision != 1 || accepted.PolicyVersion != 1 || accepted.TargetState != state || accepted.Reason != "target_"+string(state) {
					t.Fatalf("receipt: %+v", accepted)
				}
				verifyDeferralDelivery(t, s, request, metadata)
				after, err := s.GetExecution(t.Context(), start.Key)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("execution changed: %+v %v", after, err)
				}
				afterHistory, err := s.ReadHistory(t.Context(), start.Key, 0, 100)
				if err != nil || !reflect.DeepEqual(history, afterHistory) {
					t.Fatal("deferral changed history")
				}
				scheduled, err := s.GetTask(t.Context(), start.Key, task.ID)
				if err != nil || scheduled.Done || scheduled.Owner != "" || !scheduled.LeaseUntil.IsZero() || scheduled.Epoch != task.Epoch || scheduled.Attempt != task.Attempt || scheduled.Version != task.Version+1 || !scheduled.AvailableAt.Equal(accepted.RetryAt) {
					t.Fatalf("schedule: %+v %v", scheduled, err)
				}
				current, err := store.GetWorkflowTaskDeferral(t.Context(), start.Key, task.ID)
				if err != nil || !current.Active || current.WorkflowTaskDeferralReceipt != accepted {
					t.Fatalf("current: %+v %v", current, err)
				}
				if _, err = s.RenewTask(t.Context(), start.Key, task.Token(), time.Minute); !errors.Is(err, durable.ErrLeaseLost) {
					t.Fatalf("released token renewed: %v", err)
				}
				if grant, claimErr := s.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: ns, Queue: start.Queue, BuildID: start.BuildID, Kind: durable.TaskWorkflow, Owner: "early", LeaseDuration: time.Minute}); claimErr != nil || grant != nil {
					t.Fatalf("hot poll: %+v %v", grant, claimErr)
				}
				// State changes at the same retirement epoch still invalidate new deferrals.
				switch state {
				case durable.DeferralRetiring:
					if _, err = life.FinalizeBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "final", ExpectedEpoch: 2, ExpectedVersion: 2}); err != nil {
						t.Fatal(err)
					}
				case durable.DeferralUnregistered:
					if _, err = life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: target, RequestID: "register"}); err != nil {
						t.Fatal(err)
					}
				default:
					if _, err = life.AbortBuildRetirement(t.Context(), durable.BuildRetirementRequest{BuildTarget: target, RequestID: "resume", ExpectedEpoch: 2, ExpectedVersion: 3}); err != nil {
						t.Fatal(err)
					}
				}
				replay, replayErr := store.DeferWorkflowTask(t.Context(), request)
				if replayErr != nil || replay != accepted {
					t.Fatalf("receipt after state change: %+v %v", replay, replayErr)
				}
				request.TargetState = "invalid"
				if _, err = store.DeferWorkflowTask(t.Context(), request); !errors.Is(err, durable.ErrInvalid) {
					t.Fatalf("invalid state accepted: %v", err)
				}
				waitDeferral(t, accepted.RetryAt)
				replacement := lifecycleClaim(t, s, start)
				current, err = store.GetWorkflowTaskDeferral(t.Context(), start.Key, task.ID)
				if err != nil || current.Active {
					t.Fatalf("reclaimed marked deferred: %+v %v", current, err)
				}
				stale, err := durable.NewWorkflowTaskDeferralRequest(replacement, 1, refusal)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = store.DeferWorkflowTask(t.Context(), stale); !errors.Is(err, durable.ErrAdmissionChanged) {
					t.Fatalf("state change released task: %v", err)
				}
				held, err := s.GetTask(t.Context(), start.Key, task.ID)
				if err != nil || held.Owner != replacement.Owner || held.Version != replacement.Version {
					t.Fatal("changed target mutated task")
				}
				if state == durable.DeferralUnregistered {
					if _, err = s.CommitTransition(t.Context(), durable.CommitRequest{Key: start.Key, RequestID: "close", ExpectedRevision: 1, Token: replacement.Token(), State: durable.StateCompleted, Events: []durable.EventInput{{Type: "completed"}}}); err != nil {
						t.Fatal(err)
					}
					request.TargetState = state
					closedReplay, replayErr := store.DeferWorkflowTask(t.Context(), request)
					if replayErr != nil || closedReplay != accepted {
						t.Fatalf("receipt after replacement/closure: %+v %v", closedReplay, replayErr)
					}
				}
				if state == durable.DeferralRetiring {
					refusal.State = string(durable.DeferralRetired)
					changed, makeErr := durable.NewWorkflowTaskDeferralRequest(replacement, 1, refusal)
					if makeErr != nil {
						t.Fatal(makeErr)
					}
					reset, deferErr := store.DeferWorkflowTask(t.Context(), changed)
					if deferErr != nil || reset.DeferralCount != 1 {
						t.Fatalf("state count reset: %+v %v", reset, deferErr)
					}
					waitDeferral(t, reset.RetryAt)
					again := lifecycleClaim(t, s, start)
					repeated, makeErr := durable.NewWorkflowTaskDeferralRequest(again, 1, refusal)
					if makeErr != nil {
						t.Fatal(makeErr)
					}
					second, deferErr := store.DeferWorkflowTask(t.Context(), repeated)
					if deferErr != nil || second.DeferralCount != 2 || second.RetryAt.Sub(second.RecordedAt) != 2*time.Second {
						t.Fatalf("backoff: %+v %v", second, deferErr)
					}
				}
			})
		}
	}
}
func waitDeferral(t *testing.T, at time.Time) {
	t.Helper()
	timer := time.NewTimer(time.Until(at) + time.Millisecond)
	defer timer.Stop()
	select {
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-timer.C:
	}
}

func RunWorkflowTaskDeferralIntentRollback(t *testing.T, s durable.Store, inject func() func()) {
	t.Helper()
	life, catalog := lifecycleCapabilities(t, s)
	store, ok := s.(durable.WorkflowTaskDeferralStore)
	if !ok {
		t.Fatal("deferral capability missing")
	}
	ns := fmt.Sprintf("defer-rollback-%d", time.Now().UnixNano())
	if _, err := catalog.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "i", Namespace: ns, AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}); err != nil {
		t.Fatal(err)
	}
	start := lifecycleStart(t, s, ns, "source", "source")
	task := lifecycleClaim(t, s, start)
	if _, err := life.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "i", Namespace: ns}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	request, err := durable.NewWorkflowTaskDeferralRequest(task, 1, &durable.BuildAdmissionError{BuildID: "missing", State: "unregistered", ReferenceKind: "continuation"})
	if err != nil {
		t.Fatal(err)
	}
	restore := inject()
	if _, err = store.DeferWorkflowTask(t.Context(), request); err == nil {
		t.Fatal("audit failure accepted deferral")
	}
	after, err := s.GetTask(t.Context(), start.Key, task.ID)
	if err != nil || !reflect.DeepEqual(after, task) {
		t.Fatalf("audit failure released task: %+v %v", after, err)
	}
	if _, err = store.GetWorkflowTaskDeferral(t.Context(), start.Key, task.ID); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("audit failure stored deferral: %v", err)
	}
	restore()
	if _, err = store.DeferWorkflowTask(t.Context(), request); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}

func verifyDeferralDelivery(t *testing.T, s durable.Store, r durable.WorkflowTaskDeferralRequest, metadata durable.AuditMetadata) {
	t.Helper()
	outbox, ok := s.(durable.OutboxStore)
	if !ok {
		t.Fatal("outbox capability missing")
	}
	q := durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "i", Destination: durable.DestinationChronicle}, Limit: durable.MaxDeliveryBatch}
	metadata.RequestID = r.RequestID
	metadata.ReasonCode = "target_" + string(r.TargetState)
	for {
		status, err := outbox.DeliveryStatus(t.Context(), q)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range status.Records {
			d := record.Delivery
			q.After = d.ID
			if d.Namespace != r.Namespace || d.WorkflowID != r.WorkflowID || d.RunID != r.RunID || d.SourceKind != "execution_receipt" || d.SourceID != durable.ReceiptSourceID(r.RequestID) {
				continue
			}
			if d.Action != "workflow.task_deferred" || d.Metadata != metadata || d.Verify() != nil {
				t.Fatalf("deferral audit facts: %+v", d)
			}
			mapped, mapErr := ecosystem.ChronicleRequest(ecosystem.Binding{Producer: "dispatch", InstallationID: d.InstallationID, Namespace: d.Namespace, AppID: d.AppID, TenantID: d.TenantID}, d)
			if mapErr != nil || mapped.SourceKey != d.ID || mapped.SourceFingerprint != d.Fingerprint || mapped.Event.Action != d.Action || mapped.Event.RequestID != r.RequestID || mapped.Event.Reason != metadata.ReasonCode {
				t.Fatalf("deferral Chronicle mapping: %+v %v", mapped, mapErr)
			}
			return
		}
		if len(status.Records) < q.Limit {
			t.Fatal("required deferral intent missing")
		}
	}
}
