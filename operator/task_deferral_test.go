package operator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

type deferralReadSpy struct {
	*memory.Store
	calls int
}

func (s *deferralReadSpy) GetWorkflowTaskDeferral(ctx context.Context, key durable.Key, id string) (durable.WorkflowTaskDeferral, error) {
	s.calls++
	return s.Store.GetWorkflowTaskDeferral(ctx, key, id)
}
func TestTaskDeferralProjectionRequiresTaskAuthorization(t *testing.T) {
	service, store, _ := fixture(t)
	key := seed(t, store, "allowed", "w")
	if _, err := store.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: durable.NamespaceTarget{InstallationID: "install", Namespace: key.Namespace}, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	task, err := store.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: key.Namespace, Queue: "queue", BuildID: "historic", Kind: durable.TaskWorkflow, Owner: "worker", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatalf("claim: %+v %v", task, err)
	}
	request, err := durable.NewWorkflowTaskDeferralRequest(*task, 1, &durable.BuildAdmissionError{BuildID: "missing", State: "unregistered", ReferenceKind: "child", CommandID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.DeferWorkflowTask(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	spy := &deferralReadSpy{Store: store}
	service.store = spy
	page, err := service.Tasks(t.Context(), reader(), durable.TaskList{Key: key, Limit: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].Deferral == nil {
		t.Fatalf("tasks: %+v %v", page, err)
	}
	d := page.Items[0].Deferral
	if !d.Active || d.TargetRetirementEpoch != "0" || d.SourceRevision != "1" || d.Count != "1" || d.Reason != "target_unregistered" {
		t.Fatalf("projection: %+v", d)
	}
	data, err := json.Marshal(page)
	if err != nil || strings.Contains(string(data), "PAYLOAD_SECRET") {
		t.Fatal("unsafe projection")
	}
	denied := reader()
	denied.Subject = "denied"
	if _, err = service.Tasks(t.Context(), denied, durable.TaskList{Key: key, Limit: 10}); !errors.Is(err, security.ErrForbidden) {
		t.Fatalf("denial: %v", err)
	}
	if spy.calls != 1 {
		t.Fatalf("unauthorized deferral lookup: %d", spy.calls)
	}
}
