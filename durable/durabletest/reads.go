package durabletest

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

type ReadBackend interface {
	durable.Store
	durable.ReadStore
	durable.NamespaceStore
	durable.OutboxStore
}

// RunReads qualifies namespace predicates and immutable keyset order on real stores.
func RunReads(t *testing.T, s ReadBackend, tie func([]durable.Key)) {
	t.Helper()
	ctx := t.Context()
	t.Run("maximum_identifiers", func(t *testing.T) { readMaximumIdentifiers(t, s) })
	keys := make([]durable.Key, 0, 6)
	for _, ns := range []string{"read-allowed", "read-foreign"} {
		if _, err := s.RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: "read-install", Namespace: ns, AppID: "read-app", TenantID: "read-tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}); err != nil {
			t.Fatal(err)
		}
		for _, wf := range []string{"a", "z", "é"} {
			key := durable.Key{Namespace: ns, WorkflowID: wf, RunID: "run"}
			keys = append(keys, key)
			if _, err := s.StartExecution(ctx, durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "workflow", BuildID: "build", Queue: "queue"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	tie(keys)
	list := durable.ExecutionList{Namespace: "read-allowed", Limit: 1}
	first, next, err := s.ListExecutions(ctx, list)
	if err != nil || len(first) != 1 || next == "" || first[0].WorkflowID != "é" {
		t.Fatalf("%+v %s %v", first, next, err)
	}
	list.Cursor = next
	if _, err = s.SignalExecution(ctx, durable.SignalRequest{Key: durable.Key{Namespace: "read-allowed", WorkflowID: "a", RunID: "run"}, RequestID: "signal", BuildID: "build", Name: "event"}); err != nil {
		t.Fatal(err)
	}
	second, next, err := s.ListExecutions(ctx, list)
	if err != nil || len(second) != 1 || second[0].WorkflowID != "z" || next == "" {
		t.Fatalf("%+v %v", second, err)
	}
	list.Cursor = next
	last, next, err := s.ListExecutions(ctx, list)
	if err != nil || len(last) != 1 || last[0].WorkflowID != "a" || next != "" {
		t.Fatalf("%+v %v", last, err)
	}
	list.Namespace = "read-foreign"
	if _, _, err = s.ListExecutions(ctx, list); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("foreign cursor", err)
	}
	list.Namespace = ""
	if _, _, err = s.ListExecutions(ctx, list); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("empty namespace", err)
	}
	list = durable.ExecutionList{Namespace: "absent", Limit: 1}
	if rows, _, readErr := s.ListExecutions(ctx, list); readErr != nil || len(rows) != 0 {
		t.Fatal(rows, readErr)
	}
	facts, err := s.ReadBuildFacts(ctx, "read-allowed", "build")
	if err != nil || facts.Executions != 3 || facts.Running != 3 || facts.PendingTasks != 4 {
		t.Fatalf("%+v %v", facts, err)
	}
	tasks, _, err := s.ListTasks(ctx, durable.TaskList{Key: keys[0], Limit: 1})
	if err != nil || len(tasks) != 1 || tasks[0].Namespace != "read-allowed" {
		t.Fatal(tasks, err)
	}
	if _, _, err = s.ListTasks(ctx, durable.TaskList{Key: durable.Key{WorkflowID: "a", RunID: "run"}, Limit: 1}); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal(err)
	}
	taskRequest := durable.TaskList{Key: keys[0], Limit: 1}
	taskRows, taskNext, taskErr := s.ListTasks(ctx, taskRequest)
	if taskErr != nil || len(taskRows) != 1 || taskNext == "" {
		t.Fatal(taskRows, taskNext, taskErr)
	}
	taskRequest.Cursor = taskNext
	taskRequest.RunID = "foreign"
	if _, _, taskErr = s.ListTasks(ctx, taskRequest); !errors.Is(taskErr, durable.ErrInvalid) {
		t.Fatal("foreign task cursor", taskErr)
	}
	taskRequest.Key = keys[0]
	taskRequest.Kind = durable.TaskActivity
	if _, _, taskErr = s.ListTasks(ctx, taskRequest); !errors.Is(taskErr, durable.ErrInvalid) {
		t.Fatal("changed task filter", taskErr)
	}
	taskRequest.Kind = ""
	taskRows, taskNext, taskErr = s.ListTasks(ctx, taskRequest)
	if taskErr != nil || len(taskRows) != 1 || taskNext != "" {
		t.Fatal(taskRows, taskNext, taskErr)
	}

	statusReq := durable.ScopedDeliveryStatus{Key: keys[0], DeliveryStatusRequest: durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "read-install", Destination: durable.DestinationChronicle}, Limit: 100}}
	status, err := s.ReadDeliveryStatus(ctx, statusReq)
	if err != nil || status.Pending < 2 {
		t.Fatalf("%+v %v", status, err)
	}
	for _, d := range status.Records {
		if d.Delivery.Namespace != keys[0].Namespace || d.Delivery.WorkflowID != "a" {
			t.Fatal("foreign delivery", d.Delivery)
		}
	}
	claims, err := s.ClaimDeliveries(ctx, durable.DeliveryClaim{DeliveryScope: statusReq.DeliveryScope, Owner: "read-worker", Limit: 100, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range claims {
		if d.Delivery.Namespace == keys[0].Namespace && d.Delivery.WorkflowID == "a" {
			if err = s.BlockDelivery(ctx, d.Token()); err != nil {
				t.Fatal(err)
			}
		}
	}
	status, err = s.ReadDeliveryStatus(ctx, statusReq)
	if err != nil || status.Blocked != status.Pending {
		t.Fatalf("%+v %v", status, err)
	}
	statusReq.Namespace = "read-foreign"
	foreign, err := s.ReadDeliveryStatus(ctx, statusReq)
	if err != nil || foreign.Blocked != 0 || foreign.Pending < 1 {
		t.Fatalf("%+v %v", foreign, err)
	}
	statusReq.Namespace = ""
	if _, err = s.ReadDeliveryStatus(ctx, statusReq); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal(err)
	}
}

// readMaximumIdentifiers uses the write API, so the fixtures cannot invent
// identities that the durable execution and task contracts reject.
func readMaximumIdentifiers(t *testing.T, s ReadBackend) {
	ctx := t.Context()
	for i, character := range []string{"x", "<", "\x01"} {
		t.Run(fmt.Sprintf("encoding_%d", i), func(t *testing.T) {
			namespace := fmt.Sprintf("max-read-%d", i) + strings.Repeat("n", 502)
			id := "a" + strings.Repeat(character, 510) + "z"
			keys := []durable.Key{}
			for j := 0; j < 3; j++ {
				key := durable.Key{Namespace: namespace, WorkflowID: id[:511] + fmt.Sprint(j), RunID: id}
				r := durable.StartRequest{Key: key, RequestID: "start", WorkflowType: id, BuildID: id, Queue: namespace}
				if _, err := s.StartExecution(ctx, r); err != nil {
					t.Fatal(err)
				}
				keys = append(keys, key)
				claimed := claim(t, s, r, time.Minute)
				req := completion(r, claimed)
				for k := 0; k < 3; k++ {
					req.Tasks = append(req.Tasks, durable.TaskSpec{ID: id[:511] + fmt.Sprint(k), Kind: durable.TaskActivity, Queue: namespace})
				}
				if _, err := s.CommitTransition(ctx, req); err != nil {
					t.Fatal(err)
				}
			}
			list := durable.ExecutionList{Namespace: namespace, WorkflowType: id, BuildID: id, Limit: 1}
			seen := map[durable.Key]bool{}
			for page := 0; page < 3; page++ {
				rows, next, err := s.ListExecutions(ctx, list)
				if err != nil || len(rows) != 1 || seen[rows[0].Key] {
					t.Fatalf("execution page %d: rows=%d err=%v", page, len(rows), err)
				}
				seen[rows[0].Key] = true
				if (next == "") != (page == 2) {
					t.Fatal("execution continuation coverage")
				}
				list.Cursor = next
			}
			list.WorkflowID, list.Cursor = keys[0].WorkflowID, ""
			if rows, _, err := s.ListExecutions(ctx, list); err != nil || len(rows) != 1 {
				t.Fatal("workflow filter", err)
			}
			facts, err := s.ReadBuildFacts(ctx, namespace, id)
			if err != nil || facts.Executions != 3 {
				t.Fatal("build filter", facts, err)
			}
			taskList := durable.TaskList{Key: keys[0], Kind: durable.TaskActivity, Limit: 1}
			last := ""
			for page := 0; page < 3; page++ {
				rows, next, err := s.ListTasks(ctx, taskList)
				if err != nil || len(rows) != 1 || rows[0].ID <= last {
					t.Fatalf("task page %d: rows=%d err=%v", page, len(rows), err)
				}
				last = rows[0].ID
				if (next == "") != (page == 2) {
					t.Fatal("task continuation coverage")
				}
				taskList.Cursor = next
			}
		})
	}
}
