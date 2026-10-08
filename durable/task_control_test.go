package durable_test

import (
	"errors"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestTimeoutFieldsPreserveLegacyCommitFingerprint(t *testing.T) {
	request := durable.CommitRequest{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}, RequestID: "commit", ExpectedRevision: 1, Token: durable.TaskToken{TaskID: "a", Owner: "owner", Epoch: 1}, Events: []durable.EventInput{{Type: "progress"}}, Tasks: []durable.TaskSpec{{ID: "next", Kind: durable.TaskActivity, Queue: "q"}}, TaskUpdate: &durable.TaskUpdate{Action: durable.TaskKeep}}
	digest, err := durable.Fingerprint("commit", request)
	// Captured from b7dfb90 before timeout fields existed.
	if err != nil || digest != "5278bf2db885ba8e61f5e3e92799484b9138713f9f0f25525423aac1d5846ad3" {
		t.Fatalf("legacy request receipt cannot be retried after upgrade: %s %v", digest, err)
	}
}

func TestTimeoutLeaseRequiresElapsedDeadline(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	task := durable.Task{TaskSpec: durable.TaskSpec{ID: "activity"}, Owner: "coordinator", Epoch: 2,
		LeaseKind: durable.LeaseTimeout, LeaseUntil: now.Add(time.Hour), DeadlineAt: now.Add(time.Second)}
	if err := durable.CheckLease(task, task.Token(), now); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("timeout grant accepted a future deadline after clock rollback: %v", err)
	}
	task.DeadlineAt = time.Time{}
	if err := durable.CheckLease(task, task.Token(), now); !errors.Is(err, durable.ErrTaskConflict) {
		t.Fatalf("timeout grant accepted a missing deadline: %v", err)
	}
}
