package durable_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
)

func TestReceiptValidation(t *testing.T) {
	key := durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}
	good := strings.Repeat("ab", 32)
	query := durable.ReceiptRequest{Key: key, RequestID: "request", IntentDigest: good}
	if err := query.Validate(); err != nil {
		t.Fatal(err)
	}
	commit := durable.CommitRequest{Key: key, RequestID: "request", ExpectedRevision: 1,
		Token: durable.TaskToken{TaskID: "task", Owner: "worker", Epoch: 1}, Events: []durable.EventInput{{Type: "done"}}, IntentDigest: good}
	if err := commit.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, digest := range []string{"", "bad", strings.Repeat("AB", 32), strings.Repeat("ff", 31), strings.Repeat("ff", 33), strings.Repeat("zz", 32)} {
		query.IntentDigest, commit.IntentDigest = digest, digest
		if err := query.Validate(); !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid lookup digest accepted: %v", err)
		}
		if err := commit.Validate(); digest != "" && !errors.Is(err, durable.ErrInvalid) {
			t.Fatalf("invalid commit digest accepted: %v", err)
		} else if digest == "" && err != nil {
			t.Fatalf("legacy commit rejected: %v", err)
		}
	}
	query.IntentDigest, query.RequestID = good, " "
	if err := query.Validate(); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("invalid request ID accepted: %v", err)
	}
	query.RequestID, query.Namespace = "request", ""
	if err := query.Validate(); !errors.Is(err, durable.ErrInvalid) {
		t.Fatalf("missing namespace accepted: %v", err)
	}
	if err := durable.CheckReceiptIntent(good, good); err != nil {
		t.Fatal(err)
	}
	for _, stored := range []string{"", strings.Repeat("cd", 32)} {
		if err := durable.CheckReceiptIntent(stored, good); !errors.Is(err, durable.ErrRequestConflict) {
			t.Fatalf("unbound intent matched: %v", err)
		}
	}
}
