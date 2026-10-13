package memory

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryRemovalGuard(t *testing.T) { durabletest.RunQueryRemovalGuard(t, New()) }

func TestQueryRemovalGuardMaximum(t *testing.T) { durabletest.RunQueryRemovalGuardMaximum(t, New()) }
func TestQueryRemovalGuardExpiry(t *testing.T)  { durabletest.RunQueryRemovalGuardExpiry(t, New()) }
func TestQueryRemovalGuardRaces(t *testing.T)   { durabletest.RunQueryRemovalGuardRaces(t, New()) }

func TestQueryRemovalGuardAuditRollback(t *testing.T) {
	s := New()
	f := durabletest.NewQueryGuardFixture(t, s)
	failure := errors.New("injected guard audit failure")
	snapshot := func() []byte {
		data, err := json.Marshal(struct {
			Guards    int
			Current   int
			Materials int
			Commits   int
			Closes    int
			Outbox    int
		}{len(s.queryRemovalGuards), len(s.queryRemovalCurrent), len(s.queryRemovalMaterials), len(s.queryRemovalCommits), len(s.queryRemovalCloses), len(s.outbox)})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := string(snapshot())
	s.outboxPrepare = func(durable.Delivery) error { return failure }
	if _, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request); !errors.Is(err, failure) {
		t.Fatalf("revalidation rollback: %v", err)
	}
	if string(snapshot()) != before {
		t.Fatal("failed audit published revalidation state")
	}
	s.outboxPrepare = nil
	r, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	before = string(snapshot())
	s.outboxPrepare = func(durable.Delivery) error { return failure }
	if _, err = s.BindQueryRemovalCommit(t.Context(), durabletest.QueryGuardCommitFixture(f, r)); !errors.Is(err, failure) {
		t.Fatalf("bind rollback: %v", err)
	}
	if _, err = s.CloseQueryRemovalGuard(t.Context(), durabletest.QueryGuardCloseFixture(f, r)); !errors.Is(err, failure) {
		t.Fatalf("close rollback: %v", err)
	}
	if string(snapshot()) != before {
		t.Fatal("failed audit published bind or close")
	}
	if s.queryRemovalGuards[queryGuardKey{f.Candidate.Namespace, r.GuardID}].State != "active" {
		t.Fatal("failed close released guard")
	}
}
func TestQueryRemovalGuardPositiveEffect(t *testing.T) {
	durabletest.RunQueryRemovalGuardPositiveEffect(t, New())
}
func TestQueryRemovalGuardRowBounds(t *testing.T) {
	durabletest.RunQueryRemovalGuardRowBounds(t, New())
}

func TestQueryRemovalGuardEncodingEnvelope(t *testing.T) {
	s := New()
	f := durabletest.NewQueryGuardMaximumFixture(t, s)
	f.Request.ExternalScope.ProfileVersion = math.MaxInt64
	r, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)
	commit := durabletest.QueryGuardCommitFixture(f, r)
	commit.RequestID = strings.Repeat("c", 256)
	commit.Permission.PermissionID = strings.Repeat("p", 256)
	commit.Permission.CommandID = strings.Repeat("i", 256)
	commit.Permission.Attestation = strings.Repeat("x", 4094)
	commit.Permission.AcceptedAt = at
	cr := durable.QueryRemovalCommitReceipt{Request: commit, RequestDigest: strings.Repeat("a", 64), ReceiptDigest: strings.Repeat("b", 64), AcceptedAt: at, DeliveryID: strings.Repeat("c", 64)}
	closeRequest := durabletest.QueryGuardCloseFixture(f, r)
	closeRequest.RequestID = strings.Repeat("z", 256)
	closeRequest.Closure.CutoffReceiptID = strings.Repeat("k", 256)
	closeRequest.Closure.Attestation = strings.Repeat("x", 4094)
	closeRequest.Closure.CutoffAt = at
	closeRequest.Closure.TerminalAt = at
	closeRequest.Closure.Disposition = "settled_without_deletion"
	zr := durable.QueryRemovalGuardCloseReceipt{Request: closeRequest, RequestDigest: strings.Repeat("a", 64), ReceiptDigest: strings.Repeat("b", 64), AcceptedAt: at, DeliveryID: strings.Repeat("c", 64)}
	for _, row := range []struct {
		name string
		v    any
	}{{"maximum_commit_request", commit}, {"maximum_commit_receipt", cr}, {"maximum_close_request", closeRequest}, {"maximum_close_receipt", zr}} {
		data, e := json.Marshal(row.v)
		if e != nil {
			t.Fatal(e)
		}
		t.Logf("%s=%d", row.name, len(data))
	}
}

func TestQueryRemovalGuardCorruptAuthority(t *testing.T) {
	for _, damage := range []string{"missing-current", "missing-initial", "corrupt-material", "corrupt-root", "corrupt-current"} {
		t.Run(damage, func(t *testing.T) {
			s := New()
			f := durabletest.NewQueryGuardFixture(t, s)
			r, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
			if err != nil {
				t.Fatal(err)
			}
			key := queryGuardKey{f.Candidate.Namespace, r.GuardID}
			switch damage {
			case "missing-current":
				delete(s.queryRemovalCurrent, key)
			case "missing-initial":
				delete(s.queryRemovalMaterials, queryGuardKey{f.Candidate.Namespace, f.Request.RequestID})
			case "corrupt-material":
				k := queryGuardKey{f.Candidate.Namespace, f.Request.RequestID}
				m := s.queryRemovalMaterials[k]
				m.Request.Evidence.CandidateAttestation = "changed"
				s.queryRemovalMaterials[k] = m
			case "corrupt-root":
				g := s.queryRemovalGuards[key]
				g.RootDigest = strings.Repeat("f", 64)
				s.queryRemovalGuards[key] = g
			case "corrupt-current":
				c := s.queryRemovalCurrent[key]
				c.MaterialDigest = strings.Repeat("f", 64)
				s.queryRemovalCurrent[key] = c
			}
			outbox := len(s.outbox)
			if _, err = s.BindQueryRemovalCommit(t.Context(), durabletest.QueryGuardCommitFixture(f, r)); err == nil {
				t.Fatal("corrupt authority allowed bind")
			}
			if _, err = s.CloseQueryRemovalGuard(t.Context(), durabletest.QueryGuardCloseFixture(f, r)); err == nil {
				t.Fatal("corrupt authority allowed closure")
			}
			fresh := f.Request.Clone()
			fresh.RequestID = "refresh"
			fresh.ExpectedCurrentDigest = r.MaterialDigest
			fresh.Evidence.ObservedAt = durable.Timestamp(time.Now())
			if _, err = s.RevalidateQueryRuntimeRemoval(t.Context(), fresh); err == nil {
				t.Fatal("corrupt authority synthesized repair")
			}
			if len(s.queryRemovalCommits) != 0 || len(s.queryRemovalCloses) != 0 || len(s.outbox) != outbox {
				t.Fatal("corrupt authority refusal partially wrote state")
			}
		})
	}
}
