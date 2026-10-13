package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

func TestQueryRemovalGuard(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRemovalGuard(t, s)
}

func TestQueryRemovalGuardMaximum(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRemovalGuardMaximum(t, s)
}
func TestQueryRemovalGuardExpiry(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRemovalGuardExpiry(t, s)
}
func TestQueryRemovalGuardRaces(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRemovalGuardRaces(t, s)
}

func TestQueryRemovalGuardLockWaitExpiry(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	f := durabletest.NewQueryGuardExpiringFixture(t, s)
	gate, observe := retirementConn(t, dsn), retirementConn(t, dsn)
	tx, err := gate.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, f.Candidate.Namespace); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, e := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request); result <- e }()
	queryWaitNamespace(t, observe, f.Candidate.Namespace)
	waitProtocolProof(t, f.Request.Evidence.ValidUntil)
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err == nil {
		t.Fatal("expired waiter accepted")
	}
	var count int
	if err = observe.QueryRow(t.Context(), `SELECT count(*) FROM dispatch_query_removal_guards WHERE namespace=$1`, f.Candidate.Namespace).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired waiter wrote guard: %d %v", count, err)
	}
}

type guardReopenProof struct {
	Fixture        durabletest.QueryGuardFixture
	MaterialDigest string
	Commit         *durable.QueryRemovalCommitReceipt
	Close          *durable.QueryRemovalGuardCloseReceipt
}

func TestQueryRemovalGuardProcessReopen(t *testing.T) {
	if input := os.Getenv("DISPATCH_GUARD_REOPEN_INPUT"); input != "" {
		data, err := os.ReadFile(input)
		if err != nil {
			t.Fatal(err)
		}
		var want guardReopenProof
		if err = json.Unmarshal(data, &want); err != nil {
			t.Fatal(err)
		}
		s := openWakeStore(t, os.Getenv("DISPATCH_LIFECYCLE_TEST_DSN"))
		f := want.Fixture
		m, err := s.LookupQueryRemovalRevalidation(t.Context(), durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: f.Request.RequestID, CommandDigest: f.Request.CommandDigest})
		if err != nil || m.MaterialDigest != want.MaterialDigest {
			t.Fatalf("fresh-process material recovery: %v", err)
		}
		if want.Commit != nil {
			r, e := s.BindQueryRemovalCommit(t.Context(), want.Commit.Request)
			if e != nil || !reflect.DeepEqual(r, *want.Commit) {
				t.Fatalf("fresh-process bind replay: %v", e)
			}
		}
		if want.Close != nil {
			r, e := s.CloseQueryRemovalGuard(t.Context(), want.Close.Request)
			if e != nil || !reflect.DeepEqual(r, *want.Close) {
				t.Fatalf("fresh-process close replay: %v", e)
			}
		} else {
			if _, err = s.BeginQueryRuntimeRemoval(t.Context(), durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "reopen-invalidator", ExpectedVersion: f.Survivor.Version}); err == nil {
				t.Fatal("fresh process lost pin")
			}
		}
		return
	}
	s, _, _, _ := retirementFixture(t)
	f := durabletest.RunQueryRemovalGuardMaximum(t, s)
	r, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	proof := guardReopenProof{Fixture: f, MaterialDigest: r.MaterialDigest}
	run := func() {
		t.Helper()
		data, e := json.Marshal(proof)
		if e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(t.TempDir(), "reopen.json")
		if e = os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestQueryRemovalGuardProcessReopen$", "-test.v")
		cmd.Env = append(os.Environ(), "DISPATCH_GUARD_REOPEN_INPUT="+path)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("fresh OS process: %v\n%s", e, out)
		}
		t.Log("fresh OS process recovered native records")
	}
	run() // Native revalidation accepted, response lost to the external producer.
	commit, err := s.BindQueryRemovalCommit(t.Context(), durabletest.QueryGuardCommitFixture(f, r))
	if err != nil {
		t.Fatal(err)
	}
	proof.Commit = &commit
	run() // A native bind is retained without an external marker.
	closed, err := s.CloseQueryRemovalGuard(t.Context(), durabletest.QueryGuardCloseFixture(f, r))
	if err != nil {
		t.Fatal(err)
	}
	proof.Close = &closed
	run()
}

func TestQueryRemovalGuardSQLMaterialCap(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	f := durabletest.RunQueryRemovalGuardMaximum(t, s)
	m, err := s.LookupQueryRemovalRevalidation(t.Context(), durable.QueryRemovalRevalidationLookup{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: f.Request.RequestID, CommandDigest: f.Request.CommandDigest})
	if err != nil {
		t.Fatal(err)
	}
	c := retirementConn(t, dsn)
	var stored int
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.QueryRow(t.Context(), `SELECT octet_length(data::text) FROM dispatch_query_removal_receipts WHERE namespace=$1 AND request_id=$2`, f.Candidate.Namespace, f.Request.RequestID).Scan(&stored); err != nil || stored != len(data) {
		t.Fatalf("stored JSON size %d, expected %d: %v", stored, len(data), err)
	}
	m.Request.RequestID = "sql-over-cap"
	m.Request.Evidence.CandidateAttestation = ""
	data, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	m.Request.Evidence.CandidateAttestation = strings.Repeat("x", durable.QueryRemovalMaterialLimit+1-len(data))
	data, err = json.Marshal(m)
	if err != nil || len(data) != durable.QueryRemovalMaterialLimit+1 {
		t.Fatal("overflow fixture is not one byte over cap")
	}
	tx, err := c.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, f.Candidate.Namespace); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(), `INSERT INTO dispatch_query_removal_receipts(namespace,request_id,guard_id,candidate_id,survivor_id,build_id,removal_epoch,material_digest,delivery_id,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, f.Candidate.Namespace, m.Request.RequestID, m.GuardID, m.Candidate.RuntimeID, m.Survivor.RuntimeID, m.Build.BuildID, m.RemovalEpoch, m.MaterialDigest, m.DeliveryID, data)
	retirementSQLState(t, err, "23514")
}

func TestQueryRemovalGuardOldExecutable(t *testing.T) {
	binary := os.Getenv("DISPATCH_QUERYPREGUARD_BINARY")
	if binary == "" {
		if os.Getenv("DISPATCH_QUERY_REQUIRED") == "1" {
			t.Fatal("DISPATCH_QUERYPREGUARD_BINARY required")
		}
		t.Skip("published query-capable pre-guard executable required")
	}
	s, dsn, _, _ := retirementFixture(t)
	f := durabletest.NewQueryGuardFixture(t, s)
	// A third live binding makes the old survivor Begin otherwise admissible.
	identity := f.Survivor.QueryRuntimeIdentity
	identity.RuntimeID = "third"
	identity.InstanceID = "third-instance"
	if _, err := s.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: "third"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: identity.QueryRuntimeTarget, RequestID: "third-proof", ExpectedVersion: 1, Verification: durabletest.QueryProofFixture(identity, "third-proof")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request); err != nil {
		t.Fatal(err)
	}
	type input struct {
		Operation string
		Begin     durable.BeginQueryRemovalRequest
		Abort     durable.AbortQueryRemovalRequest
		Finish    durable.FinishQueryRemovalRequest
		Build     durable.BuildRetirementRequest
		Verify    durable.VerifyQueryRuntimeRequest
		Lookup    durable.LifecycleReceiptLookup
		Target    durable.QueryRuntimeTarget
	}
	call := func(in input, wantFailure bool) json.RawMessage {
		t.Helper()
		data, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), binary)
		cmd.Env = append(os.Environ(), "DISPATCH_QUERYPREGUARD_DSN="+dsn)
		cmd.Stdin = bytes.NewReader(data)
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("old executable: %v", err)
		}
		var r struct {
			Value    json.RawMessage
			Failed   bool
			SQLState string
		}
		if err = json.Unmarshal(output, &r); err != nil || r.Failed != wantFailure {
			t.Fatalf("old %s: failed=%v state=%s err=%v", in.Operation, r.Failed, r.SQLState, err)
		}
		return r.Value
	}
	call(input{Operation: "begin", Begin: durable.BeginQueryRemovalRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "old-begin", ExpectedVersion: f.Survivor.Version}}, true)
	call(input{Operation: "finish", Finish: durable.FinishQueryRemovalRequest{Fence: f.Begin.QueryRuntime.Removal, RequestID: "old-finish"}}, true)
	call(input{Operation: "resume", Build: durable.BuildRetirementRequest{BuildTarget: f.Candidate.BuildTarget, RequestID: "old-resume", ExpectedVersion: 2, ExpectedEpoch: 2}}, true)
	settlement := durabletest.QuerySettlementFixture(t, f.Begin.QueryRuntime.Removal)
	call(input{Operation: "abort", Abort: durable.AbortQueryRemovalRequest{Fence: f.Begin.QueryRuntime.Removal, Settlement: settlement, VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: f.Candidate.QueryRuntimeTarget, RequestID: "old-abort", ExpectedVersion: f.Candidate.Version, Verification: durabletest.QueryProofFixture(f.Candidate.QueryRuntimeIdentity, "old-abort")}}}, true)
	for _, op := range []string{"delete-runtime", "update-runtime", "delete-build", "update-build"} {
		call(input{Operation: op, Target: f.Candidate.QueryRuntimeTarget}, true)
	}
	call(input{Operation: "delete-runtime", Target: f.Survivor.QueryRuntimeTarget}, true)
	// Same-identity proof refresh remains compatible through the unchanged library.
	call(input{Operation: "verify", Verify: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: f.Survivor.QueryRuntimeTarget, RequestID: "old-refresh", ExpectedVersion: f.Survivor.Version, Verification: durabletest.QueryProofFixture(f.Survivor.QueryRuntimeIdentity, "old-refresh")}}, false)
	raw := call(input{Operation: "lookup", Lookup: durable.LifecycleReceiptLookup{NamespaceTarget: f.Candidate.NamespaceTarget, Operation: durable.OperationBeginQueryRemoval, RequestID: f.Begin.RequestID, RequestDigest: f.Begin.RequestDigest}}, false)
	var original durable.LifecycleReceipt
	if err := json.Unmarshal(raw, &original); err != nil || !reflect.DeepEqual(original, f.Begin) {
		t.Fatal("old lookup changed original bytes")
	}
}

func TestQueryRemovalGuardAtomicRollback(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	c := retirementConn(t, dsn)
	for _, table := range []string{"dispatch_durable_outbox", "dispatch_query_removal_guards", "dispatch_query_removal_receipts", "dispatch_query_removal_current", "dispatch_query_removal_commits", "dispatch_query_removal_closes"} {
		t.Run(table, func(t *testing.T) {
			f := durabletest.NewQueryGuardFixture(t, s)
			var accepted durable.QueryRemovalRevalidationReceipt
			if table == "dispatch_query_removal_commits" || table == "dispatch_query_removal_closes" {
				var err error
				accepted, err = s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
				if err != nil {
					t.Fatal(err)
				}
			}
			count := func() int {
				t.Helper()
				var n int
				if err := c.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM dispatch_query_removal_guards WHERE namespace=$1)+(SELECT count(*) FROM dispatch_query_removal_current WHERE namespace=$1)+(SELECT count(*) FROM dispatch_query_removal_receipts WHERE namespace=$1)+(SELECT count(*) FROM dispatch_query_removal_commits WHERE namespace=$1)+(SELECT count(*) FROM dispatch_query_removal_closes WHERE namespace=$1)+(SELECT count(*) FROM dispatch_durable_outbox WHERE namespace=$1)`, f.Candidate.Namespace).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			before := count()
			// Table comes only from this finite test list; the trigger fails after its row write.
			if _, err := c.Exec(t.Context(), `CREATE FUNCTION dispatch_guard_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected guard write failure'; END $$; CREATE TRIGGER dispatch_guard_test_fail AFTER INSERT OR UPDATE ON `+table+` FOR EACH ROW EXECUTE FUNCTION dispatch_guard_test_fail()`); err != nil {
				t.Fatal(err)
			}
			var err error
			switch table {
			case "dispatch_query_removal_commits":
				_, err = s.BindQueryRemovalCommit(t.Context(), durabletest.QueryGuardCommitFixture(f, accepted))
			case "dispatch_query_removal_closes":
				_, err = s.CloseQueryRemovalGuard(t.Context(), durabletest.QueryGuardCloseFixture(f, accepted))
			default:
				_, err = s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
			}
			if _, dropErr := c.Exec(t.Context(), `DROP TRIGGER dispatch_guard_test_fail ON `+table+`; DROP FUNCTION dispatch_guard_test_fail()`); dropErr != nil {
				t.Fatal(dropErr)
			}
			if err == nil {
				t.Fatal("injected failure accepted")
			}
			if after := count(); after != before {
				t.Fatalf("partial transaction: before=%d after=%d", before, after)
			}
			if table == "dispatch_query_removal_closes" {
				var state string
				if err = c.QueryRow(t.Context(), `SELECT state FROM dispatch_query_removal_guards WHERE namespace=$1 AND guard_id=$2`, f.Candidate.Namespace, accepted.GuardID).Scan(&state); err != nil || state != "active" {
					t.Fatalf("failed close released pins: %s %v", state, err)
				}
			}
		})
	}
}
func TestQueryRemovalGuardPositiveEffect(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRemovalGuardPositiveEffect(t, s)
}
func TestQueryRemovalGuardRowBounds(t *testing.T) {
	s, _, _, _ := retirementFixture(t)
	durabletest.RunQueryRemovalGuardRowBounds(t, s)
}

func TestQueryRemovalGuardSQLCommitCap(t *testing.T) {
	s, dsn, _, _ := retirementFixture(t)
	f := durabletest.NewQueryGuardFixture(t, s)
	r, err := s.RevalidateQueryRuntimeRemoval(t.Context(), f.Request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.BindQueryRemovalCommit(t.Context(), durabletest.QueryGuardCommitFixture(f, r))
	if err != nil {
		t.Fatal(err)
	}
	receipt.Request.RequestID = "sql-commit-cap"
	receipt.Request.Permission.Attestation = ""
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Request.Permission.Attestation = strings.Repeat("x", durable.QueryRemovalCommitReceiptLimit+1-len(data))
	data, err = json.Marshal(receipt)
	if err != nil || len(data) != durable.QueryRemovalCommitReceiptLimit+1 {
		t.Fatal("commit overflow fixture is not cap+1")
	}
	if receipt.Seal() == nil {
		t.Fatal("receipt codec accepted cap+1")
	}
	c := retirementConn(t, dsn)
	tx, err := c.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_retirement_coordinator_lock($1,1)`, f.Candidate.Namespace); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(t.Context(), `INSERT INTO dispatch_query_removal_commits(namespace,request_id,guard_id,delivery_id,data) VALUES($1,$2,$3,$4,$5)`, f.Candidate.Namespace, receipt.Request.RequestID, receipt.Request.Guard.GuardID, receipt.DeliveryID, data)
	retirementSQLState(t, err, "23514")
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = c.QueryRow(t.Context(), `SELECT count(*) FROM dispatch_query_removal_commits WHERE namespace=$1`, f.Candidate.Namespace).Scan(&count); err != nil || count != 1 {
		t.Fatalf("oversize commit changed saved rows: %d %v", count, err)
	}
}
