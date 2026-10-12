package operatorhost

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xraph/dispatch/operator"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	pgstore "github.com/xraph/dispatch/store/postgres"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/memory"
)

func queryAuthorityFixture(t *testing.T, store Store) (*Host, durable.QueryRuntimeBinding) {
	t.Helper()
	h, err := NewWithLifecycle(t.Context(), store, LifecycleOptions{InstanceID: "clock-host", SkipSampleExecutions: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := h.Close(context.Background()); e != nil {
			t.Error(e)
		}
	})
	identity := h.RuntimeIdentities()[0]
	life := store.(durable.LifecycleStore)
	enrollment := store.(durable.RetirementEnrollmentStore)
	queries := store.(durable.QueryRuntimeStore)
	if _, err = enrollment.EnrollRetirement(t.Context(), durable.RetirementEnrollmentRequest{NamespaceTarget: identity.NamespaceTarget, RequestID: "enroll", SchemaVersion: 1, WriterProtocol: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = life.RegisterBuild(t.Context(), durable.RegisterBuildRequest{BuildTarget: identity.BuildTarget, RequestID: "build", Identity: &identity.BuildIdentity}); err != nil {
		t.Fatal(err)
	}
	receipt, err := queries.RegisterQueryRuntime(t.Context(), durable.RegisterQueryRuntimeRequest{Identity: identity, RequestID: "register"})
	if err != nil {
		t.Fatal(err)
	}
	probe := defaultProbes(identity.BuildID)[0]
	if _, err = store.StartExecution(t.Context(), durable.StartRequest{Key: probe.Key, RequestID: "start", WorkflowType: "operator", BuildID: identity.BuildID, Queue: "operator", Input: []byte("history")}); err != nil {
		t.Fatal(err)
	}
	worker := h.runtime.workers[identity.BuildID]
	handle, err := worker.BeginDrainBeforeDeadline(t.Context(), drt.DrainRequest{OperationID: "drain", Deadline: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if result, e := worker.WaitDrain(t.Context(), handle); e != nil || !result.Complete {
		t.Fatalf("drain: %+v %v", result, e)
	}
	return h, *receipt.QueryRuntime
}
func TestMemoryDefaultQueryProofStoreAuthority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(40 * 365 * 24 * time.Hour)
		store := memory.New()
		h, b := queryAuthorityFixture(t, store)
		defer closeQueryAuthority(t, h, store)
		proveQueryAuthority(t, h, b, false)
	})
}
func TestPostgresDefaultQueryProofDespiteHostAhead(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	synctest.Test(t, func(t *testing.T) {
		// Advance before any periodic resource exists. PostgreSQL's clock is unchanged.
		time.Sleep(40 * 365 * 24 * time.Hour)
		driver := pgdriver.New()
		if err := driver.Open(t.Context(), os.Getenv("DISPATCH_OPERATOR_DSN")); err != nil {
			t.Fatal(err)
		}
		db, err := grove.Open(driver)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if e := db.Close(); e != nil {
				t.Error(e)
			}
		}()
		store := pgstore.New(db)
		h, b := queryAuthorityFixture(t, store)
		defer closeQueryAuthority(t, h, store)
		proveQueryAuthority(t, h, b, true)
	})
}
func closeQueryAuthority(t *testing.T, h *Host, store Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Error(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Error("repeated close", err)
	}
	// The fixture never starts its Dispatch engine, so close its owned store here.
	if err := store.Close(); err != nil {
		t.Error(err)
	}
}
func proveQueryAuthority(t *testing.T, h *Host, b durable.QueryRuntimeBinding, hostAhead bool) {
	t.Helper()
	queries := h.Store.(durable.QueryRuntimeStore)
	before, err := queries.InspectQueryRetention(t.Context(), b.BuildTarget)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := h.lifecycle.Verify(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	after, err := queries.InspectQueryRetention(t.Context(), b.BuildTarget)
	if err != nil {
		t.Fatal(err)
	}
	if proof.VerifiedAt.Before(before.ObservedAt) || proof.VerifiedAt.After(after.ObservedAt) {
		t.Fatalf("default proof used another clock: proof=%s store=[%s,%s] host=%s", proof.VerifiedAt, before.ObservedAt, after.ObservedAt, time.Now())
	}
	t.Logf("store authority: before=%s proof=%s after=%s host=%s", before.ObservedAt, proof.VerifiedAt, after.ObservedAt, time.Now())
	if hostAhead && !time.Now().After(proof.ValidUntil.Add(24*time.Hour)) {
		t.Fatal("fixture did not separate host and store clocks")
	}
	accepted, err := queries.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: b.QueryRuntimeTarget, RequestID: "verify", ExpectedVersion: b.Version, Verification: proof})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.QueryRuntime.Verification.VerifiedAt != proof.VerifiedAt {
		t.Fatal("store rewrote proof")
	}
	bad := proof
	bad.VerifiedAt = after.ObservedAt.Add(time.Hour)
	bad.ValidUntil = bad.VerifiedAt.Add(time.Minute)
	_, err = queries.RecordQueryRuntimeVerification(t.Context(), durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: b.QueryRuntimeTarget, RequestID: "invalid-future", ExpectedVersion: accepted.QueryRuntime.Version, Verification: bad})
	d, ok := durable.QueryRejectionDetails(err)
	if !errors.Is(err, durable.ErrQueryRetention) || !ok || d.Reason != "verified_future" {
		t.Fatalf("strict future rule changed: %+v %v", d, err)
	}
}

type querySampleStore struct {
	*memory.Store
	sample func(context.Context, durable.BuildTarget) (durable.QueryRetentionFacts, error)
}

func (s *querySampleStore) InspectQueryRetention(ctx context.Context, target durable.BuildTarget) (durable.QueryRetentionFacts, error) {
	if s.sample != nil {
		return s.sample(ctx, target)
	}
	return s.Store.InspectQueryRetention(ctx, target)
}

func TestQueryClockRefusalKeepsAbortRevoked(t *testing.T) {
	for _, scenario := range []string{"read", "target", "zero", "rollback"} {
		t.Run(scenario, func(t *testing.T) {
			s := &querySampleStore{Store: memory.New()}
			h, b := queryAuthorityFixture(t, s)
			fence := durable.QueryRemovalFence{Candidate: b.QueryRuntimeIdentity, CandidateStateVersion: b.Version, RemovalEpoch: 1}
			b.State = durable.QueryRuntimeRemoving
			b.Removal = fence
			b.RemovalEpoch = 1
			fixed := durable.Timestamp(time.Now())
			calls := 0
			s.sample = func(_ context.Context, target durable.BuildTarget) (durable.QueryRetentionFacts, error) {
				calls++
				facts := durable.QueryRetentionFacts{BuildTarget: target, ObservedAt: fixed}
				switch scenario {
				case "read":
					return facts, errors.New("private store failure")
				case "target":
					facts.BuildID = "wrong"
				case "zero":
					facts.ObservedAt = time.Time{}
				case "rollback":
					if calls == 2 {
						facts.ObservedAt = fixed.Add(-time.Microsecond)
					}
				}
				return facts, nil
			}
			settlement, proof, err := h.lifecycle.VerifyAbort(t.Context(), b, fence)
			digest, _ := durable.Fingerprint("query_runtime.removal_fence.v1", fence)
			if !h.lifecycle.revokedOperations[digest] {
				t.Fatal("clock failure restored issuance")
			}
			if scenario == "rollback" {
				if err != nil {
					t.Fatal(err)
				}
				if !proof.VerifiedAt.Before(settlement.SettledAt) {
					t.Fatal("host clamped rollback")
				}
				// Actual durable validation must reject the preserved ordering, not rewrite it.
				if _, err = durable.AbortQueryRemoval(b, durable.AbortQueryRemovalRequest{VerifyQueryRuntimeRequest: durable.VerifyQueryRuntimeRequest{QueryRuntimeTarget: b.QueryRuntimeTarget, ExpectedVersion: b.Version, Verification: proof}, Fence: fence, Settlement: settlement}, fixed); !errors.Is(err, durable.ErrQueryRetention) {
					t.Fatalf("rollback accepted: %v", err)
				}
			} else {
				d, ok := durable.QueryRejectionDetails(err)
				if !ok || d.Stage != "host_clock" {
					t.Fatalf("clock rejection missing: %+v %v", d, err)
				}
			}
		})
	}
}

func TestQueryDiagnosticCaptureBoundsAndRedaction(t *testing.T) {
	h := &lifecycleHost{}
	secret := "private-query-credential"
	d := durable.QueryRejectionDiagnostic{Stage: "host_probe", Reason: "digest", RequestID: secret, ExpectedDigest: strings.Repeat("a", 64), ActualDigest: strings.Repeat("b", 64)}
	for range queryDiagnosticRecords + 5 {
		h.observeQueryRejection(t.Context(), d)
	}
	records := h.queryDiagnosticSnapshot()
	if len(records) != queryDiagnosticRecords {
		t.Fatal("diagnostic ring is unbounded")
	}
	output := strings.Join(records, "\n") + "\nnot-a-diagnostic " + secret
	safe := nativeQueryDiagnostics(output, map[string]Credential{"test": {Token: secret}})
	if len(safe) != queryDiagnosticRecords || strings.Contains(strings.Join(safe, "\n"), secret) {
		t.Fatal("native diagnostic redaction failed")
	}
	for _, line := range safe {
		if len(line) > queryDiagnosticLimit+len(queryDiagnosticPrefix) {
			t.Fatal("record unbounded")
		}
	}
}

func TestQueryHostRejectionPredicates(t *testing.T) {
	for _, reason := range []string{"identity", "removed", "digest", "query_read"} {
		t.Run(reason, func(t *testing.T) {
			h, b := queryAuthorityFixture(t, memory.New())
			switch reason {
			case "identity":
				b.InstanceID = "different-instance"
			case "removed":
				h.lifecycle.removedRuntimes[b.RuntimeID] = "removed"
			case "digest":
				h.lifecycle.options.Probes[b.BuildID][0].ExpectedDigest = strings.Repeat("a", 64)
			case "query_read":
				h.lifecycle.options.Probes[b.BuildID][0].Key.RunID = "missing-run"
			}
			_, err := h.lifecycle.Verify(t.Context(), b)
			d, ok := durable.QueryRejectionDetails(err)
			if !ok || d.Reason != reason || d.Target != b.QueryRuntimeTarget {
				t.Fatalf("actual predicate missing: %+v %v", d, err)
			}
			if reason == "digest" && (d.ExpectedDigest == "" || d.ActualDigest == "" || d.ExpectedDigest == d.ActualDigest) {
				t.Fatal("digest evidence missing")
			}
		})
	}
}

func TestQueryDiagnosticsPreserveHTTPErrorMapping(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
		cause  error
	}{
		{"query-missing", 404, nil}, {"clock-missing", 404, durable.ErrNotFound}, {"clock-unavailable", 503, errors.New("private-clock-driver-message")},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store := &querySampleStore{Store: memory.New()}
			h, b := queryAuthorityFixture(t, store)
			server := httptest.NewServer(h.Handler)
			t.Cleanup(server.Close)
			c := &commandClient{t: t, host: h, server: server}
			status, raw := c.request(http.MethodGet, "/api/dashboard/v1/csrf", h.Credentials["commander"].Token, nil)
			var csrf struct {
				Token string `json:"token"`
			}
			if status != 200 || json.Unmarshal(raw, &csrf) != nil || csrf.Token == "" {
				t.Fatal("CSRF unavailable")
			}
			c.csrf = csrf.Token
			reason := "clock_read"
			if scenario.cause == nil {
				h.lifecycle.options.Probes[b.BuildID][0].Key.RunID = "missing-run"
				reason = "query_read"
			} else {
				store.sample = func(context.Context, durable.BuildTarget) (durable.QueryRetentionFacts, error) {
					return durable.QueryRetentionFacts{}, scenario.cause
				}
			}
			in := operator.QueryRuntimeCommand{QueryRuntimeInput: operator.QueryRuntimeInput{BuildInput: operator.BuildInput{Namespace: b.Namespace, BuildID: b.BuildID}, RuntimeID: b.RuntimeID}, RequestID: "verify", ExpectedVersion: "1"}
			response := c.command("durable.queryRuntimeVerify", in, scenario.status)
			records := h.lifecycle.queryDiagnosticSnapshot()
			if len(records) != 1 || !strings.Contains(records[0], `"reason":"`+reason+`"`) {
				t.Fatalf("private predicate missing: %v", records)
			}
			for _, private := range []string{"private-clock-driver-message", "host_clock", "host_probe", "clock_read", "query_read"} {
				if strings.Contains(string(response), private) {
					t.Fatal("private diagnostic reached HTTP")
				}
			}
			if strings.Contains(strings.Join(records, "\n"), "private-clock-driver-message") {
				t.Fatal("raw driver message reached diagnostics")
			}
		})
	}
}
