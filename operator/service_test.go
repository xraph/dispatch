package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
	"github.com/xraph/dispatch/store/memory"
)

func fixture(t *testing.T) (*Service, *memory.Store, *map[string]bool) {
	t.Helper()
	store := memory.New()
	ctx := t.Context()
	for _, ns := range []string{"audit", "allowed", "foreign"} {
		_, err := store.RegisterNamespace(ctx, durable.NamespaceConfig{InstallationID: "install", Namespace: ns, AppID: "app-" + ns, TenantID: "tenant-" + ns, RequireAudit: true, RequireHooks: true, SchemaVersion: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	audit := security.Boundary{Resource: security.Resource{InstallationID: "install", PolicyTenant: "tenant-audit"}, Audit: &security.AuditService{}}
	if err := audit.Audit.Activate(ctx, store, store, audit.Resource, "audit", false); err != nil {
		t.Fatal(err)
	}
	grants := map[string]bool{"allowed": true}
	service, err := New(Options{Store: store, Reads: store, Catalog: store, InstallationID: "install", Audit: audit, CursorKeys: CursorKeys{Active: "v1", Keys: map[string][]byte{"v1": make([]byte, 32)}}, Authorizer: AuthorizerFunc(func(_ context.Context, p security.Principal, action string, r Resource) error {
		if r.InstallationID != "install" || r.AppID != "app-"+r.Namespace || r.TenantID != "tenant-"+r.Namespace {
			t.Errorf("untrusted ownership: %+v", r)
		}
		if grants[r.Namespace] && p.Subject == "reader" && action != ReadPayload && r.RunID != "hidden" {
			return nil
		}
		return security.ErrForbidden
	})})
	if err != nil {
		t.Fatal(err)
	}
	return service, store, &grants
}
func reader() security.Principal { return security.Principal{Subject: "reader", Kind: "user"} }
func seed(t *testing.T, store durable.Store, ns, wf string) durable.Key {
	t.Helper()
	key := durable.Key{Namespace: ns, WorkflowID: wf, RunID: "run"}
	if _, err := store.StartExecution(t.Context(), durable.StartRequest{Key: key, RequestID: "start", WorkflowType: "workflow", BuildID: "historic", Queue: "queue", Input: []byte("PAYLOAD_SECRET")}); err != nil {
		t.Fatal(err)
	}
	return key
}
func TestDiscoveryBudgetConfidentialityAndRevocation(t *testing.T) {
	s, store, grants := fixture(t)
	for i := 0; i < 40; i++ {
		ns := fmt.Sprintf("000-denied-%02d", i)
		if _, err := store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: "install", Namespace: ns, AppID: "app-" + ns, TenantID: "tenant-" + ns, RequireAudit: true, SchemaVersion: 1}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.Namespaces(t.Context(), reader(), NamespaceInput{Limit: 1})
	if err != nil || page.Complete || page.Total != nil || len(page.Items) != 0 || page.Cursor == "" {
		t.Fatalf("%+v %v", page, err)
	}
	_, encoded, _ := strings.Cut(page.Cursor, ".")
	decoded, _ := base64.RawURLEncoding.DecodeString(encoded)
	if strings.Contains(string(decoded), "denied") {
		t.Fatal("readable denied key")
	}
	for _, p := range []security.Principal{{Subject: "other", Kind: "user"}, {Subject: "reader", Kind: "service"}} {
		if _, err = s.Namespaces(t.Context(), p, NamespaceInput{Limit: 1, Cursor: page.Cursor}); !errors.Is(err, durable.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err = s.Namespaces(t.Context(), reader(), NamespaceInput{Limit: 2, Cursor: page.Cursor}); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal("changed filter", err)
	}
	page, err = s.Namespaces(t.Context(), reader(), NamespaceInput{Limit: 1, Cursor: page.Cursor})
	if err != nil || len(page.Items) != 1 || page.Items[0].Namespace != "allowed" || page.Complete {
		t.Fatalf("%+v %v", page, err)
	}
	(*grants)["allowed"] = false
	if _, err = s.Namespaces(t.Context(), reader(), NamespaceInput{Limit: 1, Cursor: page.Cursor}); !errors.Is(err, security.ErrForbidden) {
		t.Fatal("revocation", err)
	}
}
func TestSafeMetadataHistoryAndSensitiveAudit(t *testing.T) {
	s, store, _ := fixture(t)
	key := seed(t, store, "allowed", "workflow")
	seed(t, store, "foreign", "other")
	for i := 0; i < 3; i++ {
		if _, err := store.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: fmt.Sprintf("signal%d", i), Name: "signal", BuildID: "historic", Input: []byte("EVENT_SECRET")}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.History(t.Context(), reader(), RunInput{Key: key, Limit: 2})
	if err != nil || first.Complete || first.HighWater != "4" {
		t.Fatalf("%+v %v", first, err)
	}
	if _, err = store.SignalExecution(t.Context(), durable.SignalRequest{Key: key, RequestID: "late", Name: "signal", BuildID: "historic", Input: []byte("LATE_SECRET")}); err != nil {
		t.Fatal(err)
	}
	last, err := s.History(t.Context(), reader(), RunInput{Key: key, Limit: 2, Cursor: first.Cursor})
	if err != nil || !last.Complete || last.HighWater != "4" || len(last.Items) != 2 {
		t.Fatalf("%+v %v", last, err)
	}
	foreign := key
	foreign.Namespace = "foreign"
	if _, err = s.History(t.Context(), reader(), RunInput{Key: foreign, Limit: 2, Cursor: first.Cursor}); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	detail, err := s.Detail(t.Context(), reader(), key)
	if err != nil || detail.Payload != "restricted" || detail.Runtime != "unavailable" {
		t.Fatalf("%+v %v", detail, err)
	}
	tasks, err := s.Tasks(t.Context(), reader(), durable.TaskList{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := s.Deliveries(t.Context(), reader(), DeliveryInput{Key: key, Destination: durable.DestinationChronicle})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []any{detail, tasks, first, last, deliveries} {
		b, _ := json.Marshal(v)
		for _, marker := range []string{"PAYLOAD_SECRET", "EVENT_SECRET", "LATE_SECRET", "fingerprint", "intent_digest", "async", "owner", "receipt"} {
			if strings.Contains(string(b), marker) {
				t.Fatalf("metadata leak %s", b)
			}
		}
	}
	if _, err = s.Payload(t.Context(), reader(), key); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	s.authorizer = AuthorizerFunc(func(context.Context, security.Principal, string, Resource) error { return nil })
	reveal, err := s.Payload(t.Context(), reader(), key)
	if err != nil || string(reveal.Input) != "PAYLOAD_SECRET" {
		t.Fatalf("%+v %v", reveal, err)
	}
	status, err := store.ReadDeliveryStatus(t.Context(), durable.ScopedDeliveryStatus{Key: durable.Key{Namespace: "allowed"}, DeliveryStatusRequest: durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "install", Destination: durable.DestinationChronicle}, Limit: 100}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range status.Records {
		found = found || (d.Delivery.Action == ReadPayload && d.Delivery.Outcome == "allowed")
	}
	if !found {
		t.Fatal("sensitive read audit absent")
	}
	s.audit.Audit.Deactivate()
	if _, err = s.Payload(t.Context(), reader(), key); !errors.Is(err, security.ErrUnavailable) {
		t.Fatal("audit failure allowed", err)
	}
}
func TestCursorTamperExpiryAndPreciseCounters(t *testing.T) {
	s, _, _ := fixture(t)
	state := cursorState{Position: "denied-secret"}
	token, err := s.cursors.seal(state, "binding")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.cursors.open(token+"x", "binding"); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal(err)
	}
	a := s.cursors.keys["v1"]
	raw, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "v1."))
	plain, err := a.Open(nil, raw[:a.NonceSize()], raw[a.NonceSize():], []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(plain, &state); err != nil {
		t.Fatal(err)
	}
	state.Expires = time.Now().Add(-time.Minute).Unix()
	plain, _ = json.Marshal(state)
	expired := "v1." + base64.RawURLEncoding.EncodeToString(a.Seal(raw[:a.NonceSize()], raw[:a.NonceSize()], plain, []byte("v1")))
	if _, err = s.cursors.open(expired, "binding"); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal(err)
	}
	b, _ := json.Marshal(s.project(durable.Execution{Revision: 9007199254740993, LastSequence: 9007199254740994, RunNumber: 9007199254740995, RetryAttempt: 9007199254740996, Input: []byte("secret")}))
	for _, field := range []string{`"revision":"9007199254740993"`, `"last_sequence":"9007199254740994"`, `"run_number":"9007199254740995"`, `"retry_attempt":"9007199254740996"`} {
		if !strings.Contains(string(b), field) {
			t.Fatal(string(b))
		}
	}
}
func TestUnknownIdentityActionNamespaceAndPolicyFailClosed(t *testing.T) {
	s, store, _ := fixture(t)
	key := seed(t, store, "allowed", "workflow")
	if _, err := s.Detail(t.Context(), security.Principal{}, key); !errors.Is(err, security.ErrUnauthenticated) {
		t.Fatal(err)
	}
	if err := s.check(t.Context(), reader(), "unknown", key); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	missing := key
	missing.Namespace = "missing"
	if _, err := s.Detail(t.Context(), reader(), missing); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	s.authorizer = AuthorizerFunc(func(context.Context, security.Principal, string, Resource) error {
		return errors.New("PROVIDER_SECRET")
	})
	if _, err := s.Detail(t.Context(), reader(), key); err == nil || strings.Contains(err.Error(), "PROVIDER_SECRET") {
		t.Fatal(err)
	}
}

type linkedStore struct {
	durable.Store
	key durable.Key
}

func (s linkedStore) GetExecution(ctx context.Context, key durable.Key) (durable.Execution, error) {
	e, err := s.Store.GetExecution(ctx, key)
	if err == nil && key == s.key {
		e.FirstRunID = "hidden"
		e.NextRunID = "hidden"
	}
	return e, err
}
func (s linkedStore) ListChildExecutions(_ context.Context, _ durable.Key, _ string, _ int) ([]durable.ChildExecution, error) {
	return []durable.ChildExecution{{CurrentKey: durable.Key{Namespace: s.key.Namespace, WorkflowID: s.key.WorkflowID, RunID: "hidden"}, ChildStartSpec: durable.ChildStartSpec{CommandID: "DENIED_CHILD_ID"}}}, nil
}

type secretTasks struct{ durable.ReadStore }

func (s secretTasks) ListTasks(_ context.Context, r durable.TaskList) ([]durable.Task, string, error) {
	return []durable.Task{{Key: r.Key, TaskSpec: durable.TaskSpec{ID: "activity", Kind: durable.TaskActivity, Payload: []byte("TASK_PAYLOAD_SECRET")}, Owner: "LEASE_PROOF_SECRET", Epoch: 99, Version: 9007199254740993, Attempt: 9007199254740994, AsyncKeyHash: "ASYNC_HASH_SECRET", Progress: []byte("PROGRESS_SECRET")}}, "", nil
}
func TestLinkedTargetsAndTaskSecrets(t *testing.T) {
	s, store, _ := fixture(t)
	key := seed(t, store, "allowed", "workflow")
	s.store = linkedStore{Store: store, key: key}
	s.reads = secretTasks{ReadStore: store}
	detail, err := s.Detail(t.Context(), reader(), key)
	if err != nil || len(detail.Links) != 0 || !detail.LinksRestricted {
		t.Fatalf("%+v %v", detail, err)
	}
	chain, err := s.Chain(t.Context(), reader(), RunInput{Key: key})
	if err != nil || !chain.Restricted || len(chain.Items) != 1 {
		t.Fatalf("%+v %v", chain, err)
	}
	children, err := s.Children(t.Context(), reader(), RunInput{Key: key})
	if err != nil || !children.Restricted || len(children.Items) != 0 {
		t.Fatalf("%+v %v", children, err)
	}
	tasks, err := s.Tasks(t.Context(), reader(), durable.TaskList{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []any{detail, chain, children, tasks} {
		b, _ := json.Marshal(v)
		for _, marker := range []string{"hidden", "DENIED_CHILD_ID", "TASK_PAYLOAD_SECRET", "LEASE_PROOF_SECRET", "ASYNC_HASH_SECRET", "PROGRESS_SECRET"} {
			if strings.Contains(string(b), marker) {
				t.Fatalf("leak %s", b)
			}
		}
	}
	if tasks.Items[0].Attempt != "9007199254740994" || tasks.Items[0].Version != "9007199254740993" {
		t.Fatal(tasks)
	}
}
func TestExecutionCursorScopeAndGrantChanges(t *testing.T) {
	s, store, grants := fixture(t)
	seed(t, store, "allowed", "a")
	seed(t, store, "allowed", "b")
	first, err := s.Executions(t.Context(), reader(), durable.ExecutionList{Namespace: "allowed", Limit: 1})
	if err != nil || first.Cursor == "" {
		t.Fatal(first, err)
	}
	if _, err = s.Executions(t.Context(), reader(), durable.ExecutionList{Namespace: "allowed", Limit: 1, BuildID: "historic", Cursor: first.Cursor}); !errors.Is(err, durable.ErrInvalid) {
		t.Fatal(err)
	}
	(*grants)["allowed"] = false
	if _, err = s.Executions(t.Context(), reader(), durable.ExecutionList{Namespace: "allowed", Limit: 1, Cursor: first.Cursor}); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
	if _, err = s.Deliveries(t.Context(), reader(), DeliveryInput{Key: durable.Key{Namespace: "foreign"}, Destination: durable.DestinationChronicle}); !errors.Is(err, security.ErrForbidden) {
		t.Fatal(err)
	}
}
