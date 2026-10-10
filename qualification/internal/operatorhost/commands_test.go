package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aid "github.com/xraph/authsome/id"
	"github.com/xraph/authsome/principal"
	"github.com/xraph/authsome/serviceaccount"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"

	"github.com/xraph/dispatch/qualification/internal/authority"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/store/memory"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

type commandClient struct {
	t      *testing.T
	host   *Host
	server *httptest.Server
	csrf   string
}

func newCommandClient(t *testing.T, store Store) *commandClient {
	t.Helper()
	return newConfiguredCommandClient(t, store, nil)
}

func newConfiguredCommandClient(t *testing.T, store Store, options *LifecycleOptions) *commandClient {
	t.Helper()
	h, err := newHost(t.Context(), store, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(h.Handler)
	t.Cleanup(server.Close)
	c := &commandClient{t: t, host: h, server: server}
	status, raw := c.request(http.MethodGet, "/api/dashboard/v1/csrf", h.Credentials["commander"].Token, nil)
	var result struct {
		Token string `json:"token"`
	}
	if status != 200 || json.Unmarshal(raw, &result) != nil || result.Token == "" {
		t.Fatalf("CSRF status %d", status)
	}
	c.csrf = result.Token
	return c
}
func (c *commandClient) request(method, path, token string, raw []byte) (int, []byte) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.server.URL+path, bytes.NewReader(raw))
	if err != nil {
		c.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	if bytes.Contains(body, []byte(c.host.Machine.Secret)) {
		c.t.Fatal("machine key disclosed")
	}
	return response.StatusCode, body
}
func (c *commandClient) envelope(intent string, payload any) []byte {
	c.t.Helper()
	kind := "command"
	if intent == "durable.query" || intent == "durable.capabilities" || intent == "durable.compatibility" || intent == "durable.build" || intent == "durable.workerStatus" || intent == "durable.workerDrainReceipt" || intent == "durable.queryRuntime" || intent == "durable.queryRuntimeRemovalCheck" {
		kind = "query"
	}
	raw, err := json.Marshal(map[string]any{"envelope": "v1", "kind": kind, "contributor": "dispatch", "intent": intent, "intentVersion": 1, "idempotencyKey": "transport-" + intent, "csrf": c.csrf, "params": map[string]any{"namespace": "foreign"}, "payload": payload})
	if err != nil {
		c.t.Fatal(err)
	}
	return raw
}
func (c *commandClient) command(intent string, payload any, want int) []byte {
	c.t.Helper()
	status, raw := c.request(http.MethodPost, "/api/dashboard/v1", c.host.Credentials["commander"].Token, c.envelope(intent, payload))
	if status != want {
		c.t.Fatalf("%s HTTP %d want %d: %s", intent, status, want, raw)
	}
	return raw
}
func commandStart(t *testing.T) operator.StartInput {
	return operator.StartInput{Key: durable.Key{Namespace: "production", WorkflowID: t.Name(), RunID: "run"}, RequestID: "start", WorkflowType: "operator", BuildID: "operator-v1", Queue: "operator", Input: []byte(`{"large":9007199254740993}`)}
}
func postgresCommands(t *testing.T) Store {
	t.Helper()
	dsn := os.Getenv("DISPATCH_OPERATOR_DSN")
	if dsn == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("dedicated PostgreSQL required")
		}
		t.Skip("dedicated PostgreSQL required")
	}
	driver := pgdriver.New()
	if err := driver.Open(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	db, err := grove.Open(driver)
	if err != nil {
		t.Fatal(err)
	}
	return pgstore.New(db)
}
func TestMemoryCommands(t *testing.T)   { testCommands(t, memory.New()) }
func TestPostgresCommands(t *testing.T) { testCommands(t, postgresCommands(t)) }
func testCommands(t *testing.T, store Store) {
	c := newCommandClient(t, store)
	start := commandStart(t)
	first := data[operator.Acceptance](t, c.command("durable.start", start, 200))
	if again := data[operator.Acceptance](t, c.command("durable.start", start, 200)); again != first {
		t.Fatal("start receipt changed")
	}
	changed := start
	changed.Input = []byte("changed")
	c.command("durable.start", changed, 409)
	query := drt.QueryRequest{Key: start.Key, BuildID: start.BuildID, Name: "status"}
	result := data[operator.QueryResult](t, c.command("durable.query", query, 200))
	if result.Key != start.Key || !bytes.Equal(result.Output, start.Input) {
		t.Fatal("query bytes changed")
	}
	query.Name = "mutate"
	c.command("durable.query", query, 400)
	query.Name = "status"
	query.BuildID = "operator-v2"
	c.command("durable.query", query, 409)
	signal := durable.SignalRequest{Key: start.Key, BuildID: start.BuildID, RequestID: "signal", Name: "message", Input: start.Input}
	receipt := data[operator.Acceptance](t, c.command("durable.signal", signal, 200))
	if again := data[operator.Acceptance](t, c.command("durable.signal", signal, 200)); again != receipt {
		t.Fatal("signal receipt changed")
	}
	signal.Input = []byte("changed")
	c.command("durable.signal", signal, 409)
	signal.Input = start.Input
	signal.RunID = ""
	c.command("durable.signal", signal, 400)
	signal.Key = start.Key
	signal.Namespace = "foreign"
	c.command("durable.signal", signal, 403)
	signal.Key = start.Key
	sw := operator.SignalStartInput{Start: start, Name: "message", Input: start.Input}
	sw.Start.RequestID = "signal-start"
	sw.Start.RunID = "proposed"
	existing := data[operator.Acceptance](t, c.command("durable.signalStart", sw, 200))
	if existing.Key != start.Key || existing.Started {
		t.Fatal("wrong atomic existing branch")
	}
	sw.Start.WorkflowID += "-new"
	fresh := data[operator.Acceptance](t, c.command("durable.signalStart", sw, 200))
	if !fresh.Started {
		t.Fatal("missing new branch")
	}
	cancel := durable.CancelExecutionRequest{Key: start.Key, BuildID: start.BuildID, RequestID: "cancel"}
	cancelled := data[operator.Acceptance](t, c.command("durable.cancel", cancel, 200))
	execution, err := store.GetExecution(t.Context(), start.Key)
	if err != nil || cancelled.Status != "cancellation_requested" || execution.State != durable.StateRunning {
		t.Fatal("cancel incorrectly terminal")
	}
	before := execution.Revision
	if err = c.host.Policies.DeletePolicy(t.Context(), "tenant-production", c.host.CommandPolicies[operator.SignalWorkflow]); err != nil {
		t.Fatal(err)
	}
	c.command("durable.signal", signal, 403)
	c.command("durable.signalStart", sw, 403)
	execution, err = store.GetExecution(t.Context(), start.Key)
	if err != nil || execution.Revision != before {
		t.Fatal("denied replay mutated")
	}
	unavailable := start
	unavailable.WorkflowID += "-missing"
	unavailable.BuildID = "historical-v1"
	c.command("durable.start", unavailable, 503)
	wrongType := start
	wrongType.WorkflowID += "-wrong-type"
	wrongType.WorkflowType = "unregistered"
	c.command("durable.start", wrongType, 403)
	oversize := start
	oversize.WorkflowID += "-oversized"
	oversize.Input = bytes.Repeat([]byte("x"), (1<<20)+1)
	c.command("durable.start", oversize, 413)
	// Every accepted command has durable local intents even without a publisher.
	status, err := store.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: durable.DeliveryScope{InstallationID: "operator-host", Destination: durable.DestinationChronicle}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range status.Records {
		if record.Delivery.WorkflowID == start.WorkflowID {
			found = true
			if record.Delivery.Metadata.ActorID != c.host.Credentials["commander"].Subject {
				t.Fatal("command actor missing")
			}
		}
	}
	if !found {
		t.Fatal("required local audit missing")
	}
}
func callbackHandle(h drt.AsyncActivityHandle) operator.CallbackHandle {
	return operator.CallbackHandle{Version: h.Version, Key: h.Key, BuildID: h.BuildID, Secret: h.Secret, InitialHeartbeatSequence: h.InitialHeartbeatSequence, Token: operator.CallbackToken{TaskID: h.Token.TaskID, Owner: h.Token.Owner, Epoch: h.Token.Epoch, LeaseKind: h.Token.LeaseKind}}
}
func (c *commandClient) handoff(workflow string) operator.CallbackHandle {
	c.t.Helper()
	start := commandStart(c.t)
	start.WorkflowType = workflow
	start.BuildID = "operator-v2"
	c.command("durable.start", start, 200)
	worker := c.host.runtime.workers[start.BuildID]
	for _, kind := range []durable.TaskKind{durable.TaskWorkflow, durable.TaskActivity} {
		worked, err := worker.RunOnce(c.t.Context(), kind)
		if err != nil || !worked {
			c.t.Fatalf("handoff %s failed: %v", kind, err)
		}
	}
	c.host.runtime.mu.Lock()
	defer c.host.runtime.mu.Unlock()
	handles := c.host.runtime.handles[start.Key]
	if len(handles) != 1 {
		c.t.Fatal("genuine handoff absent")
	}
	return callbackHandle(handles[0])
}
func (c *commandClient) callback(path, token string, input any, want int) operator.Acceptance {
	c.t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		c.t.Fatal(err)
	}
	status, body := c.request(http.MethodPost, "/v1/durable/activities/"+path, token, raw)
	if status != want {
		c.t.Fatalf("callback %s status %d want %d", path, status, want)
	}
	var secret string
	switch in := input.(type) {
	case operator.CompletionInput:
		secret = in.Handle.Secret
	case operator.HeartbeatInput:
		secret = in.Handle.Secret
	}
	if secret != "" && bytes.Contains(body, []byte(secret)) {
		c.t.Fatal("callback proof disclosed")
	}
	var out operator.Acceptance
	if want == 200 {
		if err = json.Unmarshal(body, &out); err != nil {
			c.t.Fatal(err)
		}
	}
	return out
}
func TestMemoryMachineCallbacks(t *testing.T)   { testMachineCallbacks(t, memory.New()) }
func TestPostgresMachineCallbacks(t *testing.T) { testMachineCallbacks(t, postgresCommands(t)) }
func testMachineCallbacks(t *testing.T, store Store) {
	c := newCommandClient(t, store)
	handle := c.handoff("async")
	token := c.host.Machine.Secret
	status, metadata := call(t, c.server.URL, c.host.Credentials["reader"].Token, "durable.execution", handle.Key, false)
	if status != 200 || bytes.Contains(metadata, []byte(handle.Secret)) {
		t.Fatal("metadata exposed callback proof")
	}
	heartbeat := operator.HeartbeatInput{Handle: handle, RequestID: "heartbeat", Sequence: handle.InitialHeartbeatSequence + 1, Details: []byte("private-progress")}
	beat := c.callback("heartbeat", token, heartbeat, 200)
	if again := c.callback("heartbeat", token, heartbeat, 200); again != beat {
		t.Fatal("heartbeat replay changed")
	}
	heartbeat.Details = []byte("changed")
	c.callback("heartbeat", token, heartbeat, 409)
	completion := operator.CompletionInput{Handle: handle, RequestID: "complete", Output: []byte(`{"large":9007199254740993}`)}
	wrong := completion
	wrong.Handle.Secret = strings.Repeat("a", 64)
	c.callback("complete", token, wrong, 409)
	wrong = completion
	wrong.Handle.Token.Epoch++
	c.callback("complete", token, wrong, 409)
	first := c.callback("complete", token, completion, 200)
	if again := c.callback("complete", token, completion, 200); again != first {
		t.Fatal("completion replay changed")
	}
	completion.Output = []byte("changed")
	c.callback("complete", token, completion, 409)
	completion.Output = []byte(`{"large":9007199254740993}`)
	if err := c.host.Policies.DeletePolicy(t.Context(), "tenant-production", c.host.CommandPolicies[operator.CompleteActivity]); err != nil {
		t.Fatal(err)
	}
	c.callback("complete", token, completion, 403)
	if err := c.host.Policies.DeletePolicy(t.Context(), "tenant-production", c.host.CommandPolicies[operator.HeartbeatActivity]); err != nil {
		t.Fatal(err)
	}
	heartbeat.Details = []byte("private-progress")
	c.callback("heartbeat", token, heartbeat, 403)
}

func TestCallbackCredentialIsolation(t *testing.T) {
	for _, scenario := range []string{"missing", "forged", "human", "revoked-key", "expired-key", "revoked-account", "malformed", "numeric-counter", "foreign", "wrong-build", "unavailable-build", "denied-heartbeat"} {
		t.Run(scenario, func(t *testing.T) {
			c := newCommandClient(t, memory.New())
			handle := c.handoff("async")
			input := operator.CompletionInput{Handle: handle, RequestID: "complete", Output: []byte("private-result")}
			token := c.host.Machine.Secret
			want := 401
			before, err := c.host.Store.GetExecution(t.Context(), handle.Key)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing":
				token = ""
			case "forged":
				token = "forged-credential"
			case "human":
				token = c.host.Credentials["commander"].Token
				want = 403
			case "revoked-key", "expired-key":
				c.callback("complete", token, input, 200)
				before, err = c.host.Store.GetExecution(t.Context(), handle.Key)
				if err != nil {
					t.Fatal(err)
				}
				key, keyErr := c.host.Auth.APIKeyStore().GetAPIKey(t.Context(), c.host.Machine.KeyID)
				if keyErr != nil {
					t.Fatal(keyErr)
				}
				if scenario == "revoked-key" {
					key.Revoked = true
				} else {
					expired := time.Now().Add(-time.Second)
					key.ExpiresAt = &expired
				}
				if err = c.host.Auth.APIKeyStore().UpdateAPIKey(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			case "revoked-account":
				c.callback("complete", token, input, 200)
				before, err = c.host.Store.GetExecution(t.Context(), handle.Key)
				if err != nil {
					t.Fatal(err)
				}
				if err = c.host.Auth.DeleteServiceAccount(t.Context(), c.host.Machine.AccountID); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				status, _ := c.request(http.MethodPost, "/v1/durable/activities/complete", token, []byte(`{"handle":{"secret":"private malformed"},"request_id":`))
				if status != 400 {
					t.Fatal(status)
				}
				return
			case "numeric-counter":
				raw, marshalErr := json.Marshal(input)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				var wire map[string]any
				if err = json.Unmarshal(raw, &wire); err != nil {
					t.Fatal(err)
				}
				wire["handle"].(map[string]any)["token"].(map[string]any)["epoch"] = 1
				c.callback("complete", token, wire, 400)
				after, getErr := c.host.Store.GetExecution(t.Context(), handle.Key)
				if getErr != nil || after.Revision != before.Revision {
					t.Fatal("malformed callback mutated")
				}
				return
			case "foreign":
				input.Handle.Key.Namespace = "foreign"
				want = 403
			case "wrong-build":
				input.Handle.BuildID = "operator-v1"
				want = 409
			case "unavailable-build":
				// Preserve the genuine handle but remove only fixture code availability.
				delete(c.host.runtime.workers, "operator-v2")
				want = 503
			case "denied-heartbeat":
				if err = c.host.Policies.DeletePolicy(t.Context(), "tenant-production", c.host.CommandPolicies[operator.HeartbeatActivity]); err != nil {
					t.Fatal(err)
				}
				c.callback("heartbeat", token, operator.HeartbeatInput{Handle: handle, RequestID: "heartbeat", Sequence: 1}, 403)
				return
			}
			c.callback("complete", token, input, want)
			after, err := c.host.Store.GetExecution(t.Context(), handle.Key)
			if err != nil || after.Revision != before.Revision {
				t.Fatal("denied callback mutated")
			}
		})
	}
}
func TestCallbackGenuineExpiryAndRotatedAttempt(t *testing.T) {
	for _, scenario := range []string{"expiry", "retry"} {
		t.Run(scenario, func(t *testing.T) {
			c := newCommandClient(t, memory.New())
			workflow := "async"
			if scenario == "expiry" {
				workflow = "async-expiry"
			}
			handle := c.handoff(workflow)
			token := c.host.Machine.Secret
			input := operator.CompletionInput{Handle: handle, RequestID: "complete", Output: []byte("done")}
			if scenario == "expiry" {
				time.Sleep(300 * time.Millisecond)
				c.callback("complete", token, input, 409)
				c.callback("heartbeat", token, operator.HeartbeatInput{Handle: handle, RequestID: "expired-heartbeat", Sequence: 1}, 409)
				return
			}
			failure := operator.CompletionInput{Handle: handle, RequestID: "failed", Failure: &drt.ApplicationError{Type: drt.FailureWorkerLost, Message: "retry fixture"}}
			accepted := c.callback("complete", token, failure, 200)
			task, err := c.host.Store.GetTask(t.Context(), handle.Key, handle.Token.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if wait := time.Until(task.AvailableAt); wait > 0 {
				time.Sleep(wait + time.Millisecond)
			}
			worked, err := c.host.runtime.workers[handle.BuildID].RunOnce(t.Context(), durable.TaskActivity)
			if err != nil || !worked {
				t.Fatal("retry did not execute", err)
			}
			c.host.runtime.mu.Lock()
			handles := c.host.runtime.handles[handle.Key]
			c.host.runtime.mu.Unlock()
			if len(handles) != 2 || handles[1].Secret == handle.Secret || handles[1].Token.Epoch <= handle.Token.Epoch {
				t.Fatal("authority did not rotate")
			}
			if again := c.callback("complete", token, failure, 200); again != accepted {
				t.Fatal("accepted failure recovery changed")
			}
			c.callback("complete", token, input, 409)
			input.Handle = callbackHandle(handles[1])
			c.callback("complete", token, input, 200)
		})
	}
}

func TestSeededNonhumanKindsUseRealIssuerAndProvider(t *testing.T) {
	for _, kind := range []principal.Kind{principal.KindAgent, principal.KindWorkload} {
		t.Run(string(kind), func(t *testing.T) {
			c := newCommandClient(t, memory.New())
			handle := c.handoff("async")
			base, err := c.host.Auth.GetServiceAccount(t.Context(), c.host.Machine.AccountID)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			account := &serviceaccount.ServiceAccount{ID: aid.NewServiceAccountID(), AppID: base.AppID, EnvID: base.EnvID, Kind: kind, Name: "rejected-kind", Scopes: base.Scopes, Active: true, CreatedAt: now, UpdatedAt: now}
			if err = c.host.authStore.CreateServiceAccount(t.Context(), account); err != nil {
				t.Fatal(err)
			}
			key, secret, err := c.host.Auth.CreateServiceAccountAPIKey(t.Context(), account.ID, "rejected-kind", account.Scopes, nil)
			if err != nil {
				t.Fatalf("genuine issuer refused fixture kind: %v", err)
			}
			c.host.machineProvider.Credentials = append(c.host.machineProvider.Credentials, authority.Credential{KeyID: key.ID, AccountID: account.ID})
			strategy, ok := c.host.Auth.Strategies().Get("apikey")
			if !ok {
				t.Fatal("missing real strategy")
			}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1/callback", nil)
			req.Header.Set("Authorization", "Bearer "+secret)
			req.Header.Set("X-App-ID", base.AppID.String())
			authenticated, err := strategy.Authenticate(t.Context(), req)
			if err != nil || authenticated == nil || authenticated.Session == nil || authenticated.Session.PrincipalKind != kind {
				t.Fatal("real strategy did not preserve issued kind")
			}
			c.callback("complete", secret, operator.CompletionInput{Handle: handle, RequestID: "rejected", Output: []byte("private")}, 401)
			t.Log("fixture-seeded original kind; engine-issued key; real strategy preserved kind; service-account-only provider refused")
		})
	}
}

type selectorRaceStore struct {
	Store
	afterResolve func()
}

func (s *selectorRaceStore) ResolveExecution(ctx context.Context, target durable.ExecutionTarget) (durable.Execution, error) {
	execution, err := s.Store.ResolveExecution(ctx, target)
	if err == nil && target.Selection != durable.RunExplicit && s.afterResolve != nil {
		s.afterResolve()
	}
	return execution, err
}
func (s *selectorRaceStore) SignalWithStartOutcome(ctx context.Context, request durable.SignalWithStartRequest) (durable.SignalStartOutcome, error) {
	capability, ok := s.Store.(durable.SignalStartOutcomeStore)
	if !ok {
		return durable.SignalStartOutcome{}, errors.New("fixture outcome unavailable")
	}
	return capability.SignalWithStartOutcome(ctx, request)
}
func TestHTTPContinuationSelectionAndReceiptAuthority(t *testing.T) {
	for _, selection := range []durable.RunSelection{durable.RunCurrent, durable.RunLatest} {
		t.Run(string(selection), func(t *testing.T) {
			store := &selectorRaceStore{Store: memory.New()}
			c := newCommandClient(t, store)
			start := commandStart(t)
			start.WorkflowType = "continue"
			in := operator.SignalStartInput{Start: start, Name: "signal"}
			first := data[operator.Acceptance](t, c.command("durable.signalStart", in, 200))
			var once sync.Once
			store.afterResolve = func() {
				once.Do(func() {
					worked, err := c.host.runtime.workers[start.BuildID].RunOnce(t.Context(), durable.TaskWorkflow)
					if err != nil || !worked {
						t.Fatal("continuation failed", err)
					}
				})
			}
			out := data[operator.QueryResult](t, c.command("durable.query", drt.QueryRequest{Key: durable.Key{Namespace: start.Namespace, WorkflowID: start.WorkflowID}, Selection: selection, BuildID: start.BuildID, Name: "status"}, 200))
			if out.Key != start.Key || !bytes.Equal(out.Output, start.Input) {
				t.Fatal("query retargeted after selection")
			}
			successor, err := store.Store.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: durable.Key{Namespace: start.Namespace, WorkflowID: start.WorkflowID}, Selection: durable.RunCurrent})
			if err != nil || successor.RunID == start.RunID {
				t.Fatal("successor absent")
			}
			if worked, runErr := c.host.runtime.workers[start.BuildID].RunOnce(t.Context(), durable.TaskWorkflow); runErr != nil || !worked {
				t.Fatal("successor did not run")
			}
			current, err := store.Store.ResolveExecution(t.Context(), durable.ExecutionTarget{Key: durable.Key{Namespace: start.Namespace, WorkflowID: start.WorkflowID}, Selection: durable.RunCurrent})
			if err != nil || current.Key != successor.Key {
				t.Fatal("fixture continued more than once")
			}
			if replay := data[operator.Acceptance](t, c.command("durable.signalStart", in, 200)); replay != first {
				t.Fatal("recovered receipt retargeted")
			}
			record, err := store.GetNamespace(t.Context(), "operator-host", "production")
			if err != nil {
				t.Fatal(err)
			}
			deny := &policy.Policy{ID: wid.NewPolicyID(), AppID: record.AppID, TenantID: record.TenantID, NamespacePath: record.Namespace, Name: "deny-original", Effect: policy.EffectDeny, IsActive: true, Subjects: []policy.SubjectMatch{{Kind: "user", ID: c.host.Credentials["commander"].Subject}}, Actions: []string{operator.SignalWorkflow, operator.SignalStartWorkflow}, Resources: []string{"dispatch_namespace:production"}, Conditions: []policy.Condition{{Field: "resource.run_id", Operator: policy.OpEquals, Value: start.RunID}}}
			if err = c.host.Policies.CreatePolicy(t.Context(), deny); err != nil {
				t.Fatal(err)
			}
			old, err := store.GetExecution(t.Context(), start.Key)
			if err != nil {
				t.Fatal(err)
			}
			c.command("durable.signalStart", in, 403)
			after, err := store.GetExecution(t.Context(), start.Key)
			if err != nil || after.Revision != old.Revision {
				t.Fatal("denied recovered receipt mutated")
			}
		})
	}
}

func TestHTTPResponseLostAfterCommit(t *testing.T) {
	for _, callback := range []bool{false, true} {
		t.Run(map[bool]string{false: "signal", true: "callback"}[callback], func(t *testing.T) {
			c := newCommandClient(t, memory.New())
			start := commandStart(t)
			path := "/api/dashboard/v1"
			token := c.host.Credentials["commander"].Token
			var raw []byte
			if callback {
				handle := c.handoff("async")
				start.Key = handle.Key
				path = "/v1/durable/activities/complete"
				token = c.host.Machine.Secret
				var err error
				raw, err = json.Marshal(operator.CompletionInput{Handle: handle, RequestID: "lost-complete", Output: []byte("private")})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				c.command("durable.start", start, 200)
				raw = c.envelope("durable.signal", durable.SignalRequest{Key: start.Key, BuildID: start.BuildID, RequestID: "lost-signal", Name: "message", Input: start.Input})
			}
			original := c.host.Handler
			var suppress sync.Once
			var committed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				lose := false
				suppress.Do(func() { lose = true })
				if !lose {
					original.ServeHTTP(w, r)
					return
				}
				record := httptest.NewRecorder()
				original.ServeHTTP(record, r)
				if record.Code != 200 {
					t.Errorf("suppressed response status %d", record.Code)
				}
				committed.Store(true)
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					t.Error("no HTTP hijacker")
					return
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			}))
			defer server.Close()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+path, bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if response != nil {
				_ = response.Body.Close()
			}
			if err == nil || !committed.Load() {
				t.Fatal("did not lose committed HTTP response")
			}
			before, err := c.host.Store.GetExecution(t.Context(), start.Key)
			if err != nil {
				t.Fatal(err)
			}
			status, _ := c.request(http.MethodPost, path, token, raw)
			if status != 200 {
				t.Fatal("identical unknown-response recovery failed", status)
			}
			after, err := c.host.Store.GetExecution(t.Context(), start.Key)
			if err != nil || after.Revision != before.Revision || after.LastSequence != before.LastSequence {
				t.Fatal("retry duplicated persisted mutation")
			}
		})
	}
}

func TestEveryHumanCommandGrantAndWorkflowWideSignalStart(t *testing.T) {
	for _, action := range []string{operator.StartWorkflow, operator.SignalWorkflow, operator.SignalStartWorkflow, operator.CancelWorkflow, operator.QueryWorkflow} {
		t.Run(action, func(t *testing.T) {
			c := newCommandClient(t, memory.New())
			start := commandStart(t)
			c.command("durable.start", start, 200)
			if err := c.host.Policies.DeletePolicy(t.Context(), "tenant-production", c.host.CommandPolicies[action]); err != nil {
				t.Fatal(err)
			}
			switch action {
			case operator.StartWorkflow:
				c.command("durable.start", start, 403)
			case operator.SignalWorkflow:
				c.command("durable.signal", durable.SignalRequest{Key: start.Key, BuildID: start.BuildID, RequestID: "signal", Name: "signal"}, 403)
			case operator.SignalStartWorkflow:
				c.command("durable.signalStart", operator.SignalStartInput{Start: start, Name: "signal"}, 403)
			case operator.CancelWorkflow:
				c.command("durable.cancel", durable.CancelExecutionRequest{Key: start.Key, BuildID: start.BuildID, RequestID: "cancel"}, 403)
			case operator.QueryWorkflow:
				c.command("durable.query", drt.QueryRequest{Key: start.Key, BuildID: start.BuildID, Name: "status"}, 403)
			}
			// Both atomic branches must reject a missing operation grant.
			if action == operator.StartWorkflow || action == operator.SignalWorkflow || action == operator.SignalStartWorkflow {
				in := operator.SignalStartInput{Start: start, Name: "signal"}
				in.Start.RequestID = "atomic-denied"
				c.command("durable.signalStart", in, 403)
				in.Start.WorkflowID += "-new"
				c.command("durable.signalStart", in, 403)
			}
		})
	}
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "run-only-new", true: "run-only-existing"}[existing], func(t *testing.T) {
			c := newCommandClient(t, memory.New())
			start := commandStart(t)
			if existing {
				c.command("durable.start", start, 200)
			}
			p, err := c.host.Policies.GetPolicy(t.Context(), "tenant-production", c.host.CommandPolicies[operator.SignalWorkflow])
			if err != nil {
				t.Fatal(err)
			}
			p.Conditions = append(p.Conditions, policy.Condition{Field: "resource.run_id", Operator: policy.OpEquals, Value: start.RunID})
			if err = c.host.Policies.UpdatePolicy(t.Context(), p); err != nil {
				t.Fatal(err)
			}
			start.RequestID = "run-only"
			c.command("durable.signalStart", operator.SignalStartInput{Start: start, Name: "signal"}, 403)
		})
	}
}

func TestLegacyHTTPDefaultCacheReauthorizes(t *testing.T) {
	c := newCommandClient(t, memory.New())
	j, err := c.host.engine.EnqueueRaw(t.Context(), "fixture", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"id": j.ID.String()}
	first := c.command("jobs.cancel", input, 200)
	again := c.command("jobs.cancel", input, 200)
	var a, b struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(first, &a) != nil || json.Unmarshal(again, &b) != nil || !bytes.Equal(a.Data, b.Data) {
		t.Fatal("legacy dedup changed reply")
	}
	if err = c.host.Policies.DeletePolicy(t.Context(), "tenant-audit", c.host.LegacyPolicy); err != nil {
		t.Fatal(err)
	}
	c.command("jobs.cancel", input, 403)
}

func TestPostgresRequiredIntentFailureRollsBackHTTP(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			store := postgresCommands(t)
			c := newCommandClient(t, store)
			start := commandStart(t)
			if existing {
				c.command("durable.start", start, 200)
			}
			pg := pgdriver.Unwrap(store.(*pgstore.Store).DB())
			ctx := t.Context()
			outboxCount := func() int {
				var count int
				if err := pg.QueryRow(ctx, `SELECT count(*) FROM dispatch_durable_outbox WHERE namespace=$1 AND workflow_id=$2`, start.Namespace, start.WorkflowID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				return count
			}
			beforeOutbox := outboxCount()
			if _, err := pg.Exec(ctx, `CREATE FUNCTION operator_reject_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_kind='signal_receipt' AND NEW.workflow_id LIKE 'TestPostgresRequiredIntentFailureRollsBackHTTP/%' THEN RAISE EXCEPTION 'private fixture persistence error'; END IF; RETURN NEW; END $$; CREATE TRIGGER operator_reject_receipt BEFORE INSERT ON dispatch_durable_outbox FOR EACH ROW EXECUTE FUNCTION operator_reject_receipt()`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := pg.Exec(context.Background(), `DROP TRIGGER IF EXISTS operator_reject_receipt ON dispatch_durable_outbox; DROP FUNCTION IF EXISTS operator_reject_receipt()`); err != nil {
					t.Error(err)
				}
			})
			in := operator.SignalStartInput{Start: start, Name: "signal"}
			in.Start.RequestID = "atomic-start"
			raw := c.command("durable.signalStart", in, 503)
			if bytes.Contains(raw, []byte("private")) {
				t.Fatal("persistence details leaked")
			}
			if outboxCount() != beforeOutbox {
				t.Fatal("failed acceptance saved outbox intents")
			}
			var receipts int
			if err := pg.QueryRow(ctx, `SELECT count(*) FROM dispatch_signal_receipts WHERE namespace=$1 AND workflow_id=$2`, start.Namespace, start.WorkflowID).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatal("failed acceptance saved receipt")
			}
			execution, err := store.GetExecution(ctx, start.Key)
			if existing {
				if err != nil || execution.Revision != 1 || execution.LastSequence != 1 {
					t.Fatal("existing acceptance mutated")
				}
			} else if !errors.Is(err, durable.ErrNotFound) {
				t.Fatal("failed start persisted")
			}
			if _, err = pg.Exec(ctx, `DROP TRIGGER operator_reject_receipt ON dispatch_durable_outbox; DROP FUNCTION operator_reject_receipt()`); err != nil {
				t.Fatal(err)
			}
			c.command("durable.signalStart", in, 200)
		})
	}
}

func TestCallbackMountExcludesLegacyRoutes(t *testing.T) {
	c := newCommandClient(t, memory.New())
	for _, path := range []string{"/v1/jobs/not-a-job/cancel", "/v1/workflows/not-a-workflow/cancel"} {
		status, _ := c.request(http.MethodPost, path, c.host.Credentials["commander"].Token, []byte(`{}`))
		if status != http.StatusNotFound {
			t.Fatalf("unexpected legacy route %s status %d", path, status)
		}
	}
	status, _ := c.request(http.MethodGet, "/v1/stats", c.host.Credentials["commander"].Token, nil)
	if status != http.StatusNotFound {
		t.Fatalf("unexpected legacy stats route status %d", status)
	}
}
