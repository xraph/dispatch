package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	ca "github.com/xraph/chronicle/acceptance"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func auditEventually(t *testing.T, label string, check func() bool) {
	t.Helper()
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal(label)
}

func auditCount(t *testing.T, db *pgx.Conn, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(t.Context(), query, args...).Scan(&count); err != nil {
		t.Fatal("audit observation unavailable")
	}
	return count
}

func denyAuditRead(t *testing.T, c *commandClient) {
	t.Helper()
	status, _ := call(t, c.server.URL, c.host.Credentials["denied"].Token, "durable.executions", map[string]any{"namespace": "production"}, false)
	if status != http.StatusForbidden {
		t.Fatalf("denied read status=%d", status)
	}
}

func readAuditSource(t *testing.T, db *pgx.Conn, pending bool) durable.Delivery {
	t.Helper()
	var raw []byte
	query := `SELECT envelope FROM dispatch_durable_outbox WHERE namespace='audit' AND destination='chronicle'`
	if pending {
		query += ` AND delivered_at IS NULL`
	}
	query += ` ORDER BY accepted_at DESC LIMIT 1`
	if err := db.QueryRow(t.Context(), query).Scan(&raw); err != nil {
		t.Fatal("audit source unavailable")
	}
	var source durable.Delivery
	if json.Unmarshal(raw, &source) != nil || source.Verify() != nil {
		t.Fatal("audit source invalid")
	}
	return source
}

func verifyAuditReceipt(t *testing.T, profile sinkhost.Config, db map[string]*pgx.Conn, source durable.Delivery) {
	t.Helper()
	var raw, accepted []byte
	if err := db["dispatch"].QueryRow(t.Context(), `SELECT receipt FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NOT NULL`, source.ID).Scan(&raw); err != nil {
		t.Fatal("source acknowledgement missing")
	}
	var receipt durable.SinkReceipt
	if json.Unmarshal(raw, &receipt) != nil || receipt.Verify(source) != nil {
		t.Fatal("source receipt invalid")
	}
	request, err := ecosystem.ChronicleRequest(profile.Binding, source)
	if err != nil {
		t.Fatal("source mapping failed")
	}
	fingerprint, err := ca.Fingerprint(request)
	if err != nil || receipt.SinkFingerprint != fingerprint {
		t.Fatal("mapped fingerprint mismatch")
	}
	if err = db["chronicle"].QueryRow(t.Context(), `SELECT receipt FROM chronicle_acceptances WHERE receipt->>'source_key'=$1`, source.ID).Scan(&accepted); err != nil {
		t.Fatal("native acceptance missing")
	}
	var persisted, evidence ca.Receipt
	if json.Unmarshal(accepted, &persisted) != nil || json.Unmarshal([]byte(receipt.Evidence), &evidence) != nil {
		t.Fatal("native evidence invalid")
	}
	want, _ := json.Marshal(persisted)
	got, _ := json.Marshal(evidence)
	if !bytes.Equal(want, got) || auditCount(t, db["chronicle"], `SELECT count(*) FROM chronicle_acceptances WHERE receipt->>'source_key'=$1`, source.ID) != 1 {
		t.Fatal("native receipt differs or duplicates")
	}
}

// legacyChronicleSink reproduces the old fixed binding until the regression has
// observed its failure. Recovery then uses the explicitly provisioned native routes.
type legacyChronicleSink struct {
	mu   sync.RWMutex
	sink delivery.Sink
}

func (s *legacyChronicleSink) Accept(ctx context.Context, d durable.Delivery) (durable.SinkReceipt, error) {
	s.mu.RLock()
	sink := s.sink
	s.mu.RUnlock()
	return sink.Accept(ctx, d)
}

func TestNativeAuditProductionOnlyMappingFailure(t *testing.T) {
	profiles, db := lifecycleChronicleProfiles(t)
	profiles.start(t)
	storeDB, err := sinkhost.Open(t.Context(), profiles.configs["production"].DSNs["dispatch"])
	if err != nil {
		t.Fatal(err)
	}
	c := newConfiguredCommandClient(t, pgstore.New(storeDB), &LifecycleOptions{InstanceID: "legacy-audit-mapping", SkipSampleExecutions: true}, profiles.supervisor)
	router := chronicleRouter{}
	for _, route := range profiles.routes.Routes {
		remote, remoteErr := ecosystem.NewRemote("http://"+route.Address+"/accept", route.Credential.Secret, time.Second, true)
		if remoteErr != nil {
			t.Fatal(remoteErr)
		}
		t.Cleanup(remote.Close)
		b := route.Binding
		router[chronicleTuple{b.InstallationID, b.Namespace, b.AppID, b.TenantID}] = ecosystem.Chronicle{Binding: b, Client: remote}
	}
	b := profiles.configs["production"].Binding
	legacy := &legacyChronicleSink{sink: router[chronicleTuple{b.InstallationID, b.Namespace, b.AppID, b.TenantID}]}
	publisher, err := delivery.New(c.host.Store, delivery.Config{InstallationID: "operator-host", Owner: "legacy-audit-reproduction", PollInterval: 25 * time.Millisecond, CallTimeout: time.Second, StoreTimeout: time.Second, LeaseDuration: 5 * time.Second, RetryMin: 50 * time.Millisecond, RetryMax: 100 * time.Millisecond}, map[durable.Destination]delivery.Sink{durable.DestinationChronicle: legacy})
	if err != nil {
		t.Fatal(err)
	}
	if err = publisher.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		legacy.mu.Lock()
		legacy.sink = router
		legacy.mu.Unlock()
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if stopErr := publisher.Stop(cleanup); stopErr != nil {
			t.Error("legacy regression cleanup incomplete")
		}
	}()
	denyAuditRead(t, c)
	source := readAuditSource(t, db["dispatch"], true)
	auditEventually(t, "production-only binding did not retain attempted denial audit", func() bool {
		return auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND attempts>0 AND delivered_at IS NULL AND error_category='unavailable'`, source.ID) == 1
	})
	if _, mapErr := ecosystem.ChronicleRequest(b, source); !errors.Is(mapErr, durable.ErrInvalid) {
		t.Fatal("legacy mapping failure not established")
	}
	if auditCount(t, db["chronicle"], `SELECT count(*) FROM chronicle_acceptances WHERE receipt->>'source_key'=$1`, source.ID) != 0 {
		t.Fatal("legacy audit unexpectedly accepted")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err = publisher.Stop(deadline)
	cancel()
	if !errors.Is(err, delivery.ErrIncompleteShutdown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("legacy drain did not report incomplete deadline")
	}
	// The sink processes are still serving. The failure occurred before transport.
	for _, profile := range profiles.configs {
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+profile.Addresses["chronicle"]+"/health", nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		response, responseErr := (&http.Client{Timeout: time.Second}).Do(request)
		if responseErr != nil {
			t.Fatal("native sink stopped during mapping reproduction")
		}
		_ = response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatal("native sink unhealthy")
		}
	}
	t.Log("production-only mapping independently reproduced: denied read 403, audit source pending, no native receipt, bounded incomplete drain while all sinks remained alive")
}

type auditTrackingStore struct {
	*pgstore.Store
	closes     atomic.Int32
	afterClose atomic.Int32
}

func (s *auditTrackingStore) touch() {
	if s.closes.Load() != 0 {
		s.afterClose.Add(1)
	}
}
func (s *auditTrackingStore) Close() error { s.closes.Add(1); return s.Store.Close() }
func (s *auditTrackingStore) ClaimDeliveries(ctx context.Context, c durable.DeliveryClaim) ([]durable.DeliveryRecord, error) {
	s.touch()
	return s.Store.ClaimDeliveries(ctx, c)
}
func (s *auditTrackingStore) RenewDelivery(ctx context.Context, c durable.DeliveryToken, d time.Duration) (time.Time, error) {
	s.touch()
	return s.Store.RenewDelivery(ctx, c, d)
}
func (s *auditTrackingStore) AcknowledgeDelivery(ctx context.Context, c durable.DeliveryToken, r durable.SinkReceipt) error {
	s.touch()
	return s.Store.AcknowledgeDelivery(ctx, c, r)
}
func (s *auditTrackingStore) RetryDelivery(ctx context.Context, r durable.DeliveryRetry) error {
	s.touch()
	return s.Store.RetryDelivery(ctx, r)
}
func (s *auditTrackingStore) BlockDelivery(ctx context.Context, c durable.DeliveryToken) error {
	s.touch()
	return s.Store.BlockDelivery(ctx, c)
}
func (s *auditTrackingStore) DeliveryStatus(ctx context.Context, r durable.DeliveryStatusRequest) (durable.DeliveryStatus, error) {
	s.touch()
	return s.Store.DeliveryStatus(ctx, r)
}

func TestNativeAuditRoutingShutdownRecovery(t *testing.T) {
	profiles, db := lifecycleChronicleProfiles(t)
	sinks := profiles.start(t)
	storeDB, err := sinkhost.Open(t.Context(), profiles.configs["production"].DSNs["dispatch"])
	if err != nil {
		t.Fatal(err)
	}
	tracked := &auditTrackingStore{Store: pgstore.New(storeDB)}
	var diagnostics bytes.Buffer
	c := newConfiguredCommandClient(t, tracked, &LifecycleOptions{InstanceID: "audit-shutdown", SkipSampleExecutions: true, DiagnosticWriter: &diagnostics}, profiles.supervisor)
	if err = c.host.StartChroniclePublisher(t.Context(), profiles.routes.Routes); err != nil {
		t.Fatal(err)
	}
	denyAuditRead(t, c)
	source := readAuditSource(t, db["dispatch"], false)
	auditEventually(t, "denial audit not delivered", func() bool {
		return auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NOT NULL`, source.ID) == 1
	})
	verifyAuditReceipt(t, profiles.configs["audit"], db, source)
	sinks["audit"].kill(t)
	denyAuditRead(t, c)
	pending := readAuditSource(t, db["dispatch"], true)
	auditEventually(t, "audit outage did not retain attempted source", func() bool {
		return auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND attempts>0 AND delivered_at IS NULL`, pending.ID) == 1
	})
	// Other bindings must continue while the audit endpoint is unavailable.
	for _, namespace := range []string{"production", "foreign"} {
		audit, captureErr := durable.CaptureSecurityAudit("operator-host", namespace, "qualification.route", "allowed", "operator-host", durable.AuditMetadata{ActorKind: "system", ActorID: "qualification", RequestID: "route-" + namespace})
		if captureErr != nil {
			t.Fatal(captureErr)
		}
		d, appendErr := tracked.AppendSecurityAudit(t.Context(), audit)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		auditEventually(t, "healthy route stalled behind audit outage", func() bool {
			return auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NOT NULL`, d.ID) == 1
		})
		verifyAuditReceipt(t, profiles.configs[namespace], db, d)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	err = c.host.Close(deadline)
	cancel()
	if !errors.Is(err, delivery.ErrIncompleteShutdown) || tracked.closes.Load() != 0 {
		t.Fatal("incomplete publisher drain closed storage")
	}
	if _, err = tracked.GetNamespace(t.Context(), "operator-host", "audit"); err != nil {
		t.Fatal("retained storage unavailable")
	}
	if !strings.Contains(diagnostics.String(), `"category":"incomplete"`) || !strings.Contains(diagnostics.String(), `"deadline":true`) {
		t.Fatal("incomplete stage diagnostics missing")
	}
	sinks["audit"] = profiles.startSink(t, "audit")
	fresh, freshCancel := context.WithTimeout(context.Background(), 15*time.Second)
	err = c.host.Close(fresh)
	freshCancel()
	if err != nil {
		t.Fatal("fresh-context shutdown recovery failed")
	}
	verifyAuditReceipt(t, profiles.configs["audit"], db, pending)
	if err = c.host.Close(t.Context()); err != nil || tracked.closes.Load() != 1 || tracked.afterClose.Load() != 0 {
		t.Fatal("shutdown repeated storage close or accessed closed storage")
	}
	if auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE destination='chronicle' AND delivered_at IS NULL`) != 0 {
		t.Fatal("recovered publisher left pending sources")
	}
	for _, route := range profiles.routes.Routes {
		if strings.Contains(diagnostics.String(), route.Credential.Secret) {
			t.Fatal("diagnostic secret disclosure")
		}
	}
	t.Log("native denial receipt verified; audit outage retained pending source and storage; other routes progressed; fresh-context retry produced one receipt and one storage close")
}

func (s *auditTrackingStore) CheckDeliveryCompatibility(ctx context.Context, protocol int) error {
	s.touch()
	return s.Store.CheckDeliveryCompatibility(ctx, protocol)
}
func (s *auditTrackingStore) AppendSecurityAudit(ctx context.Context, audit durable.SecurityAudit) (durable.Delivery, error) {
	s.touch()
	return s.Store.AppendSecurityAudit(ctx, audit)
}
