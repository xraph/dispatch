//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/durabletest"
)

// TestSinkConflictRestart uses an explicitly supplied dedicated PostgreSQL
// database so qualification can cap one container and avoid parallel runtimes.
func TestSinkConflictRestart(t *testing.T) {
	dsn := os.Getenv("DISPATCH_SINK_TEST_DSN")
	if dsn == "" {
		t.Skip("DISPATCH_SINK_TEST_DSN is required")
	}
	s := openWakeStore(t, dsn)
	durabletest.RunOutboxConflict(t, s)
	restarted := openWakeStore(t, dsn)
	scope := durable.DeliveryScope{InstallationID: "conflict-install", Destination: durable.DestinationChronicle}
	status, err := restarted.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: scope, Limit: 100})
	if err != nil || status.Pending != 1 || status.Blocked != 1 {
		t.Fatalf("restart retention: %+v %v", status, err)
	}
	if err = restarted.CheckDeliveryCompatibility(t.Context(), 0); err == nil {
		t.Fatal("old publisher protocol admitted")
	}
	pg, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(context.Background())
	var blocked durable.DeliveryRecord
	for _, r := range status.Records {
		if r.Blocked() {
			blocked = r
		}
	}
	// Dispatch v1.7.1-0.20261009193606-6d86536a4ba5 uses this mutation
	// after selecting pending rows. Missing or malformed capability is refused
	// before that artifact can obtain ownership and call its sink.
	for _, marker := range []string{"", "0", "garbage"} {
		if _, err = pg.Exec(t.Context(), `SELECT set_config('dispatch.delivery_publisher_protocol',$1,FALSE)`, marker); err != nil {
			t.Fatal(err)
		}
		_, err = pg.Exec(t.Context(), `UPDATE dispatch_durable_outbox SET owner=$2,epoch=$3,attempts=$4,lease_until=$5 WHERE id=$1`, blocked.Delivery.ID, "old-artifact", blocked.Epoch+1, blocked.Attempts+1, time.Now().Add(time.Minute))
		var pgError interface{ SQLState() string }
		if !errors.As(err, &pgError) || pgError.SQLState() != "DA003" {
			t.Fatalf("old artifact claim not refused (%q): %v", marker, err)
		}
	}
	if _, err = pg.Exec(t.Context(), `RESET dispatch.delivery_publisher_protocol`); err != nil {
		t.Fatal(err)
	}
	tx, err := pg.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), `SELECT dispatch_delivery_publisher_check(1)`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	var retained bool
	if err = pg.QueryRow(t.Context(), `SELECT current_setting('dispatch.delivery_publisher_protocol',TRUE)='1'`).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained {
		t.Fatal("transaction marker survived commit")
	}
	var owner string
	var attempts, epoch int64
	if err = pg.QueryRow(t.Context(), `SELECT owner,attempts,epoch FROM dispatch_durable_outbox WHERE id=$1`, blocked.Delivery.ID).Scan(&owner, &attempts, &epoch); err != nil {
		t.Fatal(err)
	}
	if owner != "" || attempts != blocked.Attempts || epoch != blocked.Epoch {
		t.Fatal("old artifact acquired ownership")
	}
	a := durable.SecurityAudit{InstallationID: scope.InstallationID, Namespace: blocked.Delivery.Namespace, AttemptID: strings.Repeat("c", 64), OccurredAt: time.Now().UTC(), Action: "read", Outcome: "denied", Metadata: durable.AuditMetadata{ActorKind: "anonymous"}}
	if _, err = restarted.AppendSecurityAudit(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	rows, err := restarted.ClaimDeliveries(t.Context(), durable.DeliveryClaim{DeliveryScope: scope, Owner: "compatible", Limit: 100, LeaseDuration: time.Minute})
	if err != nil || len(rows) != 1 || rows[0].Delivery.ID == blocked.Delivery.ID {
		t.Fatalf("unrelated progress: %+v %v", rows, err)
	}
	row := rows[0]
	receipt := durable.SinkReceipt{ID: "verified-receipt", DeliveryID: row.Delivery.ID, Destination: row.Delivery.Destination, SchemaVersion: 1, Fingerprint: row.Delivery.Fingerprint, MappingVersion: 1, SinkFingerprint: strings.Repeat("d", 64), Evidence: `{"accepted":true}`}
	if err = restarted.AcknowledgeDelivery(t.Context(), row.Token(), receipt); err != nil {
		t.Fatal(err)
	}
	status, err = restarted.DeliveryStatus(t.Context(), durable.DeliveryStatusRequest{DeliveryScope: scope, Limit: 100})
	if err != nil || status.Pending != 1 || status.Blocked != 1 {
		t.Fatalf("post-progress status: %+v %v", status, err)
	}
	for _, r := range status.Records {
		if r.Delivery.ID == row.Delivery.ID && r.Receipt != receipt {
			t.Fatal("receipt binding not persisted")
		}
	}
	t.Logf("restart: pending=%d blocked=%d; old artifact SQLSTATE=DA003; unrelated delivery acknowledged", status.Pending, status.Blocked)
}
