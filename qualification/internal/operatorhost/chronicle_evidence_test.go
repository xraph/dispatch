package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	ca "github.com/xraph/chronicle/acceptance"
	"github.com/xraph/chronicle/hash"
	"github.com/xraph/chronicle/keys"
	cpg "github.com/xraph/chronicle/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

type lifecycleHMAC struct{ key []byte }

func (p lifecycleHMAC) Current(context.Context, keys.Use) ([]byte, string, error) {
	return append([]byte(nil), p.key...), "qualification-hmac-1", nil
}
func (p lifecycleHMAC) ByID(_ context.Context, id string) ([]byte, error) {
	if id != "qualification-hmac-1" {
		return nil, keys.ErrKeyNotFound
	}
	return append([]byte(nil), p.key...), nil
}

func verifyLifecycleChronicle(t *testing.T, config sinkhost.Config, db map[string]*pgx.Conn, actor string) {
	t.Helper()
	rows, err := db["dispatch"].Query(t.Context(), `SELECT r.operation,r.request_id,o.envelope,o.receipt FROM dispatch_lifecycle_receipts r JOIN dispatch_durable_outbox o ON o.id=r.delivery_id ORDER BY r.operation,r.request_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[durable.LifecycleOperation]int{}
	for rows.Next() {
		var operation durable.LifecycleOperation
		var requestID string
		var raw, receiptRaw []byte
		if err = rows.Scan(&operation, &requestID, &raw, &receiptRaw); err != nil {
			t.Fatal(err)
		}
		var source durable.Delivery
		var receipt durable.SinkReceipt
		if json.Unmarshal(raw, &source) != nil || json.Unmarshal(receiptRaw, &receipt) != nil {
			t.Fatal("invalid persisted source or receipt")
		}
		if source.SourceKind != "security" || source.SourceID != durable.LifecycleSourceID("production", operation, requestID) || source.Action != durable.LifecycleAction(operation) || source.Metadata.ActorKind != "user" || source.Metadata.ActorID != actor || receipt.Verify(source) != nil || receipt.MappingVersion != ecosystem.MappingVersion {
			t.Fatal("lifecycle source receipt mapping changed")
		}
		mapped, mapErr := ecosystem.ChronicleRequest(config.Binding, source)
		if mapErr != nil {
			t.Fatal(mapErr)
		}
		fingerprint, fingerprintErr := ca.Fingerprint(mapped)
		if fingerprintErr != nil || fingerprint != receipt.SinkFingerprint || fingerprint == source.Fingerprint {
			t.Fatal("mapped sink fingerprint mismatch")
		}
		var persisted []byte
		if err = db["chronicle"].QueryRow(t.Context(), `SELECT receipt FROM chronicle_acceptances WHERE receipt->>'source_key'=$1`, source.ID).Scan(&persisted); err != nil {
			t.Fatal(err)
		}
		var sinkReceipt, ack ca.Receipt
		if json.Unmarshal(persisted, &sinkReceipt) != nil || json.Unmarshal([]byte(receipt.Evidence), &ack) != nil {
			t.Fatal("invalid sink acceptance evidence")
		}
		want, _ := json.Marshal(sinkReceipt)
		got, _ := json.Marshal(ack)
		if !bytes.Equal(want, got) {
			t.Fatal("outbox acknowledgement differs from persisted Chronicle receipt")
		}
		seen[operation]++
		t.Logf("lifecycle operation=%s request=%s source=%s mapped=%s sink=%s sequence=%d", operation, requestID, source.ID, receipt.SinkFingerprint, sinkReceipt.EventID, sinkReceipt.Sequence)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	for operation, want := range map[durable.LifecycleOperation]int{durable.OperationEnrollRetirement: 1, durable.OperationRegisterBuild: 2, durable.OperationRegisterQueryRuntime: 2, durable.OperationBeginRetirement: 1, durable.OperationRequestWorkerDrain: 2, durable.OperationAbortRetirement: 1} {
		if seen[operation] != want {
			t.Fatalf("operation %s count=%d want=%d", operation, seen[operation], want)
		}
	}
	sinkDB, err := sinkhost.Open(t.Context(), config.DSNs["chronicle"])
	if err != nil {
		t.Fatal(err)
	}
	defer sinkDB.Close()
	store := cpg.New(sinkDB)
	chain, err := hash.NewChain(hash.SchemeHMACV5, lifecycleHMAC{config.HMACKey})
	if err != nil {
		t.Fatal(err)
	}
	events, err := db["chronicle"].Query(t.Context(), `SELECT receipt FROM chronicle_acceptances ORDER BY (receipt->>'sequence')::numeric`)
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	var previous string
	var sequence uint64
	for events.Next() {
		var raw []byte
		if err = events.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var receipt ca.Receipt
		if err = json.Unmarshal(raw, &receipt); err != nil {
			t.Fatal(err)
		}
		event, readErr := store.Get(t.Context(), receipt.EventID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		sequence++
		if event.Sequence != sequence || event.PrevHash != previous {
			t.Fatal("live Chronicle chain linkage changed")
		}
		previous = event.Hash
		result, verifyErr := chain.VerifyWithPin(t.Context(), event.PrevHash, event, hash.Pin{Scheme: hash.SchemeHMACV5, Since: 1})
		if verifyErr != nil || !result.OK {
			t.Fatal("live Chronicle HMAC verification failed")
		}
	}
	if err = events.Err(); err != nil {
		t.Fatal(err)
	}
	var sources int64
	if err = db["dispatch"].QueryRow(t.Context(), `SELECT count(*) FROM dispatch_durable_outbox WHERE destination='chronicle' AND delivered_at IS NOT NULL`).Scan(&sources); err != nil || uint64(sources) != sequence {
		t.Fatal("source acknowledgement and unique sink acceptance counts differ")
	}
}
