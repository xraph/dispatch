package durable_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestDeliveryCanonicalFingerprint(t *testing.T) {
	n := durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: "i", Namespace: "n", AppID: "a", TenantID: "t", SchemaVersion: 1, RequireAudit: true}}
	source := durable.DeliverySource{Key: durable.Key{Namespace: "n", WorkflowID: "w", RunID: "r"}, Kind: "event", ID: "1", Sequence: 1, OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 987654321, time.FixedZone("offset", 3600)), Action: "started", Outcome: "accepted", Metadata: durable.AuditMetadata{ActorKind: "system", ActorID: "dispatch"}}
	d, err := durable.NewDelivery(n, durable.DestinationChronicle, source)
	if err != nil {
		t.Fatal(err)
	}
	source.OccurredAt = source.OccurredAt.UTC()
	same, err := durable.NewDelivery(n, durable.DestinationChronicle, source)
	if err != nil || d != same {
		t.Fatalf("offset normalization: %+v %v", same, err)
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var loaded durable.Delivery
	if err = json.Unmarshal(data, &loaded); err != nil {
		t.Fatal(err)
	}
	if err = loaded.Verify(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < reflect.TypeOf(d).NumField(); i++ {
		field := reflect.TypeOf(d).Field(i)
		if field.Name == "Fingerprint" {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			changed := d
			value := reflect.ValueOf(&changed).Elem().Field(i)
			switch value.Kind() {
			case reflect.String:
				value.SetString(value.String() + "x")
			case reflect.Int, reflect.Int64:
				value.SetInt(value.Int() + 1)
			case reflect.Struct:
				if field.Name == "OccurredAt" {
					changed.OccurredAt = changed.OccurredAt.Add(time.Microsecond)
				} else {
					changed.Metadata.CorrelationID = "other"
				}
			}
			if err := changed.Verify(); err == nil {
				t.Fatal("immutable field excluded from fingerprint")
			}
		})
	}
}

func TestAuditMachineActorKinds(t *testing.T) {
	for _, kind := range []string{"api_key", "service_acct"} {
		m := durable.AuditMetadata{ActorKind: kind, ActorID: "machine"}
		if err := m.Validate(); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}
func TestDeliveryIdentityIncludesSchema(t *testing.T) {
	n := durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: "i", Namespace: "n", AppID: "a", TenantID: "t", SchemaVersion: 1}}
	d, err := durable.NewDelivery(n, durable.DestinationChronicle, durable.DeliverySource{Key: durable.Key{Namespace: "n"}, Kind: "security", ID: "attempt", OccurredAt: time.Now(), Action: "read", Outcome: "denied", Metadata: durable.AuditMetadata{ActorKind: "anonymous"}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal([]string{"i", "chronicle", "1", "n", "", "", "security", "attempt"})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	if d.ID != hex.EncodeToString(hash[:]) {
		t.Fatal("schema omitted from identity")
	}
	n.SchemaVersion = 2
	if _, err = durable.NewDelivery(n, durable.DestinationChronicle, durable.DeliverySource{}); err == nil {
		t.Fatal("unsupported schema accepted")
	}
}
