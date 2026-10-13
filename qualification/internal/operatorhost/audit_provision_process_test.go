package operatorhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func TestNativeChronicleProvisionOriginalIdentity(t *testing.T) {
	profiles, _ := lifecycleChronicleProfiles(t)
	base := profiles.configs["production"]
	authDB, err := pgxProfileConnection(t, base.DSNs["authsome"])
	if err != nil {
		t.Fatal(err)
	}
	defer authDB.Close(t.Context())
	if auditCount(t, authDB, `SELECT count(*) FROM authsome_apps`) != 1 || auditCount(t, authDB, `SELECT count(*) FROM authsome_environments`) != 1 {
		t.Fatal("profile provisioning created another app or environment")
	}
	if auditCount(t, authDB, `SELECT count(*) FROM authsome_service_accounts`) != 6 || auditCount(t, authDB, `SELECT count(*) FROM authsome_api_keys`) != 6 {
		t.Fatal("profile credentials not separately issued")
	}
	original, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	audit := profiles.configs["audit"]
	audit.Addresses["dispatch"] = "127.0.0.1:9"
	audit.DSNs["dispatch"] = "changed"
	audit.Credentials["chronicle"] = base.Credentials["chronicle"]
	audit.TokenKey[0] ^= 1
	audit.HMACKey[0] ^= 1
	unchanged, err := json.Marshal(base)
	if err != nil || !bytes.Equal(original, unchanged) {
		t.Fatal("additional profile aliases production configuration")
	}
}

func TestNativeChronicleProvisionOwnershipConflict(t *testing.T) {
	base, _, _ := lifecycleSinkConfig(t)
	db, err := sinkhost.Open(t.Context(), base.DSNs["dispatch"])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := pgstore.New(db)
	if _, err = store.RegisterNamespace(t.Context(), durable.NamespaceConfig{InstallationID: base.Binding.InstallationID, Namespace: "audit", AppID: base.Binding.AppID, TenantID: "other-tenant", RequireAudit: true, RequireHooks: true, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	binding := base.Binding
	binding.Namespace = "audit"
	binding.TenantID = "tenant-audit"
	if _, err = sinkhost.ProvisionChronicleProfile(t.Context(), base, binding, "127.0.0.1:12345"); !errors.Is(err, durable.ErrRequestConflict) {
		t.Fatal("additional profile replaced conflicting ownership")
	}
	record, err := store.GetNamespace(t.Context(), base.Binding.InstallationID, "audit")
	if err != nil || record.TenantID != "other-tenant" {
		t.Fatal("ownership conflict changed namespace")
	}
	authDB, err := pgxProfileConnection(t, base.DSNs["authsome"])
	if err != nil {
		t.Fatal(err)
	}
	defer authDB.Close(t.Context())
	if auditCount(t, authDB, `SELECT count(*) FROM authsome_service_accounts`) != 4 {
		t.Fatal("conflicting profile issued a credential")
	}
}
