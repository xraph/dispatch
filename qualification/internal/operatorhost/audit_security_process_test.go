package operatorhost

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/warden/policy"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

func postChronicleProfile(t *testing.T, profile sinkhost.Config, bearer string, body []byte, headers map[string]string, want int) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+profile.Addresses["chronicle"]+"/accept", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		t.Fatal("native profile request unavailable")
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != want {
		t.Fatalf("native profile status=%d want=%d", response.StatusCode, want)
	}
}

func TestNativeAuditProfileAuthorization(t *testing.T) {
	profiles, db := lifecycleChronicleProfiles(t)
	sinks := profiles.start(t)
	noEffects := func() {
		t.Helper()
		if auditCount(t, db["chronicle"], `SELECT count(*) FROM chronicle_acceptances`) != 0 || auditCount(t, db["chronicle"], `SELECT count(*) FROM chronicle_events`) != 0 {
			t.Fatal("rejected profile wrote native acceptance")
		}
	}
	bodies := map[string][]byte{}
	for _, namespace := range []string{"production", "audit", "foreign"} {
		profile := profiles.configs[namespace]
		audit, err := durable.CaptureSecurityAudit("operator-host", namespace, "qualification.security", "denied", "operator-host", durable.AuditMetadata{ActorKind: "system", ActorID: "qualification", RequestID: "security-" + namespace})
		if err != nil {
			t.Fatal(err)
		}
		source, err := audit.Delivery(durable.NamespaceRecord{NamespaceConfig: durable.NamespaceConfig{InstallationID: "operator-host", Namespace: namespace, AppID: profile.Binding.AppID, TenantID: profile.Binding.TenantID, RequireAudit: true, RequireHooks: true, SchemaVersion: 1}})
		if err != nil {
			t.Fatal(err)
		}
		request, err := ecosystem.ChronicleRequest(profile.Binding, source)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		bodies[namespace] = body
	}
	// Each endpoint permits only its configured credential, even in one app/environment.
	for _, destination := range []string{"production", "audit", "foreign"} {
		for _, credential := range []string{"production", "audit", "foreign"} {
			if destination == credential {
				continue
			}
			postChronicleProfile(t, profiles.configs[destination], profiles.configs[credential].Credentials["chronicle"].Secret, bodies[destination], nil, 401)
			noEffects()
		}
	}
	profile := profiles.configs["audit"]
	bearer := profile.Credentials["chronicle"].Secret
	for header := range map[string]bool{"X-App-ID": true, "X-Tenant-ID": true, "X-Installation-ID": true} {
		postChronicleProfile(t, profile, bearer, bodies["audit"], map[string]string{header: "untrusted"}, 401)
		noEffects()
	}
	// Restarting clears process state but preserves native persisted authority checks.
	sinks["audit"].kill(t)
	sinks["audit"] = profiles.startSink(t, "audit")
	for _, field := range []string{"app_id", "tenant_id", "namespace", "producer", "source_fingerprint"} {
		var request map[string]any
		if err := json.Unmarshal(bodies["audit"], &request); err != nil {
			t.Fatal(err)
		}
		switch field {
		case "app_id", "tenant_id":
			request["event"].(map[string]any)[field] = "untrusted"
		case "namespace":
			request["event"].(map[string]any)["metadata"].(map[string]any)[field] = "unknown"
		default:
			request[field] = "untrusted"
		}
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		postChronicleProfile(t, profile, bearer, raw, nil, 400)
		noEffects()
	}
	authDB, err := sinkhost.Open(t.Context(), profile.DSNs["authsome"])
	if err != nil {
		t.Fatal(err)
	}
	defer authDB.Close()
	authorityStore := apg.New(authDB)
	key, err := authorityStore.GetAPIKey(t.Context(), profile.Credentials["chronicle"].KeyID)
	if err != nil {
		t.Fatal(err)
	}
	revoked := *key
	revoked.Revoked = true
	if err = authorityStore.UpdateAPIKey(t.Context(), &revoked); err != nil {
		t.Fatal(err)
	}
	postChronicleProfile(t, profile, bearer, bodies["audit"], nil, 401)
	noEffects()
	if err = authorityStore.UpdateAPIKey(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	reduced := *key
	reduced.Scopes = nil
	if err = authorityStore.UpdateAPIKey(t.Context(), &reduced); err != nil {
		t.Fatal(err)
	}
	postChronicleProfile(t, profile, bearer, bodies["audit"], nil, 403)
	noEffects()
	if err = authorityStore.UpdateAPIKey(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	policyDB, err := sinkhost.Open(t.Context(), profile.DSNs["warden"])
	if err != nil {
		t.Fatal(err)
	}
	defer policyDB.Close()
	policies := wpg.New(policyDB)
	original, err := policies.GetPolicyByName(t.Context(), profile.PolicyTenant, "", "chronicle-audit")
	if err != nil {
		t.Fatal(err)
	}
	denied := *original
	denied.Effect = policy.EffectDeny
	if err = policies.UpdatePolicy(t.Context(), &denied); err != nil {
		t.Fatal(err)
	}
	postChronicleProfile(t, profile, bearer, bodies["audit"], nil, 403)
	noEffects()
	if err = policies.UpdatePolicy(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	// A policy read failure must refuse acceptance; restore the table before cleanup.
	policyConnection, err := pgxProfileConnection(t, profile.DSNs["warden"])
	if err != nil {
		t.Fatal(err)
	}
	defer policyConnection.Close(t.Context())
	if _, err = policyConnection.Exec(t.Context(), `ALTER TABLE warden_policies RENAME TO audit_unavailable_policies`); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if _, restoreErr := policyConnection.Exec(t.Context(), `ALTER TABLE audit_unavailable_policies RENAME TO warden_policies`); restoreErr != nil {
				t.Error("policy fixture restore failed")
			}
		}()
		postChronicleProfile(t, profile, bearer, bodies["audit"], nil, 503)
		noEffects()
	}()
	postChronicleProfile(t, profile, bearer, bodies["audit"], nil, 200)
	if auditCount(t, db["chronicle"], `SELECT count(*) FROM chronicle_acceptances`) != 1 {
		t.Fatal("restored valid profile did not accept exactly once")
	}
	t.Log("all six opposite credentials refused; forged app/tenant/namespace/body, revoked key, insufficient scope, Warden deny and unavailable policy produced no receipts")
}

func pgxProfileConnection(t *testing.T, dsn string) (*pgx.Conn, error) {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	query := parsed.Query()
	query.Del("pool_max_conns")
	parsed.RawQuery = query.Encode()
	return pgx.Connect(t.Context(), parsed.String())
}
