package sinkhost

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/authsome/apikey"
	"github.com/xraph/authsome/serviceaccount"
	apg "github.com/xraph/authsome/store/postgres"
	"github.com/xraph/warden/policy"
	wpg "github.com/xraph/warden/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func (r *processRig) pending(role string) durable.Delivery {
	r.t.Helper()
	var raw []byte
	if err := r.db["dispatch"].QueryRow(r.ctx, "SELECT envelope FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NULL AND error_category<>'conflict' ORDER BY id LIMIT 1", role).Scan(&raw); err != nil {
		r.t.Fatal(err)
	}
	var d durable.Delivery
	if err := json.Unmarshal(raw, &d); err != nil {
		r.t.Fatal(err)
	}
	return d
}
func (r *processRig) body(role string, d durable.Delivery) []byte {
	r.t.Helper()
	var request any
	var err error
	if role == "chronicle" {
		request, err = ecosystem.ChronicleRequest(r.c.Binding, d)
	} else {
		request, err = ecosystem.RelayRequest(r.c.Binding, d)
	}
	if err != nil {
		r.t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		r.t.Fatal(err)
	}
	return raw
}
func (r *processRig) securityMatrix() {
	for _, role := range []string{"chronicle", "relay"} {
		r.run(role+"_persisted_authority", func(t *testing.T) {
			r.stop(role, true)
			r.command("security-"+role, "operator", 202)
			r.eventually(func() bool {
				return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NULL", role) > 0
			})
			r.stop("dispatch", true)
			d := r.pending(role)
			body := r.body(role, d)
			receipts := r.count(role, "SELECT count(*) FROM "+role+"_acceptances")
			events := r.count(role, "SELECT count(*) FROM "+role+"_events")
			assertNoEffects := func() {
				r.equal(receipts, r.count(role, "SELECT count(*) FROM "+role+"_acceptances"), "denied receipt effects")
				r.equal(events, r.count(role, "SELECT count(*) FROM "+role+"_events"), "denied event effects")
				r.equal(1, r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NULL AND receipt IS NULL AND error_category<>'conflict'", d.ID), "denied source stays retryable and unacknowledged")
			}
			r.freshAuthority(role, "revoked-publisher", assertNoEffects)
			assertDenied := func(want int) {
				r.post(role, "/accept", r.c.Credentials[role].Secret, body, want, nil)
				assertNoEffects()
			}
			for _, field := range []string{"producer", "org_id", "source_fingerprint"} {
				r.run("forged-body-"+field, func(_ *testing.T) {
					var fields map[string]json.RawMessage
					if e := json.Unmarshal(body, &fields); e != nil {
						r.t.Fatal(e)
					}
					fields[field] = json.RawMessage(`"untrusted"`)
					changed, e := json.Marshal(fields)
					if e != nil {
						r.t.Fatal(e)
					}
					r.post(role, "/accept", r.c.Credentials[role].Secret, changed, 400, nil)
					r.equal(receipts, r.count(role, "SELECT count(*) FROM "+role+"_acceptances"), "forged body effects")
					r.equal(1, r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NULL AND receipt IS NULL", d.ID), "forged body source acknowledgement")
				})
			}
			authDB, err := Open(r.ctx, r.authorityDSN(role))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = authDB.Close() }()
			reopenAuthority := func() {
				if e := authDB.Close(); e != nil {
					r.t.Fatal(e)
				}
				authDB, err = Open(r.ctx, r.authorityDSN(role))
				if err != nil {
					r.t.Fatal(err)
				}
			}
			st := apg.New(authDB)
			key, err := st.GetAPIKey(r.ctx, r.c.Credentials[role].KeyID)
			if err != nil {
				t.Fatal(err)
			}
			account, err := st.GetServiceAccount(r.ctx, r.c.Credentials[role].AccountID)
			if err != nil {
				t.Fatal(err)
			}
			r.run("revoked-publisher-recovery", func(t *testing.T) {
				revoked := *key
				revoked.Revoked = true
				if e := st.UpdateAPIKey(r.ctx, &revoked); e != nil {
					t.Fatal(e)
				}
				assertDenied(401)
				beforeAttempts := r.count("dispatch", "SELECT attempts FROM dispatch_durable_outbox WHERE id=$1", d.ID)
				r.start("dispatch", r.c)
				r.eventually(func() bool {
					return r.count("dispatch", "SELECT COALESCE(max(attempts),0) FROM dispatch_durable_outbox WHERE id=$1 AND error_category='unavailable' AND delivered_at IS NULL AND lease_until IS NULL AND owner=''", d.ID) > beforeAttempts
				})
				r.stop("dispatch", true)
				assertNoEffects()
				if e := st.UpdateAPIKey(r.ctx, key); e != nil {
					t.Fatal(e)
				}
				// The default failure budget persists in the old authority store.
				r.freshAuthority(role, "pending-recovery", assertNoEffects)
				r.start("dispatch", r.c)
				r.settled()
				r.equal(1, r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NOT NULL AND receipt IS NOT NULL", d.ID), "original revoked source recovered")
				r.verify()
				t.Log("revoked publisher quiesced; replacement authority fixture accepted original pending source with verified receipt")
			})
			// The live rejection campaign owns its limiter state. Independent probes
			// get a new command-created source and no background publisher requests.
			r.stop(role, true)
			r.command("security-independent-"+role, "operator", 202)
			r.eventually(func() bool {
				return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NULL", role) > 0
			})
			r.stop("dispatch", true)
			d = r.pending(role)
			body = r.body(role, d)
			receipts = r.count(role, "SELECT count(*) FROM "+role+"_acceptances")
			events = r.count(role, "SELECT count(*) FROM "+role+"_events")
			r.freshAuthority(role, "independent-key-account", assertNoEffects)
			reopenAuthority()
			st = apg.New(authDB)
			t.Log("independent authority phase: new command source pending; publisher stopped; replacement authority fixture")
			expired := time.Now().Add(-time.Hour)
			for _, tc := range []struct {
				name   string
				mutate func(*apikey.APIKey)
				status int
			}{
				{"expired", func(k *apikey.APIKey) { k.ExpiresAt = &expired }, 401},
				{"insufficient-scope", func(k *apikey.APIKey) { k.Scopes = nil }, 403},
				{"scope-growth", func(k *apikey.APIKey) { k.Scopes = append([]string{Scope(role)}, "ungranted") }, 401},
			} {
				r.run(tc.name, func(_ *testing.T) {
					changed := *key
					tc.mutate(&changed)
					if e := st.UpdateAPIKey(r.ctx, &changed); e != nil {
						r.t.Fatal(e)
					}
					assertDenied(tc.status)
					if e := st.UpdateAPIKey(r.ctx, key); e != nil {
						r.t.Fatal(e)
					}
				})
			}
			for _, tc := range []struct {
				name   string
				mutate func(*serviceaccount.ServiceAccount)
			}{
				{"inactive-account", func(a *serviceaccount.ServiceAccount) { a.Active = false }},
				{"expired-account", func(a *serviceaccount.ServiceAccount) { a.ExpiresAt = &expired }},
				{"reduced-account-scopes", func(a *serviceaccount.ServiceAccount) { a.Scopes = nil }},
			} {
				r.run(tc.name, func(_ *testing.T) {
					changed := *account
					tc.mutate(&changed)
					if e := st.UpdateServiceAccount(r.ctx, &changed); e != nil {
						r.t.Fatal(e)
					}
					assertDenied(401)
					if e := st.UpdateServiceAccount(r.ctx, account); e != nil {
						r.t.Fatal(e)
					}
				})
			}
			r.run("missing-account", func(_ *testing.T) {
				if e := st.DeleteServiceAccount(r.ctx, account.ID); e != nil {
					r.t.Fatal(e)
				}
				assertDenied(401)
				if e := st.CreateServiceAccount(r.ctx, account); e != nil {
					r.t.Fatal(e)
				}
				if e := st.CreateAPIKey(r.ctx, key); e != nil {
					r.t.Fatal(e)
				}
			})
			r.run("resolver-unavailable", func(_ *testing.T) {
				r.execAuthority(role, "ALTER TABLE authsome_service_accounts RENAME TO unavailable_accounts")
				assertDenied(401)
				r.execAuthority(role, "ALTER TABLE unavailable_accounts RENAME TO authsome_service_accounts")
			})
			// Resolver restoration and the policy matrix have an explicit process
			// boundary. The valid no-effect probe must reach mapped-body validation.
			r.stop(role, true)
			r.start(role, r.c)
			r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 400, nil)
			assertNoEffects()
			r.defaultFailureBudget(role, assertNoEffects)
			policyDB, err := Open(r.ctx, r.c.DSNs["warden"])
			if err != nil {
				t.Fatal(err)
			}
			defer policyDB.Close()
			policies := wpg.New(policyDB)
			original, err := policies.GetPolicyByName(r.ctx, r.c.PolicyTenant, "", role)
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name   string
				mutate func(*policy.Policy)
				status int
			}{
				{"policy-deny", func(p *policy.Policy) { p.Effect = policy.EffectDeny }, 403},
				{"unhandled-obligation", func(p *policy.Policy) { p.Obligations = []string{"require-mfa"} }, 503},
				{"wrong-policy-installation", func(p *policy.Policy) { p.Resources = []string{"dispatch_installation:other"} }, 403},
			} {
				r.run(tc.name, func(_ *testing.T) {
					changed := *original
					tc.mutate(&changed)
					if e := policies.UpdatePolicy(r.ctx, &changed); e != nil {
						r.t.Fatal(e)
					}
					assertDenied(tc.status)
					if e := policies.UpdatePolicy(r.ctx, original); e != nil {
						r.t.Fatal(e)
					}
				})
			}
			r.run("policy-unavailable", func(_ *testing.T) {
				r.exec("warden", "ALTER TABLE warden_policies RENAME TO unavailable_policies")
				assertDenied(503)
				r.exec("warden", "ALTER TABLE unavailable_policies RENAME TO warden_policies")
			})
			r.start("dispatch", r.c)
			r.settled()
			r.verify()
			t.Log("credentials and policies restored; original pending delivery recovered")
		})
	}
}
func (r *processRig) exec(role, query string) {
	r.t.Helper()
	if _, err := r.db[role].Exec(r.ctx, query); err != nil {
		r.t.Fatal(err)
	}
}
