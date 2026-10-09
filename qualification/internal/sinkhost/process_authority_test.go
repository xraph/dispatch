package sinkhost

import (
	"maps"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	apg "github.com/xraph/authsome/store/postgres"
)

// The baseline contains engine-issued identities and no HTTP failure history.
// Ordinary processes retain the original shared store until a scenario selects
// an isolated fixture. No authority rows or limiter counters are edited here.
func (r *processRig) sealAuthorityBaseline() {
	r.t.Helper()
	u, err := url.Parse(r.c.DSNs["authsome"])
	if err != nil {
		r.t.Fatal(err)
	}
	original := u.Path[1:]
	r.authorityBase = original + "_baseline"
	r.authorityDSNs = map[string]string{}
	if err := r.db["authsome"].Close(r.ctx); err != nil {
		r.t.Fatal(err)
	}
	if _, err := r.authorityAdmin.Exec(r.ctx, "CREATE DATABASE "+pgx.Identifier{r.authorityBase}.Sanitize()+" WITH TEMPLATE "+pgx.Identifier{original}.Sanitize()+" ALLOW_CONNECTIONS false"); err != nil {
		r.t.Fatal(err)
	}
	r.db["authsome"] = r.authorityConnection(r.c.DSNs["authsome"])
	r.t.Log("sealed pre-request Authsome baseline; ordinary topology keeps its shared authority store")
}

func (r *processRig) authorityConnection(dsn string) *pgx.Conn {
	r.t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		r.t.Fatal(err)
	}
	q := u.Query()
	q.Del("pool_max_conns")
	u.RawQuery = q.Encode()
	db, err := pgx.Connect(r.ctx, u.String())
	if err != nil {
		r.t.Fatal(err)
	}
	return db
}

func (r *processRig) authorityDSN(role string) string {
	if dsn := r.authorityDSNs[role]; dsn != "" {
		return dsn
	}
	return r.c.DSNs["authsome"]
}

func (r *processRig) authorityConfig(role string, c Config) Config {
	if dsn := r.authorityDSNs[role]; dsn != "" {
		c.DSNs = maps.Clone(c.DSNs)
		c.DSNs["authsome"] = dsn
	}
	return c
}

func (r *processRig) execAuthority(role, query string) {
	r.t.Helper()
	db := r.authorityConnection(r.authorityDSN(role))
	defer func() { _ = db.Close(r.ctx) }()
	if _, err := db.Exec(r.ctx, query); err != nil {
		r.t.Fatal(err)
	}
}

// freshAuthority replaces only the target role's authority fixture. Source,
// sinks and Warden retain their original persisted state. Container cleanup
// removes the sealed baseline and all disposable clones together.
func (r *processRig) freshAuthority(role, phase string, assertNoEffects func()) {
	r.t.Helper()
	if r.processes["dispatch"] != nil {
		r.t.Fatal("authority fixture replacement requires a stopped publisher")
	}
	r.stop(role, true)
	r.authoritySeq++
	name := r.authorityBase + "_" + role + "_" + strconv.Itoa(r.authoritySeq)
	if _, err := r.authorityAdmin.Exec(r.ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH TEMPLATE "+pgx.Identifier{r.authorityBase}.Sanitize()); err != nil {
		r.t.Fatal(err)
	}
	u, err := url.Parse(r.c.DSNs["authsome"])
	if err != nil {
		r.t.Fatal(err)
	}
	u.Path = "/" + name
	r.authorityDSNs[role] = u.String()
	r.start(role, r.c)
	r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 400, nil)
	assertNoEffects()
	r.t.Logf("%s authority fixture replaced for %s: baseline clone %d; valid identity and Warden reached body validation", role, phase, r.authoritySeq)
}

func (r *processRig) defaultFailureBudget(role string, assertNoEffects func()) {
	r.t.Helper()
	r.run("default-failure-budget", func(t *testing.T) {
		r.freshAuthority(role, "default-threshold", assertNoEffects)
		storeDB, err := Open(r.ctx, r.authorityDSN(role))
		if err != nil {
			t.Fatal(err)
		}
		defer storeDB.Close()
		store := apg.New(storeDB)
		key, err := store.GetAPIKey(r.ctx, r.c.Credentials[role].KeyID)
		if err != nil {
			t.Fatal(err)
		}
		// Timing applies only to a pristine fixture, never to an exhausted
		// budget. Authsome's real KV limiter uses UnixNano/minute windows.
		now := time.Now()
		remaining := time.Minute - time.Duration(now.UnixNano()%int64(time.Minute))
		if remaining < 25*time.Second {
			t.Logf("pristine threshold fixture waits %s for a full fixed window", remaining)
			timer := time.NewTimer(remaining + 100*time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.ctx.Done():
				t.Fatal(r.ctx.Err())
			}
		}
		window := time.Now().UnixNano() / int64(time.Minute)
		t.Logf("default threshold begins in fixed window %d", window)
		sameWindow := func() {
			t.Helper()
			if got := time.Now().UnixNano() / int64(time.Minute); got != window {
				t.Fatalf("threshold crossed fixed window: start=%d end=%d; campaign is not retried", window, got)
			}
		}
		revoked := *key
		revoked.Revoked = true
		if err := store.UpdateAPIKey(r.ctx, &revoked); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 19; i++ {
			r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 401, nil)
		}
		if err := store.UpdateAPIKey(r.ctx, key); err != nil {
			t.Fatal(err)
		}
		r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 400, nil)
		assertNoEffects()
		if err := store.UpdateAPIKey(r.ctx, &revoked); err != nil {
			t.Fatal(err)
		}
		r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 401, nil)
		if err := store.UpdateAPIKey(r.ctx, key); err != nil {
			t.Fatal(err)
		}
		r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 401, nil)
		assertNoEffects()
		r.stop(role, true)
		r.start(role, r.c)
		r.post(role, "/accept", r.c.Credentials[role].Secret, []byte(`{}`), 401, nil)
		assertNoEffects()
		sameWindow()
		t.Logf("default threshold and same-store restart stayed in fixed window %d: 19 failures -> valid 400; 20th failure -> restored valid 401; same-store restart -> 401", window)
		r.freshAuthority(role, "policy-matrix", assertNoEffects)
		t.Log("separate authority fixture -> valid 400; no source or sink effects")
	})
}
