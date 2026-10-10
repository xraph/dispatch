package sinkhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	ca "github.com/xraph/chronicle/acceptance"
	"github.com/xraph/chronicle/hash"
	cpg "github.com/xraph/chronicle/store/postgres"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

type process struct {
	cmd    *exec.Cmd
	done   chan error
	cancel context.CancelFunc
	mu     sync.Mutex
	peak   int64
}
type processRig struct {
	workersStopped bool
	seeded         map[string]int64
	t              *testing.T
	ctx            context.Context
	c              Config
	dir, binary    string
	processes      map[string]*process
	db             map[string]*pgx.Conn
	authorityAdmin *pgx.Conn
	authorityBase  string
	authorityDSNs  map[string]string
	authoritySeq   int
	client         *http.Client
}

func TestProcesses(t *testing.T) {
	dsn, binary := os.Getenv("DISPATCH_SINK_TEST_DSN"), os.Getenv("DISPATCH_SINK_HOST_BINARY")
	if dsn == "" || binary == "" {
		if os.Getenv("DISPATCH_PROCESS_REQUIRED") == "1" {
			t.Fatal("process gate requires a PostgreSQL DSN and built host executable")
		}
		t.Skip("set DISPATCH_SINK_TEST_DSN and DISPATCH_SINK_HOST_BINARY for real process qualification")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	dir := t.TempDir()
	if evidence := os.Getenv("DISPATCH_SINK_EVIDENCE_DIR"); evidence != "" {
		dir = evidence
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r := &processRig{t: t, ctx: ctx, dir: dir, binary: binary, processes: map[string]*process{}, seeded: map[string]int64{}, db: map[string]*pgx.Conn{}, client: &http.Client{Timeout: 4 * time.Second}}
	r.c = Config{Binding: ecosystem.Binding{Producer: "dispatch-qualification", InstallationID: "process-installation", Namespace: "process-ns", OrgID: "process-org", TenantID: "process-tenant"}, PolicyTenant: "process-tenant", DSNs: map[string]string{}, Addresses: map[string]string{}}
	for _, role := range []string{"dispatch", "relay", "chronicle", "receiver"} {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		r.c.Addresses[role] = listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	for _, role := range []string{"dispatch", "relay", "chronicle", "authsome", "warden"} {
		name := "task3_" + role + "_" + suffix
		if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		u, e := url.Parse(dsn)
		if e != nil {
			t.Fatal(e)
		}
		u.Path = "/" + name
		q := u.Query()
		q.Set("pool_max_conns", "2")
		u.RawQuery = q.Encode()
		r.c.DSNs[role] = u.String()
		q.Del("pool_max_conns")
		u.RawQuery = q.Encode()
		r.db[role], err = pgx.Connect(ctx, u.String())
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for role := range r.processes {
			r.stop(role, true)
		}
		for _, db := range r.db {
			_ = db.Close(context.Background())
		}
	})
	r.c.CallbackDirectory = filepath.Join(r.dir, "callback-handles")
	if err = os.Mkdir(r.c.CallbackDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if bootstrapErr := Bootstrap(ctx, &r.c); bootstrapErr != nil {
		t.Fatal(bootstrapErr)
	}
	r.authorityAdmin = admin
	r.sealAuthorityBaseline()
	for _, role := range []string{"receiver", "chronicle", "relay", "dispatch"} {
		r.start(role, r.c)
	}
	r.command("baseline", "operator", 202)
	r.settled()
	r.verify()
	for _, role := range []string{"relay", "chronicle"} {
		r.run(role+"_process_outage", func(t *testing.T) {
			r.stop(role, true)
			before := r.count(role, "SELECT count(*) FROM "+role+"_acceptances")
			r.command("outage-"+role, "operator", 202)
			r.eventually(func() bool {
				return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NULL", role) > 0
			})
			r.equal(before, r.count(role, "SELECT count(*) FROM "+role+"_acceptances"), "stopped sink receipt count")
			t.Logf("%s stopped: pending=%d PostgreSQL healthy", role, r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NULL", role))
			r.callbackWhileSinkStopped(role)
			r.start(role, r.c)
			r.settled()
			r.verify()
		})
	}
	for _, role := range []string{"chronicle", "relay"} {
		r.run(role+"_commit_before_ack", func(t *testing.T) {
			r.stop("dispatch", false)
			cfg := r.c
			cfg.LostAckDestination = role
			cfg.LostAckMarker = filepath.Join(r.dir, role+"-lost-ack.json")
			r.start("dispatch", cfg)
			r.command("lost-"+role, "operator", 202)
			var raw []byte
			r.eventually(func() bool { raw, err = os.ReadFile(cfg.LostAckMarker); return err == nil })
			r.stop("dispatch", true)
			var evidence struct {
				Delivery durable.Delivery
				Receipt  durable.SinkReceipt
			}
			if err := json.Unmarshal(raw, &evidence); err != nil {
				t.Fatal(err)
			}
			r.equal(1, r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NULL", evidence.Delivery.ID), "lost ack remains pending")
			t.Logf("%s committed receipt %s while source %s remained pending; publisher killed", role, evidence.Receipt.ID, evidence.Delivery.ID)
			r.start("dispatch", r.c)
			r.settled()
			r.verify()
			var persisted []byte
			if err := r.db["dispatch"].QueryRow(ctx, "SELECT receipt FROM dispatch_durable_outbox WHERE id=$1", evidence.Delivery.ID).Scan(&persisted); err != nil {
				t.Fatal(err)
			}
			var receipt durable.SinkReceipt
			if err := json.Unmarshal(persisted, &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt != evidence.Receipt {
				t.Fatal("recovery changed receipt")
			}
		})
	}
	r.run("denied_command_audited", func(_ *testing.T) {
		r.command("denied-run", "denied", 403)
		r.settled()
		r.equal(0, r.count("dispatch", "SELECT count(*) FROM dispatch_executions WHERE workflow_id='denied-run'"), "denied execution")
		r.eventually(func() bool {
			return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE envelope->>'Target'='denied-run' AND envelope->>'Outcome'='denied' AND delivered_at IS NOT NULL") > 0
		})
		r.verify()
	})
	r.run("workers_stopped_publisher_live", func(t *testing.T) {
		r.post("dispatch", "/workers/stop", r.c.Credentials["operator"].Secret, []byte(`{}`), 200, nil)
		r.workersStopped = true
		r.command("after-worker-stop", "operator", 202)
		r.settled()
		r.equal(0, r.count("dispatch", "SELECT count(*) FROM dispatch_executions WHERE workflow_id='after-worker-stop' AND state='completed'"), "stopped workflow completion")
		r.verify()
		t.Log("worker stop returned confirmed completion; subsequent command committed and publisher drained")
	})
	r.negativeMatrix()
	r.receiverOutage()
	r.securityMatrix()
	r.conflicts()
	r.verify()
	for _, role := range []string{"dispatch", "relay", "chronicle", "receiver"} {
		r.stop(role, false)
	}
	r.verifyNoSecrets()
}
func (r *processRig) start(role string, c Config) {
	c = r.authorityConfig(role, c)
	if role == "dispatch" {
		r.workersStopped = false
	}
	r.t.Helper()
	path := filepath.Join(r.t.TempDir(), role+".json")
	if err := Save(path, c); err != nil {
		r.t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(r.dir, role+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		r.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(r.ctx)
	cmd := exec.CommandContext(ctx, r.binary, "-role", role, "-config", path)
	cmd.Env = append(os.Environ(), "GOMEMLIMIT=128MiB")
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		cancel()
		r.t.Fatal(err)
	}
	p := &process{cmd: cmd, done: make(chan error, 1), cancel: cancel}
	r.processes[role] = p
	go func() { p.done <- cmd.Wait(); _ = log.Close() }()
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probe, stop := context.WithTimeout(ctx, time.Second)
				out, e := exec.CommandContext(probe, "ps", "-o", "rss=", "-p", strconv.Itoa(cmd.Process.Pid)).Output()
				stop()
				if e != nil {
					continue
				}
				rss, e := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
				if e != nil {
					continue
				}
				p.mu.Lock()
				if rss > p.peak {
					p.peak = rss
				}
				p.mu.Unlock()
				if rss > 384*1024 {
					cancel()
					return
				}
			}
		}
	}()
	r.eventually(func() bool {
		req, e := http.NewRequestWithContext(r.ctx, http.MethodGet, "http://"+c.Addresses[role]+"/health", nil)
		if e != nil {
			return false
		}
		resp, e := r.client.Do(req)
		if e != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == 200
	})
	p.sample(r.ctx)
	r.t.Logf("%s started pid=%d at %s", role, cmd.Process.Pid, time.Now().UTC().Format(time.RFC3339Nano))
}
func (r *processRig) stop(role string, kill bool) {
	p := r.processes[role]
	if p == nil {
		return
	}
	delete(r.processes, role)
	p.sample(r.ctx)
	if kill {
		_ = p.cmd.Process.Kill()
	} else {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	var exitErr error
	select {
	case exitErr = <-p.done:
	case <-time.After(8 * time.Second):
		_ = p.cmd.Process.Kill()
		exitErr = <-p.done
	}
	if !kill {
		if role == "dispatch" && len(r.seeded) > 0 {
			if exitErr == nil {
				r.t.Error("blocked publisher reported complete shutdown")
			}
		} else if exitErr != nil {
			r.t.Errorf("unexpected %s exit: %v", role, exitErr)
		}
	}
	p.cancel()
	p.mu.Lock()
	peak := p.peak
	p.mu.Unlock()
	r.t.Logf("%s stopped pid=%d kill=%t peak_rss_KiB=%d at %s", role, p.cmd.Process.Pid, kill, peak, time.Now().UTC().Format(time.RFC3339Nano))
	if peak > 384*1024 {
		r.t.Error("host exceeded 384 MiB RSS ceiling")
	}
}
func (r *processRig) count(role, query string, args ...any) int64 {
	r.t.Helper()
	var n int64
	if err := r.db[role].QueryRow(r.ctx, query, args...).Scan(&n); err != nil {
		r.t.Fatal(err)
	}
	return n
}
func (r *processRig) equal(want, got int64, label string) {
	r.t.Helper()
	if want != got {
		r.t.Fatalf("%s: want=%d got=%d", label, want, got)
	}
}
func (r *processRig) eventually(check func() bool) {
	r.t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-r.ctx.Done():
			r.t.Fatal("qualification deadline exceeded")
		case <-deadline.C:
			r.t.Fatal("condition did not converge")
		case <-ticker.C:
		}
	}
}
func (r *processRig) post(role, path, token string, body []byte, want int, headers map[string]string) {
	r.t.Helper()
	req, err := http.NewRequestWithContext(r.ctx, http.MethodPost, "http://"+r.c.Addresses[role]+path, bytes.NewReader(body))
	if err != nil {
		r.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != want {
		r.t.Fatalf("%s%s status=%d want=%d body=%s", role, path, resp.StatusCode, want, raw)
	}
}
func (r *processRig) command(id, credential string, want int) {
	r.post("dispatch", "/command", r.c.Credentials[credential].Secret, fmt.Appendf(nil, `{"id":%q}`, id), want, nil)
}
func (r *processRig) settled() {
	if !r.workersStopped {
		r.eventually(func() bool {
			return r.count("dispatch", "SELECT count(*) FROM dispatch_executions WHERE state='running'") == 0
		})
	}
	r.eventually(func() bool {
		return r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE delivered_at IS NULL AND error_category<>'conflict'") == 0
	})
	r.eventually(func() bool {
		return r.count("relay", "SELECT count(*) FROM relay_deliveries WHERE state<>'delivered'") == 0
	})
	r.t.Logf("delivered=%d pending=%d blocked=%d relay_fanout=%d", r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE delivered_at IS NOT NULL"), r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE delivered_at IS NULL"), r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE error_category='conflict'"), r.count("relay", "SELECT count(*) FROM qualification_received"))
}
func (r *processRig) verify() {
	r.verifyBindings()
	r.t.Helper()
	for _, role := range []string{"chronicle", "relay"} {
		r.equal(r.count("dispatch", "SELECT count(*) FROM dispatch_durable_outbox WHERE destination=$1 AND delivered_at IS NOT NULL", role)+r.seeded[role], r.count(role, "SELECT count(*) FROM "+role+"_acceptances"), role+" unique acceptances")
	}
	r.equal(2*r.count("relay", "SELECT count(*) FROM relay_acceptances"), r.count("relay", "SELECT count(*) FROM qualification_received"), "complete fanout")
	r.equal(0, r.count("relay", "SELECT count(*) FROM qualification_received WHERE convert_from(payload,'UTF8') LIKE '%qualification-payload-must-not-leak%'"), "payload exclusion")
	db, err := Open(r.ctx, r.c.DSNs["chronicle"])
	if err != nil {
		r.t.Fatal(err)
	}
	defer db.Close()
	st := cpg.New(db)
	chain, err := hash.NewChain(hash.SchemeHMACV5, keyProvider{r.c.HMACKey})
	if err != nil {
		r.t.Fatal(err)
	}
	rows, err := r.db["chronicle"].Query(r.ctx, "SELECT receipt FROM chronicle_acceptances ORDER BY (receipt->>'sequence')::numeric")
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	var previous string
	var sequence uint64
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			r.t.Fatal(err)
		}
		var receipt ca.Receipt
		if err := json.Unmarshal(raw, &receipt); err != nil {
			r.t.Fatal(err)
		}
		event, e := st.Get(r.ctx, receipt.EventID)
		if e != nil {
			r.t.Fatal(e)
		}
		sequence++
		if event.Sequence != sequence || event.PrevHash != previous {
			r.t.Fatal("Chronicle chain linkage or sequence broken")
		}
		previous = event.Hash
		result, e := chain.VerifyWithPin(r.ctx, event.PrevHash, event, hash.Pin{Scheme: hash.SchemeHMACV5, Since: 1})
		if e != nil || !result.OK {
			r.t.Fatalf("HMAC verification failed: %v", e)
		}
	}
	if err := rows.Err(); err != nil {
		r.t.Fatal(err)
	}
}
func (r *processRig) negativeMatrix() {
	for _, role := range []string{"chronicle", "relay"} {
		before := r.count(role, "SELECT count(*) FROM "+role+"_acceptances")
		for _, tc := range []struct {
			name, token string
			headers     map[string]string
			status      int
		}{{"missing", "", nil, 401}, {"invalid", "invalid", nil, 401}, {"wrong-key", r.c.Credentials["operator"].Secret, nil, 401}, {"forged-app", r.c.Credentials[role].Secret, map[string]string{"X-App-ID": "other"}, 401}, {"forged-env", r.c.Credentials[role].Secret, map[string]string{"X-Environment-ID": "other"}, 401}, {"forged-org", r.c.Credentials[role].Secret, map[string]string{"X-Org-ID": "other"}, 401}, {"forged-tenant", r.c.Credentials[role].Secret, map[string]string{"X-Tenant-ID": "other"}, 401}, {"forged-installation", r.c.Credentials[role].Secret, map[string]string{"X-Installation-ID": "other"}, 401}} {
			r.run(role+"_"+tc.name, func(_ *testing.T) {
				r.post(role, "/accept", tc.token, []byte(`{}`), tc.status, tc.headers)
				r.equal(before, r.count(role, "SELECT count(*) FROM "+role+"_acceptances"), "rejected acceptance")
			})
		}
	}
}

func (r *processRig) run(name string, fn func(*testing.T)) {
	parent := r.t
	if !parent.Run(name, func(t *testing.T) { r.t = t; defer func() { r.t = parent }(); fn(t) }) {
		parent.FailNow()
	}
}

func (p *process) sample(ctx context.Context) {
	probe, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := exec.CommandContext(probe, "ps", "-o", "rss=", "-p", strconv.Itoa(p.cmd.Process.Pid)).Output()
	if err != nil {
		return
	}
	rss, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return
	}
	p.mu.Lock()
	if rss > p.peak {
		p.peak = rss
	}
	p.mu.Unlock()
	if rss > 384*1024 {
		p.cancel()
	}
}
