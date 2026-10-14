//go:build linux

package managedhost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/managed"

	"github.com/xraph/dispatch/durable"
	dpg "github.com/xraph/dispatch/store/postgres"
)

const livePromotionImage = "postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24"
const livePromotionPGData = "/var/lib/postgresql/data/pgdata"

func livePromotionID(t *testing.T, raw []byte) string {
	t.Helper()
	value := strings.TrimSpace(string(raw))
	if len(value) != 64 || strings.Trim(value, "0123456789abcdef") != "" {
		t.Fatal("Docker did not return an exact resource ID")
	}
	return value
}

// Cleanup has an independent bounded context because testing cancels t.Context
// before cleanup. Only exact IDs returned to this invocation are removed.
func livePromotionCleanup(t *testing.T, kind, resource string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		args := []string{"--host", liveRestoreDockerHost, kind, "rm"}
		if kind == "container" {
			args = append(args, "--force", "--volumes")
		}
		args = append(args, resource)
		output := &liveRestoreBuffer{limit: 64 << 10, cancel: cancel}
		command := exec.CommandContext(ctx, "docker", args...)
		command.Stdout, command.Stderr = output, output
		command.WaitDelay = 2 * time.Second
		if err := command.Run(); err != nil {
			t.Errorf("exact promotion %s cleanup failed for %s: %v", kind, resource, err)
		}
	})
}

func livePromotionContainer(t *testing.T, network, role string) (string, string) {
	t.Helper()
	args := []string{"run", "--detach", "--pull=never", "--name", "wf-promotion-" + role + "-" + liveRandom(t), "--network", network, "--network-alias", role, "--memory=384m", "--memory-swap=384m", "--cpus=1", "--pids-limit=128", "--publish", "127.0.0.1::5432", "--tmpfs", "/var/lib/postgresql/data:rw,size=256m,uid=70,gid=70,mode=0700", "--env", "PGDATA=" + livePromotionPGData, "--env", "POSTGRES_HOST_AUTH_METHOD=trust"}
	postgresArgs := []string{"-c", "shared_buffers=16MB", "-c", "max_connections=16", "-c", "wal_level=replica", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4", "-c", "max_wal_size=64MB", "-c", "min_wal_size=32MB", "-c", "wal_keep_size=64MB"}
	if role == "replica" {
		// UID/GID70 were read from the exact preloaded PostgreSQL17 Alpine image.
		// pg_basebackup writes only this new container's bounded tmpfs.
		args = append(args, "--user", "70:70", "--entrypoint", "sh", livePromotionImage, "-ec", `pg_basebackup --host=primary --username=postgres --pgdata="$PGDATA" --write-recovery-conf --wal-method=stream --checkpoint=fast --no-password; exec postgres "$@"`, "promotion-standby")
		args = append(args, postgresArgs...)
	} else {
		args = append(args, livePromotionImage, "postgres")
		args = append(args, postgresArgs...)
	}
	container := livePromotionID(t, liveRestoreDocker(t, nil, 1024, args...))
	livePromotionCleanup(t, "container", container)
	var inspected struct {
		ID     string `json:"Id"`
		Config struct {
			Image string `json:"Image"`
		} `json:"Config"`
		HostConfig struct {
			Memory      int64    `json:"Memory"`
			MemorySwap  int64    `json:"MemorySwap"`
			NanoCPUs    int64    `json:"NanoCpus"`
			PidsLimit   int64    `json:"PidsLimit"`
			NetworkMode string   `json:"NetworkMode"`
			Binds       []string `json:"Binds"`
		} `json:"HostConfig"`
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	raw := liveRestoreDocker(t, nil, 64<<10, "inspect", "--format", "{{json .}}", container)
	if err := json.Unmarshal(raw, &inspected); err != nil {
		t.Fatal(err)
	}
	if inspected.ID != container || inspected.Config.Image != livePromotionImage || inspected.HostConfig.Memory != 384<<20 || inspected.HostConfig.MemorySwap != 384<<20 || inspected.HostConfig.NanoCPUs != 1_000_000_000 || inspected.HostConfig.PidsLimit != 128 || inspected.HostConfig.NetworkMode != network || len(inspected.HostConfig.Binds) != 0 {
		t.Fatal("promotion container identity or isolation bounds differ")
	}
	ports := inspected.NetworkSettings.Ports["5432/tcp"]
	if len(ports) != 1 || ports[0].HostIP != "127.0.0.1" {
		t.Fatalf("PostgreSQL fixture port is not loopback-only: %+v", ports)
	}
	port, err := strconv.ParseUint(ports[0].HostPort, 10, 16)
	if err != nil || port == 0 {
		t.Fatal("invalid ephemeral PostgreSQL port")
	}
	t.Log("private promotion container", role, container, "image", livePromotionImage, "loopback port", port)
	return container, "postgres://postgres@" + net.JoinHostPort("127.0.0.1", ports[0].HostPort) + "/postgres?sslmode=disable"
}

func livePromotionWait(t *testing.T, label string, check func(context.Context) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		probe, stop := context.WithTimeout(ctx, time.Second)
		ok := check(probe)
		stop()
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", label)
		case <-ticker.C:
		}
	}
}

func livePromotionSQL(t *testing.T, dsn string, standby bool) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	livePromotionWait(t, "PostgreSQL17 readiness", func(ctx context.Context) bool {
		var version int
		var recovering bool
		err := db.QueryRowContext(ctx, "SELECT current_setting('server_version_num')::integer, pg_is_in_recovery()").Scan(&version, &recovering)
		return err == nil && version >= 170000 && version < 180000 && recovering == standby
	})
	return db
}

type livePromotionWrite struct {
	start        durable.StartRequest
	startReceipt durable.Receipt
	commit       durable.CommitRequest
	receipt      durable.Receipt
	history      []durable.Event
}

func livePromotionCommit(t *testing.T, store *dpg.Store, name string) livePromotionWrite {
	t.Helper()
	out := livePromotionWrite{start: durable.StartRequest{Key: durable.Key{Namespace: "promotion", WorkflowID: name, RunID: name}, RequestID: "start-" + name, WorkflowType: "promotion", BuildID: "promotion-v1", Queue: name, Input: []byte(`{"promotion":true}`)}}
	var err error
	out.startReceipt, err = store.StartExecution(t.Context(), out.start)
	if err != nil {
		t.Fatal(err)
	}
	task, err := store.ClaimTask(t.Context(), durable.ClaimRequest{Namespace: out.start.Namespace, Queue: name, Kind: durable.TaskWorkflow, Owner: "promotion-fixture", LeaseDuration: time.Minute})
	if err != nil || task == nil {
		t.Fatal("claim real durable task", err)
	}
	out.commit = durable.CommitRequest{Key: out.start.Key, RequestID: "commit-" + name, ExpectedRevision: 1, Token: task.Token(), Events: []durable.EventInput{{Type: "workflow.progress", Payload: []byte(name)}}}
	out.commit.IntentDigest, err = durable.Fingerprint("qualification.promotion.commit", out.commit)
	if err != nil {
		t.Fatal(err)
	}
	out.receipt, err = store.CommitTransition(t.Context(), out.commit)
	if err != nil {
		t.Fatal(err)
	}
	out.history, err = store.ReadHistory(t.Context(), out.start.Key, 0, 1000)
	if err != nil || len(out.history) == 0 || len(out.history) >= 1000 {
		t.Fatal("bounded history unavailable", err)
	}
	return out
}

func livePromotionVerify(t *testing.T, store *dpg.Store, written livePromotionWrite) {
	t.Helper()
	lookup := durable.ReceiptRequest{Key: written.start.Key, RequestID: written.commit.RequestID, IntentDigest: written.commit.IntentDigest}
	retained, found, err := store.LookupReceipt(t.Context(), lookup)
	if err != nil || !found || !reflect.DeepEqual(retained, written.receipt) {
		t.Fatal("promotion changed independent receipt", err)
	}
	start, err := store.StartExecution(t.Context(), written.start)
	if err != nil || !reflect.DeepEqual(start, written.startReceipt) {
		t.Fatal("promotion changed start replay", err)
	}
	commit, err := store.CommitTransition(t.Context(), written.commit)
	if err != nil || !reflect.DeepEqual(commit, written.receipt) {
		t.Fatal("promotion changed exact commit replay", err)
	}
	history, err := store.ReadHistory(t.Context(), written.start.Key, 0, 1000)
	if err != nil || !reflect.DeepEqual(history, written.history) {
		t.Fatal("promotion or replay changed durable history", err)
	}
	t.Log("promoted retained history", written.start.WorkflowID, "events", len(history), "sha256", managed.Digest(liveJSON(t, history)))
}

// This qualifies physical PostgreSQL promotion and durable data continuity. It
// does not authorize a restored service or roll back independent admission state.
func TestLivePostgresPhysicalReplicaPromotion(t *testing.T) {
	source := newLiveRestoreDatabase(t, "promotion_seed")
	seed := dpg.New(liveGrove(t, source.dsn))
	if err := seed.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	beforeBackup := livePromotionCommit(t, seed, "before-basebackup")
	snapshot := liveDumpDatabase(t, source)
	network := livePromotionID(t, liveRestoreDocker(t, nil, 1024, "network", "create", "wf-promotion-"+liveRandom(t)))
	livePromotionCleanup(t, "network", network)
	primary, primaryDSN := livePromotionContainer(t, network, "primary")
	primarySQL := livePromotionSQL(t, primaryDSN, false)
	liveRestoreDocker(t, bytes.NewReader(snapshot.raw), 64<<10, "exec", "-i", primary, "pg_restore", "--no-password", "--username=postgres", "--dbname=postgres", "--no-owner", "--no-acl", "--single-transaction", "--exit-on-error")
	// Only this isolated primary's replication channel opens. Its network has no
	// external route and the published SQL port is bound to guest loopback.
	liveRestoreDocker(t, nil, 1024, "exec", primary, "sh", "-ec", `printf '\nhost replication postgres 0.0.0.0/0 trust\n' >> "$PGDATA/pg_hba.conf"`)
	var reloaded bool
	if err := primarySQL.QueryRowContext(t.Context(), "SELECT pg_reload_conf()").Scan(&reloaded); err != nil || !reloaded {
		t.Fatal("reload isolated replication configuration", err)
	}
	replica, replicaDSN := livePromotionContainer(t, network, "replica")
	replicaSQL := livePromotionSQL(t, replicaDSN, true)
	primaryStore := dpg.New(liveGrove(t, primaryDSN))
	afterBackup := livePromotionCommit(t, primaryStore, "after-basebackup")
	var flushLSN string
	if err := primarySQL.QueryRowContext(t.Context(), "SELECT pg_current_wal_flush_lsn()::text").Scan(&flushLSN); err != nil {
		t.Fatal(err)
	}
	livePromotionWait(t, "standby replay of post-basebackup durable write", func(ctx context.Context) bool {
		var caughtUp bool
		err := replicaSQL.QueryRowContext(ctx, "SELECT pg_is_in_recovery() AND COALESCE(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)", flushLSN).Scan(&caughtUp)
		return err == nil && caughtUp
	})
	liveRestoreDocker(t, nil, 1024, "stop", "--time=10", primary)
	stopped := liveRestoreDocker(t, nil, 1024, "inspect", "--format", "{{.State.Running}}", primary)
	if strings.TrimSpace(string(stopped)) != "false" {
		t.Fatal("old primary still running before promotion")
	}
	liveRestoreDocker(t, nil, 64<<10, "exec", "--user", "70:70", replica, "pg_ctl", "--pgdata="+livePromotionPGData, "promote", "--wait", "--timeout=30")
	livePromotionWait(t, "promoted PostgreSQL primary", func(ctx context.Context) bool {
		var recovering bool
		return replicaSQL.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&recovering) == nil && !recovering
	})
	promoted := dpg.New(liveGrove(t, replicaDSN))
	livePromotionVerify(t, promoted, beforeBackup)
	livePromotionVerify(t, promoted, afterBackup)
	afterPromotion := livePromotionCommit(t, promoted, "after-promotion")
	livePromotionVerify(t, promoted, afterPromotion)
	t.Log("physical promotion verified", primary, replica, "replay LSN", flushLSN, "snapshot sha256", snapshot.digest)
}
