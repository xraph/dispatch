package contract

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch/cluster"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dlq"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/queue"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

func runOperationalDomain(t *testing.T, s store.Store) {
	t.Helper()
	manager := resource.NewManager(resource.Set{resource.Memory: 100})
	lease, ok := manager.TryAcquire("local-job", resource.Set{resource.Memory: 20})
	if !ok {
		t.Fatal("lease refused")
	}
	defer lease.Release()
	d := contractDeps(t, s, engine.WithResourceManager(manager), engine.WithQueueConfig(queue.Config{Name: "bounded", MaxConcurrency: 3, RateLimit: 10}))
	ctx := context.Background()
	p := fc.Principal{Claims: map[string]any{"scope_org_id": "not-the-seeded-org"}}
	remote := &cluster.Worker{ID: id.NewWorkerID(), Hostname: "remote-host", Queues: []string{"remote", "bounded"}, Concurrency: 4, State: cluster.WorkerActive,
		Capacity: resource.Set{resource.Memory: 200}, CreatedAt: time.Now().Add(-time.Hour), LastSeen: time.Now().Add(-10 * time.Minute)}
	if err := s.RegisterWorker(ctx, remote); err != nil {
		t.Fatal(err)
	}
	if acquired, err := s.AcquireLeadership(ctx, remote.ID, time.Minute); err != nil || !acquired {
		t.Fatalf("leader: %v, %v", acquired, err)
	}
	workers, err := workersListHandler(d)(ctx, EmptyInput{}, p)
	if err != nil || !workers.Enabled || len(workers.Items) != 2 || workers.LeaderID == nil || *workers.LeaderID != remote.ID.String() || workers.AsOf == "" || !workers.Complete || workers.NextCursor != nil {
		t.Fatalf("workers=%+v, %v", workers, err)
	}
	for _, row := range workers.Items {
		if row.ID == remote.ID.String() {
			if row.Self || !row.IsLeader || row.HeartbeatInterval != nil || row.HeartbeatStatus != "silent" || row.HeartbeatAge == nil || row.Capacity[resource.Memory] != 200 {
				t.Fatalf("remote=%+v", row)
			}
		} else if !row.Self || row.IsLeader || row.HeartbeatInterval == nil || row.HeartbeatStatus != "recent" {
			t.Fatalf("self=%+v", row)
		}
	}
	detail, err := workersGetHandler(d)(ctx, IDInput{ID: d.Engine.WorkerID().String()}, p)
	if err != nil || detail.Worker == nil || !detail.Resources.Enabled || detail.Resources.Capacity[resource.Memory] != 100 || detail.Resources.Free[resource.Memory] != 80 ||
		len(detail.Resources.Leases) != 1 || detail.Resources.Leases[0].Owner != "local-job" {
		t.Fatalf("local=%+v, %v", detail, err)
	}
	detail.Resources.Capacity[resource.Memory] = 0
	if manager.Capacity()[resource.Memory] != 100 {
		t.Fatal("resource projection aliases manager")
	}
	other, err := workersGetHandler(d)(ctx, IDInput{ID: remote.ID.String()}, p)
	if err != nil || other.Resources.Enabled || other.Resources.Leases == nil || len(other.Resources.Capacity) != 0 {
		t.Fatalf("remote resources=%+v, %v", other, err)
	}
	if _, readErr := workersGetHandler(d)(ctx, IDInput{ID: id.NewWorkerID().String()}, p); !errors.Is(readErr, fc.ErrNotFound) {
		t.Fatalf("missing worker=%v", readErr)
	}
	if _, readErr := workersGetHandler(d)(ctx, IDInput{ID: id.NewJobID().String()}, p); !errors.Is(readErr, fc.ErrBadRequest) {
		t.Fatalf("bad worker=%v", readErr)
	}
	states := []job.State{job.StatePending, job.StateRunning, job.StateCompleted, job.StateFailed, job.StateRetrying, job.StateCancelled}
	for _, state := range states {
		seedJob(t, d, "operation", state, "app", "org", "bounded")
	}
	seedJob(t, d, "remote", job.StatePending, "app", "org", "remote")
	if !d.Engine.QueueManager().Acquire("bounded", "") {
		t.Fatal("queue acquire")
	}
	defer d.Engine.QueueManager().Release("bounded", "")
	queues, err := queuesListHandler(d)(ctx, EmptyInput{}, p)
	if err != nil || len(queues.Items) != 3 || !queues.WorkerDiscoveryEnabled || queues.AsOf == "" {
		t.Fatalf("queues=%+v, %v", queues, err)
	}
	names := make([]string, 0, len(queues.Items))
	for _, row := range queues.Items {
		names = append(names, row.Name)
	}
	if !reflect.DeepEqual(names, []string{"bounded", "default", "remote"}) {
		t.Fatalf("queues=%v", names)
	}
	bounded, err := queuesGetHandler(d)(ctx, NameInput{Name: "bounded"}, p)
	if err != nil || bounded.Total != 6 || bounded.LocalSettings == nil || bounded.LocalSettings.MaxConcurrency != 3 || bounded.LocalSettings.EffectiveRateBurst == nil ||
		*bounded.LocalSettings.EffectiveRateBurst != 1 || bounded.LocalActiveCount == nil || *bounded.LocalActiveCount != 1 || bounded.PolledByThisProcess {
		t.Fatalf("bounded=%+v, %v", bounded, err)
	}
	for _, state := range states {
		if bounded.Counts[state] != 1 {
			t.Fatalf("counts=%v", bounded.Counts)
		}
	}
	defaultQueue, err := queuesGetHandler(d)(ctx, NameInput{Name: "default"}, p)
	if err != nil || !defaultQueue.PolledByThisProcess || defaultQueue.LocalActiveCount != nil || defaultQueue.LocalSettings != nil {
		t.Fatalf("default=%+v, %v", defaultQueue, err)
	}
	unknown, err := queuesGetHandler(d)(ctx, NameInput{Name: "historical"}, p)
	if err != nil || unknown.Total != 0 || unknown.LocalActiveCount != nil || unknown.AsOf == "" {
		t.Fatalf("historical queue=%+v, %v", unknown, err)
	}
	if _, readErr := queuesGetHandler(d)(ctx, NameInput{Name: " "}, p); !errors.Is(readErr, fc.ErrBadRequest) {
		t.Fatalf("blank queue=%v", readErr)
	}
	for _, state := range []workflow.RunState{workflow.RunStateRunning, workflow.RunStateCompleted, workflow.RunStateFailed} {
		seedWorkflow(t, d, "workflow", state, "app", "org", nil)
	}
	seedDLQ(t, d, "failed", "bounded", "app", "org", time.Now(), false)
	seedDLQ(t, d, "replayed", "bounded", "app", "org", time.Now(), true)
	seedCron(t, d, "enabled", "@every 1h", true)
	seedCron(t, d, "disabled", "@every 1h", false)
	summary, err := overviewSummaryHandler(d)(ctx, EmptyInput{}, p)
	if err != nil || summary.Jobs.Total != 7 || summary.UnreplayedDeadLetters != 1 || summary.Crons.Enabled != 1 || summary.Crons.Disabled != 1 ||
		!summary.Workers.Enabled || summary.Workers.Recent != 1 || summary.Workers.Silent != 1 || summary.Workers.Unknown != 0 || summary.AsOf == "" {
		t.Fatalf("summary=%+v, %v", summary, err)
	}
	for _, count := range summary.Runs {
		if count != 1 {
			t.Fatalf("runs=%v", summary.Runs)
		}
	}
}
func TestOperationalDomainMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runOperationalDomain(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runOperationalDomain(t, sqliteContractStore(t)) })
}
func TestWorkerProjectionUnknownClockSkewAndDetachedValues(t *testing.T) {
	now := time.Now()
	worker := &cluster.Worker{ID: id.NewWorkerID(), Queues: []string{"q"}, Capacity: resource.Set{resource.Memory: 10}}
	row := projectWorker(worker, worker.ID, nil, 10*time.Second, 5*time.Minute, now)
	if row.HeartbeatAge != nil || row.LastSeen != nil || row.HeartbeatStatus != "unknown" || row.ClockSkew {
		t.Fatalf("unknown=%+v", row)
	}
	row.Queues[0] = "changed"
	row.Capacity[resource.Memory] = 0
	if worker.Queues[0] != "q" || worker.Capacity[resource.Memory] != 10 {
		t.Fatal("projection aliases worker")
	}
	worker.LastSeen = now.Add(time.Minute)
	row = projectWorker(worker, worker.ID, nil, 10*time.Second, 5*time.Minute, now)
	if row.HeartbeatStatus != "unknown" || row.HeartbeatAge != nil || !row.ClockSkew {
		t.Fatalf("future=%+v", row)
	}
	worker.LastSeen = now.Add(-5 * time.Minute)
	row = projectWorker(worker, worker.ID, nil, 10*time.Second, 5*time.Minute, now)
	if row.HeartbeatStatus != "recent" {
		t.Fatalf("threshold equality=%+v", row)
	}
}
func TestWorkersDisabledCapabilityAndDefaultQueueManager(t *testing.T) {
	disabled := Deps{Engine: &engine.Engine{}, Store: memory.New()}
	page, err := workersListHandler(disabled)(context.Background(), EmptyInput{}, fc.Principal{})
	if err != nil || page.Enabled || page.Items == nil || page.LeaderID != nil || page.SilentAfter != nil {
		t.Fatalf("disabled=%+v, %v", page, err)
	}
	detail, err := workersGetHandler(disabled)(context.Background(), IDInput{ID: id.NewWorkerID().String()}, fc.Principal{})
	if err != nil || detail.Enabled || detail.Worker != nil || detail.Resources.Enabled || detail.Resources.Leases == nil {
		t.Fatalf("disabled detail=%+v, %v", detail, err)
	}
	d := contractDeps(t, memory.New())
	queues, err := queuesListHandler(d)(context.Background(), EmptyInput{}, fc.Principal{})
	if err != nil || len(queues.Items) != 1 || queues.Items[0].LocalSettings != nil || queues.Items[0].LocalActiveCount != nil {
		t.Fatalf("default queues=%+v, %v", queues, err)
	}
}

type operationalReadFailure struct {
	store.Store
	method   string
	deadline bool
}

func (s *operationalReadFailure) fail(ctx context.Context, method string) error {
	if s.method != method {
		return nil
	}
	_, s.deadline = ctx.Deadline()
	return errors.New("storage credential must stay private")
}
func (s *operationalReadFailure) ListWorkers(ctx context.Context) ([]*cluster.Worker, error) {
	if err := s.fail(ctx, "workers"); err != nil {
		return nil, err
	}
	return s.Store.ListWorkers(ctx)
}
func (s *operationalReadFailure) GetLeader(ctx context.Context) (*cluster.Worker, error) {
	if err := s.fail(ctx, "leader"); err != nil {
		return nil, err
	}
	return s.Store.GetLeader(ctx)
}
func (s *operationalReadFailure) CountJobs(ctx context.Context, opts job.CountOpts) (int64, error) {
	if err := s.fail(ctx, "jobs"); err != nil {
		return 0, err
	}
	return s.Store.CountJobs(ctx, opts)
}
func (s *operationalReadFailure) CountRuns(ctx context.Context, opts workflow.CountRunsOpts) (int64, error) {
	if err := s.fail(ctx, "runs"); err != nil {
		return 0, err
	}
	return s.Store.CountRuns(ctx, opts)
}
func (s *operationalReadFailure) CountDLQEntries(ctx context.Context, opts dlq.CountOpts) (int64, error) {
	if err := s.fail(ctx, "dlq"); err != nil {
		return 0, err
	}
	return s.Store.CountDLQEntries(ctx, opts)
}
func (s *operationalReadFailure) ListCrons(ctx context.Context) ([]*cron.Entry, error) {
	if err := s.fail(ctx, "crons"); err != nil {
		return nil, err
	}
	return s.Store.ListCrons(ctx)
}
func TestOperationalReadFailuresRemainErrors(t *testing.T) {
	for _, method := range []string{"workers", "leader", "jobs", "runs", "dlq", "crons"} {
		t.Run(method, func(t *testing.T) {
			s := &operationalReadFailure{Store: memory.New()}
			d := contractDeps(t, s)
			s.method = method
			_, err := overviewSummaryHandler(d)(context.Background(), EmptyInput{}, fc.Principal{})
			if !errors.Is(err, fc.ErrInternal) || strings.Contains(err.Error(), "credential") || !s.deadline {
				t.Fatalf("error=%v deadline=%v", err, s.deadline)
			}
			if method == "workers" || method == "jobs" {
				if _, readErr := queuesListHandler(d)(context.Background(), EmptyInput{}, fc.Principal{}); !errors.Is(readErr, fc.ErrInternal) {
					t.Fatalf("queues=%v", readErr)
				}
			}
			if method == "workers" || method == "leader" {
				if _, readErr := workersListHandler(d)(context.Background(), EmptyInput{}, fc.Principal{}); !errors.Is(readErr, fc.ErrInternal) {
					t.Fatalf("workers=%v", readErr)
				}
			}
		})
	}
}
