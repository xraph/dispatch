// Package main demonstrates resource-aware admission: one job definition
// whose memory requirement scales with its input, enqueued twice against
// a worker that cannot hold both at once.
//
// This is the case that identical worker slots get wrong. "Concurrency 4"
// says a worker may run four jobs; it says nothing about how big they
// are. A render of a 2 MiB scene and a render of a 24 MiB scene are the
// same job to a slot-counting pool, so it starts both, and on a box sized
// for the small one the large one takes the whole machine down with it.
//
// With a resource model the two are different sizes of the same work. The
// requirement is computed once, at enqueue, from the declared input; the
// worker admits against what is actually free; and the large render waits
// for room instead of being started next to work that is already using
// it.
//
// Usage:
//
//	go run ./_examples/resources
//
// Everything runs in-process: the memory store, the in-memory artifact
// backend Dispatch's own tests use, and a staging cache in a temp
// directory. No services to start, nothing to install.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/cache"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

const (
	mib = int64(1) << 20

	// workerMemory is everything this worker will admit at once. Real
	// deployments let resource.Detect read it from the cgroup; it is
	// pinned here so the example prints the same story on every machine.
	workerMemory = 512 * mib

	// stagingBudget is the staging cache's disk allowance, which with a
	// shared ledger IS the worker's disk capacity.
	stagingBudget = 128 * mib

	// A render holds its scene in memory several times over — decoded
	// geometry, working buffers, the framebuffer. baseMemory is the
	// interpreter and the runtime; the rest scales with the input.
	baseMemory      = 32 * mib
	memoryPerInputB = 16

	smallScene = 2 * mib
	largeScene = 24 * mib

	renderTime = 900 * time.Millisecond
	notifyTime = 700 * time.Millisecond
)

// scene is the job payload. The bytes it refers to are declared as an
// artifact input, not carried here, which is what lets the engine size
// the job before scheduling it.
type scene struct {
	Name string `json:"name"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clock := newTimeline()

	// ──────────────────────────────────────────────────
	// 1. One ledger, shared
	// ──────────────────────────────────────────────────
	//
	// This is the wiring that matters. The staging cache and the worker
	// pool must be given the SAME resource.Manager: the cache holds a
	// lease per cached entry and registers itself as the ledger's disk
	// reclaimer, and the pool offers disk at dequeue as free PLUS what
	// that reclaimer could evict. Hand the cache its own manager — which
	// it builds for itself if you do not supply one — and the pool's view
	// of reclaimable disk is permanently zero.

	capacity := resource.Detect(resource.CapacityConfig{
		DiskBytes: stagingBudget,
		Explicit: resource.Set{
			resource.CPU:    4 * resource.MilliScale,
			resource.Memory: workerMemory,
		},
	})

	resources := resource.NewManager(capacity)

	store := memory.New()

	d, err := dispatch.New(
		dispatch.WithStore(store),
		dispatch.WithLogger(log.NewNoopLogger()),
		// Four slots, deliberately more than this box can afford to fill.
		// The point of the example is that the limit is memory, not slots.
		dispatch.WithConcurrency(4),
		dispatch.WithPollInterval(200*time.Millisecond),
		dispatch.WithHeartbeatInterval(time.Second),
		dispatch.WithStaleJobThreshold(30*time.Second),
	)
	if err != nil {
		return fmt.Errorf("create dispatcher: %w", err)
	}

	backend := artifacttest.NewBackend()

	artifacts := artifact.NewService(store, backend,
		artifact.WithDefaultBucket("scenes"))

	staging, err := cache.New(mustTempDir(), backend, cache.WithManager(resources))
	if err != nil {
		return fmt.Errorf("create staging cache: %w", err)
	}

	eng, err := engine.Build(d,
		engine.WithArtifacts(artifacts, staging),
		engine.WithResourceManager(resources),
	)
	if err != nil {
		return fmt.Errorf("build engine: %w", err)
	}

	// ──────────────────────────────────────────────────
	// 2. One definition, sized from its input
	// ──────────────────────────────────────────────────

	var running atomic.Int64

	engine.Register(eng, job.NewDefinition("render",
		func(ctx context.Context, p scene) error {
			clock.log("render %-5s START   (%d running, %s admitted)",
				p.Name, running.Add(1), mb(held(resources)))
			defer running.Add(-1)

			sleep(ctx, renderTime)

			clock.log("render %-5s done", p.Name)

			return nil
		},
		job.WithArtifactInputs(artifact.Input("scene", artifact.Required)),

		// The requirement is computed once, in the enqueuing process,
		// from the size of the declared input. It never runs on the
		// scheduling path, so two workers can never disagree about how
		// big this job is.
		job.WithResourceFunc(func(_ context.Context, r resource.Request) (resource.Set, error) {
			return resource.MemoryBytes(baseMemory + r.InputBytes*memoryPerInputB), nil
		}),
		job.WithMaxRetries(0),
	))

	// Unrelated work already on the box. It is what the small render runs
	// alongside, and what the large render has to wait for.
	engine.Register(eng, job.NewDefinition("notify",
		func(ctx context.Context, p scene) error {
			clock.log("notify %-5s START   (%d running, %s admitted)",
				p.Name, running.Add(1), mb(held(resources)))
			defer running.Add(-1)

			sleep(ctx, notifyTime)

			clock.log("notify %-5s done", p.Name)

			return nil
		},
		job.WithResources(resource.MemoryBytes(32*mib)),
		job.WithMaxRetries(0),
	))

	// ──────────────────────────────────────────────────
	// 3. Two renders, one small and one large
	// ──────────────────────────────────────────────────

	small, err := upload(ctx, artifacts, backend, "small.scene", smallScene)
	if err != nil {
		return err
	}

	large, err := upload(ctx, artifacts, backend, "large.scene", largeScene)
	if err != nil {
		return err
	}

	fmt.Printf("worker capacity : %s memory, %s staging disk, %d slots\n",
		mb(capacity[resource.Memory]), mb(capacity[resource.Disk]), 4)
	fmt.Printf("render(small)   : %s input -> %s memory\n",
		mb(smallScene), mb(baseMemory+smallScene*memoryPerInputB))
	fmt.Printf("render(large)   : %s input -> %s memory\n",
		mb(largeScene), mb(baseMemory+largeScene*memoryPerInputB))
	fmt.Printf("notify          : %s memory each\n\n", mb(32*mib))

	// Enqueue order is the order the store hands them back at equal
	// priority, so the small render and the two notifies reach the worker
	// first and the large render arrives to find the box already busy.
	enqueued := []struct {
		name    string
		payload scene
		opts    []job.Option
	}{
		{"render", scene{Name: "small"}, []job.Option{engine.Bind("scene", small)}},
		{"notify", scene{Name: "a"}, nil},
		{"notify", scene{Name: "b"}, nil},
		{"render", scene{Name: "large"}, []job.Option{engine.Bind("scene", large)}},
	}

	for _, e := range enqueued {
		j, eerr := engine.Enqueue(ctx, eng, e.name, e.payload, e.opts...)
		if eerr != nil {
			return fmt.Errorf("enqueue %s: %w", e.name, eerr)
		}

		fmt.Printf("enqueued %-6s %-5s requiring %s\n",
			e.name, e.payload.Name, mb(j.Resources[resource.Memory]))
	}

	fmt.Println()

	// ──────────────────────────────────────────────────
	// 4. Watch the ordering
	// ──────────────────────────────────────────────────

	// Reset BEFORE Start. Every goroutine that reads the timeline is
	// created by Start or after it, so the write is ordered ahead of all
	// of them; resetting afterwards would race the workers already
	// logging against it.
	clock.reset()

	if serr := eng.Start(ctx); serr != nil {
		return fmt.Errorf("start engine: %w", serr)
	}

	// Sample the queue while the first three are running. This is the
	// observation the example exists to make: a job sitting in pending
	// with a worker slot free next to it.
	go func() {
		sleep(ctx, 400*time.Millisecond)
		reportPending(ctx, clock, store, resources)
	}()

	if werr := waitForDrain(ctx, store); werr != nil {
		return werr
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()

	if serr := eng.Stop(stopCtx); serr != nil {
		return fmt.Errorf("stop engine: %w", serr)
	}

	fmt.Println()
	fmt.Println("The large render was claimed in the first poll and refused: 416 MiB")
	fmt.Println("did not fit the 384 MiB left once the small render and both notifies")
	fmt.Println("were admitted. A fourth worker slot was free the whole time — slots")
	fmt.Println("were never the constraint. It ran once the box could hold it.")

	return nil
}

// upload seeds an object and registers it, returning the ref that carries
// its size. The size is what the resource func reads at enqueue.
func upload(
	ctx context.Context, svc *artifact.Service, backend *artifacttest.Backend,
	key string, size int64,
) (artifact.Ref, error) {
	backend.Put("scenes", key, make([]byte, size))

	ref, err := svc.Register(ctx, "scenes", key)
	if err != nil {
		return artifact.Ref{}, fmt.Errorf("register %s: %w", key, err)
	}

	return ref, nil
}

// reportPending prints what is waiting and why, mid-run.
func reportPending(
	ctx context.Context, clock *timeline, store *memory.Store, m resource.Manager,
) {
	jobs, err := store.ListJobsByState(ctx, job.StatePending, job.ListOpts{Limit: 100})
	if err != nil {
		return
	}

	free := m.Free()[resource.Memory]

	for _, j := range jobs {
		clock.log("%-6s %-5s PENDING — needs %s, %s free",
			j.Name, nameOf(j.Payload), mb(j.Resources[resource.Memory]), mb(free))
	}
}

// nameOf pulls the payload's name field for display.
func nameOf(payload []byte) string {
	var s scene
	if err := json.Unmarshal(payload, &s); err != nil {
		return "?"
	}

	return s.Name
}

// held is the memory currently spoken for by admitted jobs.
func held(m resource.Manager) int64 {
	return m.Capacity()[resource.Memory] - m.Free()[resource.Memory]
}

// waitForDrain blocks until no job is left pending, retrying, or running.
func waitForDrain(ctx context.Context, store *memory.Store) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("jobs did not drain: %w", ctx.Err())
		case <-ticker.C:
		}

		var outstanding int

		for _, state := range []job.State{job.StatePending, job.StateRetrying, job.StateRunning} {
			jobs, err := store.ListJobsByState(ctx, state, job.ListOpts{Limit: 100})
			if err != nil {
				return fmt.Errorf("list %s jobs: %w", state, err)
			}

			outstanding += len(jobs)
		}

		if outstanding == 0 {
			return nil
		}
	}
}

// sleep is a context-aware pause, so shutdown is not held up by a
// simulated render.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// timeline prints millisecond offsets from a resettable origin, which is
// what makes the ordering legible.
type timeline struct{ start time.Time }

func newTimeline() *timeline { return &timeline{start: time.Now()} }

func (t *timeline) reset() { t.start = time.Now() }

func (t *timeline) log(format string, args ...any) {
	fmt.Printf("%6dms  %s\n",
		time.Since(t.start).Milliseconds(), fmt.Sprintf(format, args...))
}

func mb(bytes int64) string {
	return fmt.Sprintf("%d MiB", bytes/mib)
}

func mustTempDir() string {
	dir, err := os.MkdirTemp("", "dispatch-resources-example")
	if err != nil {
		panic(err)
	}

	return dir
}
