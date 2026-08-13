package engine_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/store/memory"
)

// TestReaperMarginAcceptsStockConfig is the compatibility floor. Turning
// the resource model on must not force anybody to retune their timings,
// so the shipped defaults have to clear the check with room to spare.
func TestReaperMarginAcceptsStockConfig(t *testing.T) {
	if err := engine.CheckReaperMarginForTest(dispatch.DefaultConfig()); err != nil {
		t.Fatalf("the default configuration must pass: %v", err)
	}
}

func TestReaperMargin(t *testing.T) {
	cases := []struct {
		name  string
		poll  time.Duration
		beat  time.Duration
		stale time.Duration
		ok    bool
	}{
		{
			// The failure this check exists for: a threshold at or below
			// the poll interval lets the reaper reclaim a job the fetcher
			// is still holding, and the job then runs twice.
			name: "threshold at the poll interval", poll: 30 * time.Second,
			beat: 10 * time.Second, stale: 30 * time.Second, ok: false,
		},
		{
			// Larger than the claim-to-first-heartbeat window, but with no
			// room for the heartbeat write itself to be slow.
			name: "no slack for a missed heartbeat", poll: 5 * time.Second,
			beat: 10 * time.Second, stale: 20 * time.Second, ok: false,
		},
		{
			name: "exactly twice the window", poll: 5 * time.Second,
			beat: 10 * time.Second, stale: 30 * time.Second, ok: true,
		},
		{
			// A reaper that is switched off cannot reclaim anything, so
			// there is no relationship left to police.
			name: "reaper disabled", poll: time.Hour,
			beat: time.Hour, stale: 0, ok: true,
		},
		{
			// Heartbeats off: the window is the admission stall alone.
			// Whether a never-heartbeating job survives its threshold is a
			// question that predates this model and is not ours.
			name: "heartbeats disabled", poll: time.Second,
			beat: 0, stale: 3 * time.Second, ok: true,
		},
		{
			name: "heartbeats disabled and threshold too tight", poll: 10 * time.Second,
			beat: 0, stale: 10 * time.Second, ok: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := engine.CheckReaperMarginForTest(dispatch.Config{
				PollInterval:      tc.poll,
				HeartbeatInterval: tc.beat,
				StaleJobThreshold: tc.stale,
			})

			if tc.ok {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				return
			}

			if err == nil {
				t.Fatal("expected an error")
			}

			// The message has to name every value involved, because the
			// fix is a relationship between them and an operator reading
			// it should not have to go looking for the other two.
			for _, want := range []string{
				"StaleJobThreshold", "PollInterval", "HeartbeatInterval",
				tc.stale.String(), tc.poll.String(),
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not mention %q: %v", want, err)
				}
			}
		})
	}
}

// TestBuildRejectsUnsafeReaperMargin checks the validation actually runs
// at construction, and only when a manager makes the stall possible.
func TestBuildRejectsUnsafeReaperMargin(t *testing.T) {
	newDispatcher := func(t *testing.T) *dispatch.Dispatcher {
		t.Helper()

		d, err := dispatch.New(
			dispatch.WithStore(memory.New()),
			dispatch.WithPollInterval(20*time.Second),
			dispatch.WithHeartbeatInterval(10*time.Second),
			dispatch.WithStaleJobThreshold(30*time.Second),
		)
		if err != nil {
			t.Fatalf("dispatch.New: %v", err)
		}

		return d
	}

	mgr := resource.NewManager(resource.Set{resource.Memory: 1 << 30})

	if _, err := engine.Build(newDispatcher(t), engine.WithResourceManager(mgr)); err == nil {
		t.Fatal("Build accepted a configuration the reaper can corrupt")
	}

	// The same timings without a manager are none of this check's
	// business: with no ledger, admission never waits.
	if _, err := engine.Build(newDispatcher(t)); err != nil {
		t.Fatalf("Build without a manager must not be validated: %v", err)
	}
}

// TestBuildDoesNotDeriveCapacityFromTheManager pins the ruling that
// replaced the old "publish what admission enforces" convenience.
//
// The convenience looked like it kept two numbers from drifting. What it
// actually did was answer a FLEET question with a LOCAL number: the
// unschedulable check asks "is any worker big enough", and seeding its
// floor from this process's ledger silently rescoped that to "is THIS
// process big enough". Nothing could raise it back afterwards, because
// cluster.Worker.Capacity round-trips on store/memory alone — postgres,
// sqlite, mongo and redis all enumerate worker fields by hand and drop
// it. On those four the check collapsed entirely to the local manager,
// so a light API pod with resources enabled hard-rejected at enqueue the
// tessellation job the heavy tier runs perfectly well.
//
// A manager is now just a manager. The check is inert until an operator
// states the fleet ceiling out loud.
func TestBuildDoesNotDeriveCapacityFromTheManager(t *testing.T) {
	capacity := resource.Set{resource.Memory: 8 << 30, "fpga": 2}

	d, err := dispatch.New(dispatch.WithStore(memory.New()))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng, err := engine.Build(d, engine.WithResourceManager(resource.NewManager(capacity)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if got := eng.MaxWorkerCapacity(t.Context()); len(got) != 0 {
		t.Errorf("MaxWorkerCapacity = %v, want empty: installing a ledger must not turn the "+
			"fleet-wide unschedulable check on with this process's own numbers", got)
	}

	// And with it off, a job larger than this process still enqueues —
	// some other worker may be able to run it.
	huge := resource.Set{resource.Memory: 64 << 30}

	j, err := eng.EnqueueRaw(t.Context(), "huge", []byte(`{}`), job.WithResources(huge))
	if err != nil {
		t.Fatalf("EnqueueRaw of a job larger than this worker: %v", err)
	}

	if j.Resources[resource.Memory] != 64<<30 {
		t.Errorf("stored requirement = %v, want the declaration intact", j.Resources)
	}

	// An explicit declaration is what turns the check on, and it is then
	// enforced.
	d2, err := dispatch.New(dispatch.WithStore(memory.New()))
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	eng2, err := engine.Build(d2,
		engine.WithResourceManager(resource.NewManager(capacity)),
		engine.WithWorkerCapacity(resource.Set{resource.Memory: 1 << 30}),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if got := eng2.MaxWorkerCapacity(t.Context())[resource.Memory]; got != 1<<30 {
		t.Errorf("declared capacity was overwritten: memory = %d", got)
	}

	if _, err = eng2.EnqueueRaw(t.Context(), "huge", []byte(`{}`),
		job.WithResources(huge)); !errors.Is(err, resource.ErrUnschedulable) {
		t.Errorf("EnqueueRaw error = %v, want ErrUnschedulable once a ceiling is declared", err)
	}
}
