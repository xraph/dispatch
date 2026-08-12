package sweeper_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/artifact/artifacttest"
	"github.com/xraph/dispatch/artifact/sweeper"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store/memory"
)

type rig struct {
	store   artifact.Store
	backend *artifacttest.Backend
	svc     *artifact.Service
}

func newRig(t *testing.T) *rig {
	t.Helper()

	st := memory.New()
	b := artifacttest.NewBackend()
	svc := artifact.NewService(st, b,
		artifact.WithEphemeralPrefix("ephemeral"),
		artifact.WithDefaultBucket("dispatch"))

	return &rig{store: st, backend: b, svc: svc}
}

// registerDurable creates an application-owned artifact — the kind the
// sweeper must never touch.
func (r *rig) registerDurable(t *testing.T, key string, data []byte) artifact.Ref {
	t.Helper()

	r.backend.Put("customer", key, data)

	ref, err := r.svc.Register(context.Background(), "customer", key)
	if err != nil {
		t.Fatalf("Register %q: %v", key, err)
	}

	return ref
}

// createEphemeral creates a Dispatch-owned artifact aged into the past.
func (r *rig) createEphemeral(t *testing.T, owner artifact.OwnerRef, name string, age time.Duration) artifact.Ref {
	t.Helper()

	ctx := context.Background()

	w, err := r.svc.Create(ctx, owner, 0, name)
	if err != nil {
		t.Fatalf("Create %q: %v", name, err)
	}

	if _, werr := w.Write([]byte("payload")); werr != nil {
		t.Fatalf("Write: %v", werr)
	}

	ref, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if age > 0 {
		a, gerr := r.store.GetArtifact(ctx, ref.ID)
		if gerr != nil {
			t.Fatalf("GetArtifact: %v", gerr)
		}

		a.CreatedAt = time.Now().UTC().Add(-age)

		if uerr := r.store.UpdateArtifact(ctx, a); uerr != nil {
			t.Fatalf("UpdateArtifact: %v", uerr)
		}
	}

	return ref
}

func newOwner() artifact.OwnerRef {
	return artifact.OwnerRef{Kind: artifact.OwnerJob, ID: id.NewJobID().String()}
}

// TestSweeperNeverTouchesDurable is the safety property the whole design
// rests on. It generates arbitrary sequences of create, fail, retry, and
// sweep, then asserts every durable artifact is still retrievable and its
// bytes still readable.
func TestSweeperNeverTouchesDurable(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	// A fixed seed so a failure reproduces exactly.
	rng := rand.New(rand.NewSource(1))

	durable := make([]artifact.Ref, 0, 8)
	for i := range 8 {
		durable = append(durable, r.registerDurable(t,
			fmt.Sprintf("upload-%d.ifc", i), []byte(fmt.Sprintf("customer data %d", i))))
	}

	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0),
		sweeper.WithBatchSize(100))

	for range 60 {
		switch rng.Intn(5) {
		case 0:
			owner := newOwner()
			r.createEphemeral(t, owner, fmt.Sprintf("out-%d.bin", rng.Intn(1000)),
				time.Duration(rng.Intn(72))*time.Hour)

		case 1:
			// A retried job producing the same name at a later attempt.
			owner := newOwner()
			for attempt := range 2 {
				w, err := r.svc.Create(ctx, owner, attempt, "page.png")
				if err != nil {
					t.Fatalf("Create attempt %d: %v", attempt, err)
				}

				if _, werr := w.Write([]byte("pixels")); werr != nil {
					t.Fatalf("Write: %v", werr)
				}

				if _, cerr := w.Commit(ctx); cerr != nil {
					t.Fatalf("Commit: %v", cerr)
				}
			}

		case 2:
			// An aborted write leaves nothing behind.
			w, err := r.svc.Create(ctx, newOwner(), 0, "aborted.bin")
			if err != nil {
				t.Fatalf("Create: %v", err)
			}

			if aerr := w.Abort(); aerr != nil {
				t.Fatalf("Abort: %v", aerr)
			}

		case 3:
			if _, err := s.SweepOnce(ctx); err != nil {
				t.Fatalf("SweepOnce: %v", err)
			}

		case 4:
			if _, err := s.PurgeOnce(ctx); err != nil {
				t.Fatalf("PurgeOnce: %v", err)
			}
		}
	}

	// Hammer it once more, with every window wide open.
	for range 3 {
		if _, err := s.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
	}

	for i, ref := range durable {
		a, err := r.store.GetArtifact(ctx, ref.ID)
		if err != nil {
			t.Fatalf("durable artifact %d (%v) was destroyed: %v", i, ref.ID, err)
		}

		if a.IsDeleted() {
			t.Fatalf("durable artifact %d was soft-deleted", i)
		}

		if !r.backend.Has(ref.Bucket, ref.Key) {
			t.Fatalf("durable artifact %d had its bytes purged from the backend", i)
		}
	}
}

func TestSweeperTwoPhaseDeletion(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	ref := r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)

	// Phase one: mark deleted. The bytes must survive.
	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(time.Hour))

	res, err := s.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}

	if res.Swept != 1 {
		t.Fatalf("Swept = %d, want 1", res.Swept)
	}

	if !r.backend.Has(ref.Bucket, ref.Key) {
		t.Fatal("sweep removed the bytes — phase one must only mark")
	}

	// With a grace window longer than the deletion's age, purging is a
	// no-op: this is the window in which a mistake can be caught.
	purged, err := s.PurgeOnce(ctx)
	if err != nil {
		t.Fatalf("PurgeOnce: %v", err)
	}

	if purged.Purged != 0 {
		t.Fatalf("Purged = %d during the grace window, want 0", purged.Purged)
	}

	// Phase two, grace elapsed.
	s2 := sweeper.New(r.store, r.backend, sweeper.WithPurgeGrace(0))

	purged, err = s2.PurgeOnce(ctx)
	if err != nil {
		t.Fatalf("PurgeOnce after grace: %v", err)
	}

	if purged.Purged != 1 {
		t.Fatalf("Purged = %d, want 1", purged.Purged)
	}

	if r.backend.Has(ref.Bucket, ref.Key) {
		t.Fatal("purge did not remove the bytes")
	}

	if _, err := r.store.GetArtifact(ctx, ref.ID); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("GetArtifact after purge = %v, want ErrNotFound", err)
	}
}

func TestSweeperDryRunChangesNothing(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	ref := r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)

	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0),
		sweeper.WithDryRun(true))

	res, err := s.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if res.Purged != 0 {
		t.Fatalf("dry run purged %d artifacts", res.Purged)
	}

	a, err := r.store.GetArtifact(ctx, ref.ID)
	if err != nil {
		t.Fatalf("dry run destroyed the artifact: %v", err)
	}

	if a.IsDeleted() {
		t.Fatal("dry run soft-deleted the artifact")
	}
}

func TestSweeperKillSwitch(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	ref := r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)

	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0),
		sweeper.WithEnabled(false))

	res, err := s.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if res.Swept != 0 || res.Purged != 0 {
		t.Fatalf("a disabled sweeper acted: %+v", res)
	}

	if _, err := r.store.GetArtifact(ctx, ref.ID); err != nil {
		t.Fatalf("disabled sweeper destroyed the artifact: %v", err)
	}
}

func TestSweeperLeaderOnly(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)

	leader := false

	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0),
		sweeper.WithLeaderCheck(func() bool { return leader }))

	res, err := s.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce as follower: %v", err)
	}

	if res.Swept != 0 {
		t.Fatalf("a follower swept %d artifacts", res.Swept)
	}

	leader = true

	res, err = s.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce as leader: %v", err)
	}

	if res.Swept != 1 {
		t.Fatalf("the leader swept %d artifacts, want 1", res.Swept)
	}
}

// TestPurgeSkipsOnBackendFailureAndRetries checks that one unreachable
// object cannot stall reclamation of everything else.
func TestPurgeSkipsOnBackendFailureAndRetries(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	ref := r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)

	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0))

	if _, err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}

	// Remove the bytes behind the sweeper's back. Delete-missing must not
	// be an error, so the purge still completes.
	if derr := r.backend.Delete(ctx, ref); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}

	res, err := s.PurgeOnce(ctx)
	if err != nil {
		t.Fatalf("PurgeOnce: %v", err)
	}

	if res.Purged != 1 {
		t.Fatalf("Purged = %d, want 1 — a missing object must not block the purge", res.Purged)
	}
}

type countingObserver struct {
	swept  int
	purged int
}

func (o *countingObserver) ArtifactSwept(context.Context, *artifact.Artifact)  { o.swept++ }
func (o *countingObserver) ArtifactPurged(context.Context, *artifact.Artifact) { o.purged++ }

func TestSweeperNotifiesObserver(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	r.createEphemeral(t, newOwner(), "temp.bin", 48*time.Hour)

	obs := &countingObserver{}

	s := sweeper.New(r.store, r.backend,
		sweeper.WithRetention(0),
		sweeper.WithPurgeGrace(0),
		sweeper.WithObserver(obs))

	if _, err := s.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if obs.swept != 1 {
		t.Fatalf("observer saw %d sweeps, want 1", obs.swept)
	}

	if obs.purged != 1 {
		t.Fatalf("observer saw %d purges, want 1", obs.purged)
	}
}

func TestSweeperStartStop(t *testing.T) {
	ctx := context.Background()
	r := newRig(t)

	s := sweeper.New(r.store, r.backend, sweeper.WithInterval(10*time.Millisecond))

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Stop must be idempotent so a double shutdown does not panic.
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}
