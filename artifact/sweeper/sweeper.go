package sweeper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/artifact"
)

// Defaults for a sweeper built without options.
const (
	DefaultRetention  = 168 * time.Hour
	DefaultPurgeGrace = 24 * time.Hour
	DefaultInterval   = 15 * time.Minute
	DefaultBatchSize  = 500
)

// Result reports what one pass did.
type Result struct {
	// Swept is how many artifacts were marked deleted.
	Swept int
	// Purged is how many had their bytes removed.
	Purged int
	// BytesReclaimed counts the bytes freed by purging.
	BytesReclaimed int64
	// Skipped counts artifacts a pass declined to act on, which is where
	// a backend failure shows up: the next pass retries them.
	Skipped int
}

// Observer is notified for each artifact a pass acts on, so lifecycle
// events reach the extension registry without this package depending on
// it.
type Observer interface {
	ArtifactSwept(ctx context.Context, a *artifact.Artifact)
	ArtifactPurged(ctx context.Context, a *artifact.Artifact)
}

// Sweeper reclaims ephemeral artifacts whose owners have finished.
type Sweeper struct {
	store   artifact.Store
	backend artifact.Backend
	logger  log.Logger

	retention   time.Duration
	purgeGrace  time.Duration
	interval    time.Duration
	batchSize   int
	dryRun      bool
	enabled     bool
	isLeader    func() bool
	observer    Observer
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	startedOnce sync.Once
}

// Option configures a Sweeper.
type Option func(*Sweeper)

// WithRetention sets how long an ephemeral artifact survives after its
// last owner reaches a terminal state.
func WithRetention(d time.Duration) Option {
	return func(s *Sweeper) { s.retention = d }
}

// WithPurgeGrace sets how long a soft-deleted artifact's bytes survive.
// This is the window in which a mistaken sweep can still be caught.
func WithPurgeGrace(d time.Duration) Option {
	return func(s *Sweeper) { s.purgeGrace = d }
}

// WithInterval sets how often the background loop runs.
func WithInterval(d time.Duration) Option {
	return func(s *Sweeper) { s.interval = d }
}

// WithBatchSize caps how many artifacts one pass may touch.
func WithBatchSize(n int) Option {
	return func(s *Sweeper) { s.batchSize = n }
}

// WithDryRun reports what would be swept without changing anything.
func WithDryRun(dry bool) Option {
	return func(s *Sweeper) { s.dryRun = dry }
}

// WithEnabled is the kill switch. A disabled sweeper's passes are no-ops,
// so reclamation can be stopped without redeploying.
func WithEnabled(enabled bool) Option {
	return func(s *Sweeper) { s.enabled = enabled }
}

// WithLeaderCheck restricts sweeping to the elected leader. Without one,
// every worker in a fleet would sweep concurrently.
func WithLeaderCheck(fn func() bool) Option {
	return func(s *Sweeper) { s.isLeader = fn }
}

// WithObserver receives a callback per artifact acted on.
func WithObserver(o Observer) Option {
	return func(s *Sweeper) { s.observer = o }
}

// WithLogger sets the logger.
func WithLogger(l log.Logger) Option {
	return func(s *Sweeper) { s.logger = l }
}

// New creates a Sweeper. It is enabled by default; use WithEnabled(false)
// to build one that does nothing until switched on.
func New(store artifact.Store, backend artifact.Backend, opts ...Option) *Sweeper {
	s := &Sweeper{
		store:      store,
		backend:    backend,
		logger:     log.NewNoopLogger(),
		retention:  DefaultRetention,
		purgeGrace: DefaultPurgeGrace,
		interval:   DefaultInterval,
		batchSize:  DefaultBatchSize,
		enabled:    true,
		stopCh:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// active reports whether this instance should act right now.
func (s *Sweeper) active() bool {
	if !s.enabled {
		return false
	}

	if s.isLeader != nil && !s.isLeader() {
		return false
	}

	return true
}

// SweepOnce marks eligible ephemeral artifacts deleted.
//
// Two passes run: artifacts whose owners have all finished and whose
// retention has elapsed, then orphans — artifacts with no links at all,
// which can only result from a partial failure during creation.
func (s *Sweeper) SweepOnce(ctx context.Context) (Result, error) {
	var res Result

	if !s.active() {
		return res, nil
	}

	eligible, err := s.store.SweepEphemeral(ctx, artifact.SweepOpts{
		Retention: s.retention,
		Limit:     s.batchSize,
		DryRun:    s.dryRun,
	})
	if err != nil {
		return res, fmt.Errorf("dispatch/artifact/sweeper: sweep ephemeral: %w", err)
	}

	res.Swept += s.report(ctx, eligible, "retention")

	cutoff := time.Now().UTC().Add(-s.orphanGrace())

	if !s.dryRun {
		orphans, oerr := s.store.SweepOrphans(ctx, cutoff, s.batchSize)
		if oerr != nil {
			return res, fmt.Errorf("dispatch/artifact/sweeper: sweep orphans: %w", oerr)
		}

		res.Swept += s.report(ctx, orphans, "orphan")
	}

	return res, nil
}

// orphanGrace is how long a link-less artifact is tolerated. It is
// deliberately generous: an artifact is linked in the same operation that
// creates it, so a zero-link artifact means a crash mid-create, and a
// short window risks sweeping one that is merely mid-flight.
func (s *Sweeper) orphanGrace() time.Duration {
	if s.purgeGrace > 0 {
		return s.purgeGrace
	}

	return DefaultPurgeGrace
}

// report emits observer callbacks and counts artifacts, refusing to act
// on anything that is not ephemeral.
func (s *Sweeper) report(ctx context.Context, artifacts []*artifact.Artifact, reason string) int {
	n := 0

	for _, a := range artifacts {
		// The store already constrains its sweep queries to ephemeral.
		// Re-checking here means a bug in one backend's query cannot turn
		// into deleted customer data.
		if a.Lifecycle != artifact.Ephemeral {
			s.logger.Error("dispatch/artifact/sweeper: refusing to sweep a non-ephemeral artifact",
				log.String("artifact_id", a.ID.String()),
				log.String("lifecycle", string(a.Lifecycle)),
			)

			continue
		}

		n++

		if s.observer != nil && !s.dryRun {
			s.observer.ArtifactSwept(ctx, a)
		}

		s.logger.Debug("dispatch/artifact/sweeper: swept artifact",
			log.String("artifact_id", a.ID.String()),
			log.String("reason", reason),
			log.Int64("bytes", a.Size),
		)
	}

	return n
}

// PurgeOnce removes the bytes of artifacts soft-deleted longer ago than
// the grace period, then deletes their rows.
//
// A backend failure skips that artifact rather than aborting the pass, so
// one unreachable object cannot stall reclamation of everything else. The
// next pass retries it.
func (s *Sweeper) PurgeOnce(ctx context.Context) (Result, error) {
	var res Result

	if !s.active() || s.dryRun {
		return res, nil
	}

	purgeable, err := s.store.ListPurgeable(ctx, s.purgeGrace, s.batchSize)
	if err != nil {
		return res, fmt.Errorf("dispatch/artifact/sweeper: list purgeable: %w", err)
	}

	for _, a := range purgeable {
		if a.Lifecycle != artifact.Ephemeral {
			s.logger.Error("dispatch/artifact/sweeper: refusing to purge a non-ephemeral artifact",
				log.String("artifact_id", a.ID.String()),
				log.String("lifecycle", string(a.Lifecycle)),
			)

			res.Skipped++

			continue
		}

		if derr := s.backend.Delete(ctx, a.Ref()); derr != nil {
			s.logger.Warn("dispatch/artifact/sweeper: could not delete object; will retry",
				log.String("artifact_id", a.ID.String()),
				log.String("error", derr.Error()),
			)

			res.Skipped++

			continue
		}

		if perr := s.store.PurgeArtifact(ctx, a.ID); perr != nil {
			// The bytes are gone but the row remains. The next pass finds
			// it again and the backend's delete-missing-is-not-an-error
			// contract makes the retry safe.
			s.logger.Warn("dispatch/artifact/sweeper: could not purge row; will retry",
				log.String("artifact_id", a.ID.String()),
				log.String("error", perr.Error()),
			)

			res.Skipped++

			continue
		}

		res.Purged++
		res.BytesReclaimed += a.Size

		if s.observer != nil {
			s.observer.ArtifactPurged(ctx, a)
		}
	}

	return res, nil
}

// RunOnce performs a sweep followed by a purge.
func (s *Sweeper) RunOnce(ctx context.Context) (Result, error) {
	swept, err := s.SweepOnce(ctx)
	if err != nil {
		return swept, err
	}

	purged, err := s.PurgeOnce(ctx)
	if err != nil {
		return swept, err
	}

	return Result{
		Swept:          swept.Swept,
		Purged:         purged.Purged,
		BytesReclaimed: purged.BytesReclaimed,
		Skipped:        swept.Skipped + purged.Skipped,
	}, nil
}

// Start begins the background loop. It is safe to call once.
func (s *Sweeper) Start(ctx context.Context) error {
	s.startedOnce.Do(func() {
		s.wg.Add(1)

		go s.loop(ctx)
	})

	return nil
}

func (s *Sweeper) loop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := s.RunOnce(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				s.logger.Error("dispatch/artifact/sweeper: pass failed",
					log.String("error", err.Error()))

				continue
			}

			if res.Swept > 0 || res.Purged > 0 {
				s.logger.Info("dispatch/artifact/sweeper: reclaimed storage",
					log.Int("swept", res.Swept),
					log.Int("purged", res.Purged),
					log.Int64("bytes_reclaimed", res.BytesReclaimed),
					log.Int("skipped", res.Skipped),
				)
			}
		}
	}
}

// Stop halts the background loop and waits for it to finish.
func (s *Sweeper) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopCh) })

	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
