package engine

import (
	"context"
	"errors"
	"slices"
	"time"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cluster"
)

// defaultWorkerHeartbeat is the row heartbeat's interval when
// dispatch.Config.HeartbeatInterval is zero or negative. Zero there turns
// off job lease heartbeats; it is not a request to let this worker's row
// go stale, which would get it swept by the next instance to start and
// drop its capacity out of the fleet check.
const defaultWorkerHeartbeat = 10 * time.Second

// workerRow returns the cluster row this engine registers for itself: the
// fields Build computed, with LastSeen set to now. CreatedAt stays the
// Build-time value, so a row registered again after a sweep still says
// when this process came up.
func (eng *Engine) workerRow() *cluster.Worker {
	w := eng.self
	w.Queues = slices.Clone(eng.self.Queues)
	w.Capacity = eng.self.Capacity.Clone()
	w.LastSeen = time.Now().UTC()

	return &w
}

// workerHeartbeatInterval is how often the row heartbeat runs.
func (eng *Engine) workerHeartbeatInterval() time.Duration {
	if hb := eng.d.Config().HeartbeatInterval; hb > 0 {
		return hb
	}

	return defaultWorkerHeartbeat
}

// startHeartbeat keeps this worker's cluster row alive until
// stopHeartbeat.
//
// Build registers the row once. Without this loop its LastSeen is the
// registration time forever, so after five minutes the stale sweep every
// instance runs at startup deletes it, the leader's included, and the
// enqueue-time capacity check stops counting it. The loop beats once
// straight away, then every interval.
//
// The loop's context is detached from ctx: Start's caller may cancel ctx
// once Start returns, and the row must outlive that. Only stopHeartbeat
// ends it. A second Start while the loop runs is a no-op.
func (eng *Engine) startHeartbeat(ctx context.Context) {
	eng.heartbeatMu.Lock()
	defer eng.heartbeatMu.Unlock()

	if eng.heartbeatCancel != nil {
		return
	}

	hbCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	eng.heartbeatCancel = cancel
	eng.heartbeatDone = done

	interval := eng.workerHeartbeatInterval()

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		eng.beat(hbCtx, interval)

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				eng.beat(hbCtx, interval)
			}
		}
	}()
}

// stopHeartbeat ends the loop and waits for it to exit, or for ctx to
// end, whichever comes first. Safe when the loop never started and safe
// to call twice.
func (eng *Engine) stopHeartbeat(ctx context.Context) {
	eng.heartbeatMu.Lock()
	cancel, done := eng.heartbeatCancel, eng.heartbeatDone
	eng.heartbeatCancel, eng.heartbeatDone = nil, nil
	eng.heartbeatMu.Unlock()

	if cancel == nil {
		return
	}

	cancel()

	select {
	case <-done:
	case <-ctx.Done():
		eng.logger.Warn("worker heartbeat did not stop before the shutdown deadline",
			log.String("error", ctx.Err().Error()),
		)
	}
}

// beat refreshes the row once. A row that is gone, because another
// instance's stale sweep deleted it or because Build's registration
// failed, is registered again. Any other failure is logged and left to
// the next tick. Each call is bounded by one interval so a hung store
// call cannot stack beats behind it.
func (eng *Engine) beat(ctx context.Context, interval time.Duration) {
	callCtx, cancel := context.WithTimeout(ctx, interval)
	defer cancel()

	err := eng.clusterStore.HeartbeatWorker(callCtx, eng.pool.WorkerID())
	if err == nil || ctx.Err() != nil {
		return
	}

	if !errors.Is(err, dispatch.ErrWorkerNotFound) {
		eng.logger.Warn("worker heartbeat failed",
			log.String("worker_id", eng.pool.WorkerID().String()),
			log.String("error", err.Error()),
		)

		return
	}

	if regErr := eng.clusterStore.RegisterWorker(callCtx, eng.workerRow()); regErr != nil {
		if ctx.Err() == nil {
			eng.logger.Warn("worker row missing and registering it again failed",
				log.String("worker_id", eng.pool.WorkerID().String()),
				log.String("error", regErr.Error()),
			)
		}

		return
	}

	eng.logger.Info("worker row was missing; registered it again",
		log.String("worker_id", eng.pool.WorkerID().String()),
	)
}
