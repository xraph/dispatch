package engine

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Ready checks admission, polling, storage and the worker's cluster registration.
func (eng *Engine) Ready(ctx context.Context) error {
	if !eng.accepting.Load() {
		return errors.New("worker is starting or draining")
	}
	if err := eng.pool.Ready(); err != nil {
		return err
	}
	if err := eng.Health(ctx); err != nil {
		return err
	}
	if eng.clusterStore != nil {
		w, err := eng.clusterStore.GetWorker(ctx, eng.WorkerID())
		if err != nil {
			return fmt.Errorf("worker registration: %w", err)
		}
		threshold := eng.d.Config().StaleJobThreshold
		if threshold <= 0 {
			threshold = 3 * eng.d.Config().HeartbeatInterval
		}
		if w == nil || time.Since(w.LastSeen) > threshold {
			return errors.New("worker registration heartbeat is stale")
		}
	}
	return nil
}
