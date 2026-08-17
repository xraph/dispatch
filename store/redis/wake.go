package redis

import (
	"context"
	"fmt"

	dispatchstore "github.com/xraph/dispatch/store"
)

var _ dispatchstore.WakeNotifier = (*Store)(nil)

// notifyWake signals listening instances that pending jobs exist.
// Best-effort by design: polling remains the correctness mechanism, so a
// failed publish only costs poll latency and is not worth failing the
// enqueue over.
func (s *Store) notifyWake(ctx context.Context) {
	_ = s.kv.Publish(ctx, s.keys.wakeChannel(), nil) //nolint:errcheck // best-effort: polling covers missed wakes
}

// StartWakeListener subscribes to the dispatch wake channel and invokes
// wake for each message.
//
// The driver re-subscribes after connection loss, so there is no manual
// rebuild loop; messages published while the connection was down are
// simply lost, which polling covers. The returned stop function cancels
// the subscription.
func (s *Store) StartWakeListener(ctx context.Context, wake func()) (func(), error) {
	ctx, cancel := context.WithCancel(ctx)

	if err := s.kv.Subscribe(ctx, s.keys.wakeChannel(), func([]byte) { wake() }); err != nil {
		cancel()

		return nil, fmt.Errorf("dispatch/redis: start wake listener: %w", err)
	}

	return cancel, nil
}
