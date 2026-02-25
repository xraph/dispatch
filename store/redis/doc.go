// Package redis implements store.Store using Grove KV backed by Redis.
// Suitable for high-throughput ephemeral workloads. Jobs use Sorted Sets as
// priority queues, events use Streams, and all entities are stored as JSON
// via Grove KV.
//
// The caller owns the *kv.Store lifecycle -- redis never closes it. Pass the
// KV store through the constructor:
//
//	import (
//	    "github.com/xraph/grove/kv"
//	    "github.com/xraph/dispatch/store/redis"
//	)
//
//	store := redis.New(kvStore)
//	if err := store.Ping(ctx); err != nil { ... }
package redis
