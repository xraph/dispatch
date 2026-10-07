package redis

import (
	"context"
	"net"
	"strings"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// SetListScanBudgetForTest lowers how many index members one paged list
// call on this store examines, so a test can make a filtered scan stop at
// its budget without writing thousands of rows.
func (s *Store) SetListScanBudgetForTest(n int) { s.scanBudget = n }

// commandLog records the Redis commands a store sends once it is attached.
type commandLog struct {
	mu        sync.Mutex
	singles   []string
	pipelines [][]string
}

func (l *commandLog) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (l *commandLog) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		l.mu.Lock()
		l.singles = append(l.singles, strings.ToLower(cmd.Name()))
		l.mu.Unlock()

		return next(ctx, cmd)
	}
}

func (l *commandLog) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		names := make([]string, len(cmds))
		for i, c := range cmds {
			names[i] = strings.ToLower(c.Name())
		}
		l.mu.Lock()
		l.pipelines = append(l.pipelines, names)
		l.mu.Unlock()

		return next(ctx, cmds)
	}
}

// RecordCommandsForTest starts recording every command this store sends.
// The returned function reports what was sent so far: the commands sent on
// their own, and each pipeline (a MULTI shows as multi ... exec).
func (s *Store) RecordCommandsForTest() func() (singles []string, pipelines [][]string) {
	l := &commandLog{}
	s.rdb.AddHook(l)

	return func() ([]string, [][]string) {
		l.mu.Lock()
		defer l.mu.Unlock()

		return append([]string(nil), l.singles...), append([][]string(nil), l.pipelines...)
	}
}
