package redis

import (
	"context"
	"strings"
	"sync"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv"
)

// SetListScanBudgetForTest lowers how many index members one paged list
// call on this store examines, so a test can make a filtered scan stop at
// its budget without writing thousands of rows.
func (s *Store) SetListScanBudgetForTest(n int) { s.scanBudget = n }

// commandLog records the kv operations a store sends once it is attached.
type commandLog struct {
	mu   sync.Mutex
	sent []string
}

// BeforeQuery implements hook.PreQueryHook.
func (l *commandLog) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	l.mu.Lock()
	l.sent = append(l.sent, strings.ToLower(kv.CommandName(qc.Operation)))
	l.mu.Unlock()

	return nil, nil
}

// RecordCommandsForTest starts recording every kv operation this store
// sends. The returned function reports them in order, lower-cased. A
// script shows as one "eval", whatever it runs on the server.
func (s *Store) RecordCommandsForTest() func() []string {
	l := &commandLog{}
	s.kv.Hooks().AddHook(l)

	return func() []string {
		l.mu.Lock()
		defer l.mu.Unlock()

		return append([]string(nil), l.sent...)
	}
}
