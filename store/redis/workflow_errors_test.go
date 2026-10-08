//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/id"
	redisstore "github.com/xraph/dispatch/store/redis"
	"github.com/xraph/dispatch/workflow"
)

type workflowReadFault struct{ key atomic.Value }

func (h *workflowReadFault) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, n, a string) (net.Conn, error) { return next(ctx, n, a) }
}
func (h *workflowReadFault) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}
func (h *workflowReadFault) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "get" && fmt.Sprint(cmd.Args()[1]) == h.key.Load().(string) {
			err := errors.New("injected workflow read outage")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func TestWorkflowReadsRejectPartialResults(t *testing.T) {
	ctx := context.Background()
	kvStore := setupTestKV(t)
	s := redisstore.New(kvStore)
	client := redisdriver.UnwrapClient(kvStore)
	hook := &workflowReadFault{}
	hook.key.Store("")
	client.AddHook(hook)
	parent := id.NewRunID()
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: "child", State: workflow.RunStateCompleted, ParentRunID: &parent}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	for _, step := range []string{"target", "later"} {
		if err := s.SaveCheckpoint(ctx, run.ID, step, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	runKey := "dispatch:run:" + run.ID.String()
	cpKey := "dispatch:checkpoint:" + run.ID.String() + ":later"
	reads := map[string]struct {
		key  string
		call func() error
	}{
		"runs":        {runKey, func() error { _, err := s.ListRuns(ctx, workflow.ListOpts{}); return err }},
		"children":    {runKey, func() error { _, err := s.ListChildRuns(ctx, parent); return err }},
		"checkpoints": {cpKey, func() error { _, err := s.ListCheckpoints(ctx, run.ID); return err }},
		"pruning":     {cpKey, func() error { return s.DeleteCheckpointsAfter(ctx, run.ID, "target") }},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			original, err := client.Get(ctx, read.key).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"outage", "json", "id", "parent-or-run-id"} {
				t.Run(mode, func(t *testing.T) {
					t.Cleanup(func() {
						hook.key.Store("")
						if restoreErr := client.Set(ctx, read.key, original, 0).Err(); restoreErr != nil {
							t.Error(restoreErr)
						}
					})
					if mode == "outage" {
						hook.key.Store(read.key)
					} else {
						raw := []byte("{broken")
						if mode != "json" {
							var record map[string]json.RawMessage
							if decodeErr := json.Unmarshal(original, &record); decodeErr != nil {
								t.Fatal(decodeErr)
							}
							field := "id"
							if mode == "parent-or-run-id" {
								field = "parent_run_id"
								if read.key == cpKey {
									field = "run_id"
								}
							}
							record[field] = json.RawMessage(`"invalid"`)
							raw, err = json.Marshal(record)
							if err != nil {
								t.Fatal(err)
							}
						}
						if err := client.Set(ctx, read.key, raw, 0).Err(); err != nil {
							t.Fatal(err)
						}
					}
					if err := read.call(); err == nil {
						t.Fatal("read failure became a successful partial result")
					}
				})
			}
		})
	}
	// Preflight pruning must leave the valid target and unreadable later entry intact.
	for _, step := range []string{"target", "later"} {
		data, err := s.GetCheckpoint(ctx, run.ID, step)
		if err != nil || data == nil {
			t.Fatalf("checkpoint %s lost: %s, %v", step, data, err)
		}
	}
	// A dangling set member is different from an outage or a corrupt record.
	if err := client.SAdd(ctx, "dispatch:run_ids", id.NewRunID().String()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, "dispatch:checkpoint_idx:"+run.ID.String(), "missing").Err(); err != nil {
		t.Fatal(err)
	}
	runs, err := s.ListChildRuns(ctx, parent)
	if err != nil || len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("dangling run index: %v, %v", runs, err)
	}
	cps, err := s.ListCheckpoints(ctx, run.ID)
	if err != nil || len(cps) != 2 {
		t.Fatalf("dangling checkpoint index: %v, %v", cps, err)
	}
}
