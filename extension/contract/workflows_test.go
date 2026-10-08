package contract

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/ext"
	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/store"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"
)

func seedWorkflow(t *testing.T, d Deps, name string, state workflow.RunState, app, org string, parent *id.RunID) *workflow.Run {
	t.Helper()
	now := time.Now().UTC()
	started := now.Add(-90 * time.Second)
	run := &workflow.Run{Entity: dispatch.NewEntity(), ID: id.NewRunID(), Name: name, State: state, Input: []byte(`{"n":9007199254740993}`),
		ScopeAppID: app, ScopeOrgID: org, StartedAt: started, ParentRunID: parent}
	if state != workflow.RunStateRunning {
		run.CompletedAt = &now
	}
	if err := d.Store.CreateRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	return run
}
func runWorkflowDomain(t *testing.T, s store.Store) {
	t.Helper()
	d := contractDeps(t, s)
	ctx := context.Background()
	runs := []*workflow.Run{
		seedWorkflow(t, d, "order.a", workflow.RunStateCompleted, "app-a", "org-a", nil),
		seedWorkflow(t, d, "order.b", workflow.RunStateFailed, "app-b", "org-b", nil),
		seedWorkflow(t, d, "report", workflow.RunStateRunning, "app-a", "org-a", nil),
		seedWorkflow(t, d, "order.c", workflow.RunStateCompleted, "", "", nil),
	}
	p := fc.Principal{Claims: map[string]any{"scope_app_id": "app-a", "scope_org_id": "org-a"}}
	var got []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > len(runs) {
			t.Fatal("cursor loop")
		}
		page, err := workflowsListHandler(d)(ctx, WorkflowsListInput{Limit: 2, Cursor: cursor}, p)
		if err != nil {
			t.Fatal(err)
		}
		if page.Items == nil || page.AsOf == "" {
			t.Fatal("missing page metadata")
		}
		for _, row := range page.Items {
			got = append(got, row.ID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	want := make([]string, 0, len(runs))
	for _, run := range runs {
		want = append(want, run.ID.String())
	}
	slices.Sort(want)
	slices.Reverse(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operator-wide identities/order=%v want=%v", got, want)
	}
	page, err := workflowsListHandler(d)(ctx, WorkflowsListInput{State: workflow.RunStateFailed, NamePrefix: "order.", ScopeAppID: "app-b", ScopeOrgID: "org-b"}, p)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != runs[1].ID.String() {
		t.Fatalf("filtered=%+v, %v", page, err)
	}
	for _, input := range []WorkflowsListInput{{State: "unknown"}, {Limit: -1}, {Cursor: "broken"}} {
		if _, listErr := workflowsListHandler(d)(ctx, input, p); !errors.Is(listErr, fc.ErrBadRequest) {
			t.Fatalf("bad filter %+v: %v", input, listErr)
		}
	}
	parent := runs[0]
	child := seedWorkflow(t, d, "child", workflow.RunStateCompleted, "app-a", "org-a", &parent.ID)
	grandchild := seedWorkflow(t, d, "grandchild", workflow.RunStateCompleted, "app-a", "org-a", &child.ID)
	_ = grandchild
	engine.RegisterWorkflow(d.Engine, workflow.NewWorkflowV("order.a", 1, func(*workflow.Workflow, struct{}) error { return nil }))
	for step, data := range map[string][]byte{"json": []byte(`{"n":9007199254740993}`), "opaque": {0, 255}, "empty": {}} {
		if saveErr := s.SaveCheckpoint(ctx, parent.ID, step, data); saveErr != nil {
			t.Fatal(saveErr)
		}
	}
	detail, err := workflowsGetHandler(d)(ctx, IDInput{ID: parent.ID.String()}, p)
	if err != nil {
		t.Fatal(err)
	}
	if !detail.VersionRegistered || detail.Version != 1 || detail.RecordedVersion != 0 || detail.Duration == nil || detail.Duration.MS != 90000 ||
		len(detail.Children) != 1 || detail.Children[0].ID != child.ID.String() || detail.Children[0].ParentRunID == nil || *detail.Children[0].ParentRunID != parent.ID.String() {
		t.Fatalf("detail=%+v", detail)
	}
	cps, err := s.ListCheckpoints(ctx, parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i, cp := range detail.Checkpoints {
		if cp.ID != cps[i].ID.String() {
			t.Fatalf("checkpoint order=%+v", detail.Checkpoints)
		}
		switch cp.StepName {
		case "json":
			if cp.Payload.Kind != "json" {
				t.Fatalf("JSON=%+v", cp.Payload)
			}
		case "opaque":
			if cp.Payload.Kind != "gob" || cp.Payload.Bytes == nil || *cp.Payload.Bytes != 2 {
				t.Fatalf("opaque=%+v", cp.Payload)
			}
		case "empty":
			if cp.Payload.Bytes == nil || *cp.Payload.Bytes != 0 {
				t.Fatalf("empty=%+v", cp.Payload)
			}
		}
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "9007199254740993") || strings.Contains(string(raw), "parent_run_id") {
		t.Fatalf("wire=%s", raw)
	}
	empty, err := workflowsGetHandler(d)(ctx, IDInput{ID: runs[3].ID.String()}, p)
	if err != nil || empty.Checkpoints == nil || empty.Children == nil || empty.ScopeAppID != nil || empty.ParentRunID != nil || empty.VersionRegistered {
		t.Fatalf("empty=%+v, %v", empty, err)
	}
	for _, rawID := range []string{"broken", id.NewJobID().String()} {
		if _, err := workflowsGetHandler(d)(ctx, IDInput{ID: rawID}, p); !errors.Is(err, fc.ErrBadRequest) {
			t.Fatalf("ID=%s: %v", rawID, err)
		}
	}
	if _, err := workflowsGetHandler(d)(ctx, IDInput{ID: id.NewRunID().String()}, p); !errors.Is(err, fc.ErrNotFound) {
		t.Fatalf("missing=%v", err)
	}
}
func TestWorkflowDomainMemoryAndSQLite(t *testing.T) {
	t.Run("memory", func(t *testing.T) { runWorkflowDomain(t, memory.New()) })
	t.Run("sqlite", func(t *testing.T) { runWorkflowDomain(t, sqliteContractStore(t)) })
}
func TestWorkflowDurationDoesNotInventMissingTimes(t *testing.T) {
	now := time.Now()
	run := &workflow.Run{State: workflow.RunStateCompleted, StartedAt: now}
	if projectWorkflow(run, now).Duration != nil {
		t.Fatal("terminal missing completion got a duration")
	}
	run.State = workflow.RunStateRunning
	if got := projectWorkflow(run, now.Add(2*time.Second)); got.Duration == nil || got.Duration.MS != 2000 {
		t.Fatalf("running duration=%+v", got)
	}
	run.StartedAt = time.Time{}
	if projectWorkflow(run, now).Duration != nil {
		t.Fatal("missing start got a duration")
	}
}

type workflowContractEnds struct{ ends chan struct{} }

func (*workflowContractEnds) Name() string { return "workflow-contract-ends" }
func (r *workflowContractEnds) OnWorkflowCompleted(context.Context, *workflow.Run, time.Duration) error {
	r.ends <- struct{}{}
	return nil
}
func (r *workflowContractEnds) OnWorkflowFailed(context.Context, *workflow.Run, error) error {
	r.ends <- struct{}{}
	return nil
}
func (r *workflowContractEnds) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.ends:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not finish")
	}
}
func TestWorkflowReplayContractUsesPreviewVersionGenerationAndActor(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var s store.Store = memory.New()
			if backend == "sqlite" {
				s = sqliteContractStore(t)
			}
			actions := &actionRecorder{}
			ends := &workflowContractEnds{ends: make(chan struct{}, 4)}
			d := contractDeps(t, s, engine.WithExtension(actions), engine.WithExtension(ends))
			ctx := context.Background()
			gate := make(chan struct{})
			released := false
			t.Cleanup(func() {
				if !released {
					close(gate)
				}
			})
			var wrongVersion atomic.Int32
			engine.RegisterWorkflow(d.Engine, workflow.NewWorkflowV("replay", 1, func(wf *workflow.Workflow, _ struct{}) error {
				if err := wf.Step("target", func(context.Context) error { return errors.New("kept checkpoint reran") }); err != nil {
					return err
				}
				return wf.Step("later", func(ctx context.Context) error {
					select {
					case <-gate:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}))
			engine.RegisterWorkflow(d.Engine, workflow.NewWorkflowV("replay", 2, func(*workflow.Workflow, struct{}) error { wrongVersion.Add(1); return nil }))
			run := seedWorkflow(t, d, "replay", workflow.RunStateCompleted, "", "", nil)
			for _, step := range []string{"target", "later"} {
				if err := s.SaveCheckpoint(ctx, run.ID, step, []byte("{}")); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Millisecond)
			}
			input := WorkflowReplayInput{ID: run.ID.String(), FromStep: "target"}
			principal := fc.Principal{User: &dashauth.UserInfo{Subject: "workflow-operator"}}
			preview, err := workflowsReplayPreviewHandler(d)(ctx, input, principal)
			if err != nil || preview.Version != 1 || preview.Generation != 0 || !reflect.DeepEqual(preview.Reruns, []string{"later"}) || len(actions.actions) != 0 {
				t.Fatalf("preview=%+v, %v", preview, err)
			}
			for _, generation := range []*int64{nil, new(int64(-1))} {
				if _, replayErr := workflowsReplayFromHandler(d)(ctx, WorkflowReplayCommandInput{WorkflowReplayInput: input, ExpectedGeneration: generation}, principal); !errors.Is(replayErr, fc.ErrBadRequest) {
					t.Fatalf("invalid generation=%v", replayErr)
				}
			}
			result, err := workflowsReplayFromHandler(d)(ctx, WorkflowReplayCommandInput{WorkflowReplayInput: input, ExpectedGeneration: &preview.Generation}, principal)
			if err != nil || result.AcceptedGeneration != 1 || result.Plan.Version != 1 {
				t.Fatalf("replay=%+v, %v", result, err)
			}
			current, err := s.GetRun(ctx, run.ID)
			if err != nil || current.State != workflow.RunStateRunning {
				t.Fatalf("async replay=%+v, %v", current, err)
			}
			if len(actions.actions) != 1 || actions.actions[0].Actor != "workflow-operator" || actions.actions[0].Kind != ext.ActionWorkflowReplayed {
				t.Fatalf("actions=%+v", actions.actions)
			}
			close(gate)
			released = true
			ends.wait(t)
			if wrongVersion.Load() != 0 {
				t.Fatal("used latest version")
			}
			_, err = workflowsReplayFromHandler(d)(ctx, WorkflowReplayCommandInput{WorkflowReplayInput: input, ExpectedGeneration: &preview.Generation}, principal)
			var conflict *fc.Error
			if !errors.As(err, &conflict) || conflict.Code != fc.CodeConflict || conflict.Details["state"] != workflow.RunStateCompleted || conflict.Details["generation"] != int64(1) {
				t.Fatalf("stale preview=%+v", err)
			}
			if len(actions.actions) != 1 {
				t.Fatal("stale replay emitted action")
			}
		})
	}
}

type workflowReadFailure struct {
	store.Store
	operation string
	bounded   bool
}

func (s *workflowReadFailure) ListCheckpoints(ctx context.Context, runID id.RunID) ([]*workflow.Checkpoint, error) {
	if s.operation != "checkpoints" {
		return s.Store.ListCheckpoints(ctx, runID)
	}
	_, s.bounded = ctx.Deadline()
	return nil, errors.New("private-store-secret")
}
func (s *workflowReadFailure) ListChildRuns(ctx context.Context, runID id.RunID) ([]*workflow.Run, error) {
	if s.operation != "children" {
		return s.Store.ListChildRuns(ctx, runID)
	}
	_, s.bounded = ctx.Deadline()
	return nil, errors.New("private-store-secret")
}
func (s *workflowReadFailure) ListRunsPage(ctx context.Context, opts workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	if s.operation == "page" {
		return workflow.RunPage{Runs: []*workflow.Run{}, NextCursor: "continue", Complete: false}, nil
	}
	return s.Store.ListRunsPage(ctx, opts)
}
func TestWorkflowContractPreservesIncompletePagesAndReadFailures(t *testing.T) {
	for _, operation := range []string{"checkpoints", "children", "page"} {
		t.Run(operation, func(t *testing.T) {
			s := &workflowReadFailure{Store: memory.New(), operation: operation}
			d := contractDeps(t, s)
			run := seedWorkflow(t, d, "inspect", workflow.RunStateFailed, "", "", nil)
			if operation == "page" {
				page, err := workflowsListHandler(d)(context.Background(), WorkflowsListInput{}, fc.Principal{})
				if err != nil || page.Complete || page.NextCursor == nil || *page.NextCursor != "continue" || page.Items == nil {
					t.Fatalf("page=%+v, %v", page, err)
				}
				return
			}
			_, err := workflowsGetHandler(d)(context.Background(), IDInput{ID: run.ID.String()}, fc.Principal{})
			if !errors.Is(err, fc.ErrInternal) || strings.Contains(err.Error(), "private-store-secret") || !s.bounded {
				t.Fatalf("read failure=%v, bounded=%v", err, s.bounded)
			}
		})
	}
}
func TestWorkflowReplayRefusals(t *testing.T) {
	d := contractDeps(t, memory.New())
	ctx := context.Background()
	run := seedWorkflow(t, d, "missing-definition", workflow.RunStateFailed, "", "", nil)
	if err := d.Store.SaveCheckpoint(ctx, run.ID, "target", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	input := WorkflowReplayInput{ID: run.ID.String(), FromStep: "target"}
	generation := int64(0)
	for _, step := range []string{"target", "unreached"} {
		input.FromStep = step
		if _, err := workflowsReplayPreviewHandler(d)(ctx, input, fc.Principal{}); !errors.Is(err, fc.ErrConflict) {
			t.Fatalf("preview refusal=%v", err)
		}
	}
	input.FromStep = "target"
	engine.RegisterWorkflow(d.Engine, workflow.NewWorkflow("missing-definition", func(*workflow.Workflow, struct{}) error { return nil }))
	run.State = workflow.RunStateRunning
	if err := d.Store.UpdateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if _, err := workflowsReplayFromHandler(d)(ctx, WorkflowReplayCommandInput{WorkflowReplayInput: input, ExpectedGeneration: &generation}, fc.Principal{}); !errors.Is(err, fc.ErrConflict) {
		t.Fatalf("running=%v", err)
	}
	run.State = workflow.RunStateFailed
	if err := d.Store.UpdateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := d.Engine.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := workflowsReplayFromHandler(d)(ctx, WorkflowReplayCommandInput{WorkflowReplayInput: input, ExpectedGeneration: &generation}, fc.Principal{}); !errors.Is(err, fc.ErrUnavailable) {
		t.Fatalf("stopped=%v", err)
	}
}
