// Command protocolone exercises the published pre-query-retention library.
// It is a new fixture executable, not a recovered production executable.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/store/postgres"
)

type command struct {
	Operation  string
	Namespace  durable.NamespaceConfig
	Start      durable.StartRequest
	Enrollment durable.RetirementEnrollmentRequest
	Build      durable.BuildRetirementRequest
	Register   durable.RegisterBuildRequest
	Lookup     durable.LifecycleReceiptLookup
	Query      drt.QueryRequest
	RuntimeID  string
}
type response struct {
	Operation string
	Value     any
	SQLState  string
	Failed    bool
}
type fixture struct {
	s       *postgres.Store
	workers map[string]*drt.Worker
}

func main() {
	ctx := context.Background()
	drv := pgdriver.New()
	if err := drv.Open(ctx, os.Getenv("DISPATCH_OLDWRITER_DSN")); err != nil {
		fmt.Fprintln(os.Stderr, "protocol-one database open failed")
		os.Exit(1)
	}
	db, err := grove.Open(drv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "protocol-one store open failed")
		os.Exit(1)
	}
	defer db.Close()
	f := fixture{s: postgres.New(db), workers: map[string]*drt.Worker{}}
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	for {
		var c command
		if err = decoder.Decode(&c); err != nil {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		value, callErr := f.execute(callCtx, c)
		cancel()
		r := response{Operation: c.Operation, Value: value, Failed: callErr != nil}
		var state interface{ SQLState() string }
		if errors.As(callErr, &state) {
			r.SQLState = state.SQLState()
		}
		if err = encoder.Encode(r); err != nil {
			return
		}
	}
}
func (f *fixture) execute(ctx context.Context, c command) (any, error) {
	switch c.Operation {
	case "migrate":
		return nil, f.s.Migrate(ctx)
	case "namespace":
		return f.s.RegisterNamespace(ctx, c.Namespace)
	case "enroll":
		return f.s.EnrollRetirement(ctx, c.Enrollment)
	case "register":
		return f.s.RegisterBuild(ctx, c.Register)
	case "begin":
		return f.s.BeginBuildRetirement(ctx, c.Build)
	case "finalize":
		return f.s.FinalizeBuildRetirement(ctx, c.Build)
	case "lookup":
		return f.s.LookupLifecycleReceipt(ctx, c.Lookup)
	case "seed_query":
		w, err := drt.NewWorker(f.s, drt.Options{Namespace: c.Start.Namespace, BuildID: c.Start.BuildID, Queue: c.Start.Queue, Owner: "protocol-one", InstanceID: fmt.Sprintf("protocol-one-%d", os.Getpid()), Workflows: map[string]drt.WorkflowFunc{"retained": func(w *drt.Workflow, input []byte) ([]byte, error) {
			w.SetQueryHandler("snapshot", func([]byte) ([]byte, error) { return input, nil })
			return input, nil
		}}})
		if err != nil {
			return nil, err
		}
		if _, err = w.StartExecution(ctx, c.Start); err != nil {
			return nil, err
		}
		if _, err = w.RunOnce(ctx, durable.TaskWorkflow); err != nil {
			return nil, err
		}
		handle, err := w.BeginDrain(ctx, drt.DrainRequest{OperationID: "retain-query-only", Deadline: time.Now().Add(time.Minute)})
		if err != nil {
			return nil, err
		}
		if _, err = w.WaitDrain(ctx, handle); err != nil {
			return nil, err
		}
		status := w.Status()
		f.workers[status.RuntimeID] = w
		return status, nil
	case "query":
		w := f.workers[c.RuntimeID]
		if w == nil {
			return nil, durable.ErrNotFound
		}
		return w.QueryExecution(ctx, c.Query)
	default:
		return nil, durable.ErrInvalid
	}
}
