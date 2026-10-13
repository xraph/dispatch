// Command querypreguard exercises published Dispatch before removal guards.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/store/postgres"
)

type command struct {
	Operation string
	Begin     durable.BeginQueryRemovalRequest
	Abort     durable.AbortQueryRemovalRequest
	Finish    durable.FinishQueryRemovalRequest
	Build     durable.BuildRetirementRequest
	Verify    durable.VerifyQueryRuntimeRequest
	Lookup    durable.LifecycleReceiptLookup
	Target    durable.QueryRuntimeTarget
}
type response struct {
	Value    any
	Failed   bool
	SQLState string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var c command
	if err := json.NewDecoder(os.Stdin).Decode(&c); err != nil {
		return errors.New("decode fixture command")
	}
	value, err := execute(ctx, c)
	r := response{Value: value, Failed: err != nil}
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		r.SQLState = state.SQLState()
	}
	if err = json.NewEncoder(os.Stdout).Encode(r); err != nil {
		return errors.New("encode fixture response")
	}
	return nil
}
func execute(ctx context.Context, c command) (any, error) {
	switch c.Operation {
	case "delete-runtime", "delete-build", "update-runtime", "update-build":
		return nil, invalidate(ctx, c)
	}
	drv := pgdriver.New()
	if err := drv.Open(ctx, os.Getenv("DISPATCH_QUERYPREGUARD_DSN")); err != nil {
		return nil, err
	}
	db, err := grove.Open(drv)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	s := postgres.New(db)
	switch c.Operation {
	case "begin":
		return s.BeginQueryRuntimeRemoval(ctx, c.Begin)
	case "abort":
		return s.AbortQueryRuntimeRemoval(ctx, c.Abort)
	case "finish":
		return s.FinishQueryRuntimeRemoval(ctx, c.Finish)
	case "resume":
		return s.AbortBuildRetirement(ctx, c.Build)
	case "verify":
		return s.RecordQueryRuntimeVerification(ctx, c.Verify)
	case "lookup":
		return s.LookupLifecycleReceipt(ctx, c.Lookup)
	default:
		return nil, errors.New("unsupported fixture operation")
	}
}
func invalidate(ctx context.Context, c command) error {
	conn, err := pgx.Connect(ctx, os.Getenv("DISPATCH_QUERYPREGUARD_DSN"))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // Commit or connection close releases this fixture transaction.
	if _, err = tx.Exec(ctx, `SELECT dispatch_retirement_coordinator_lock($1,1)`, c.Target.Namespace); err != nil {
		return err
	}
	switch c.Operation {
	case "delete-runtime":
		_, err = tx.Exec(ctx, `DELETE FROM dispatch_query_runtimes WHERE namespace=$1 AND runtime_id=$2`, c.Target.Namespace, c.Target.RuntimeID)
	case "delete-build":
		_, err = tx.Exec(ctx, `DELETE FROM dispatch_build_lifecycle WHERE namespace=$1 AND build_id=$2`, c.Target.Namespace, c.Target.BuildID)
	case "update-runtime":
		_, err = tx.Exec(ctx, `UPDATE dispatch_query_runtimes SET version=version+1,binding=jsonb_set(binding,'{Version}',to_jsonb(version+1)) WHERE namespace=$1 AND runtime_id=$2`, c.Target.Namespace, c.Target.RuntimeID)
	case "update-build":
		_, err = tx.Exec(ctx, `UPDATE dispatch_build_lifecycle SET epoch=epoch+1 WHERE namespace=$1 AND build_id=$2`, c.Target.Namespace, c.Target.BuildID)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
