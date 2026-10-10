// Command oldwriter exercises an unchanged published Dispatch store through a
// bounded local command stream. It never supplies a retirement writer marker.
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
	"github.com/xraph/dispatch/store/postgres"
)

type command struct {
	Operation             string
	Namespace             durable.NamespaceConfig
	Start                 durable.StartRequest
	Claim                 durable.ClaimRequest
	TimeoutClaim          durable.TimeoutClaimRequest
	ExecutionTimeoutClaim durable.ExecutionTimeoutClaimRequest
	ChildClaim            durable.ChildDeliveryClaimRequest
	Commit                durable.CommitRequest
	Signal                durable.SignalRequest
	SignalStart           durable.SignalWithStartRequest
	Heartbeat             durable.HeartbeatRequest
	Timeout               durable.ExecutionTimeoutRequest
	Child                 durable.ChildDeliveryRequest
	Key                   durable.Key
	Token                 durable.TaskToken
	TaskID                string
}

type response struct {
	Operation string
	Value     any
	SQLState  string
	Failed    bool
}

func main() {
	ctx := context.Background()
	drv := pgdriver.New()
	if err := drv.Open(ctx, os.Getenv("DISPATCH_OLDWRITER_DSN")); err != nil {
		fmt.Fprintln(os.Stderr, "old writer database open failed")
		os.Exit(1)
	}
	db, err := grove.Open(drv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "old writer store open failed")
		os.Exit(1)
	}
	defer db.Close()
	s := postgres.New(db)
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	for {
		var c command
		if err = decoder.Decode(&c); err != nil {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		value, callErr := execute(callCtx, s, c)
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

func execute(ctx context.Context, s *postgres.Store, c command) (any, error) {
	switch c.Operation {
	case "migrate":
		return nil, s.Migrate(ctx)
	case "namespace":
		return s.RegisterNamespace(ctx, c.Namespace)
	case "start":
		return s.StartExecution(ctx, c.Start)
	case "claim":
		return s.ClaimTask(ctx, c.Claim)
	case "claim_timeout":
		return s.ClaimTimeoutTask(ctx, c.TimeoutClaim)
	case "claim_execution_timeout":
		return s.ClaimExecutionTimeout(ctx, c.ExecutionTimeoutClaim)
	case "claim_child":
		return s.ClaimChildDelivery(ctx, c.ChildClaim)
	case "renew":
		return s.RenewTask(ctx, c.Key, c.Token, time.Minute)
	case "commit":
		return s.CommitTransition(ctx, c.Commit)
	case "signal":
		return s.SignalExecution(ctx, c.Signal)
	case "signal_start":
		return s.SignalWithStart(ctx, c.SignalStart)
	case "heartbeat":
		return s.RecordHeartbeat(ctx, c.Heartbeat)
	case "execution_timeout":
		return s.ApplyExecutionTimeout(ctx, c.Timeout)
	case "child":
		return s.ApplyChildDelivery(ctx, c.Child)
	case "get_task":
		return s.GetTask(ctx, c.Key, c.TaskID)
	case "get":
		return s.GetExecution(ctx, c.Key)
	case "history":
		return s.ReadHistory(ctx, c.Key, 0, 1000)
	default:
		return nil, errors.New("unknown fixture operation")
	}
}
