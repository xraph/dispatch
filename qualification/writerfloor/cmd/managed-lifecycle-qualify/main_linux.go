//go:build linux

// The service mode is built as /app/workload in the fixed qualification image.
// The controller assembly remains separate from this process-owned native worker.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xraph/grove"
	gdriver "github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/dispatch/durable"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/managedhost"
	"github.com/xraph/dispatch/store/postgres"
)

type serviceConfiguration struct {
	DSN    string                      `json:"dsn"`
	Probes []managedhost.RetainedProbe `json:"probes"`
}

func main() { os.Exit(run()) }
func run() int {
	if len(os.Args) != 2 || os.Args[1] != "service" {
		fmt.Fprintln(os.Stderr, "expected service mode")
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runService(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "managed service failure:", serviceFailureClass(err))
		return 1
	}
	return 0
}
func runService(ctx context.Context) (result error) {
	startup, err := managedhost.DecodeServiceStartup(os.Getenv("WRITERFLOOR_SERVICE_STARTUP"))
	if err != nil {
		return err
	}
	// Credentials remain private memory after this one startup read.
	if operationErr := os.Unsetenv("WRITERFLOOR_SERVICE_STARTUP"); operationErr != nil {
		return operationErr
	}
	var configuration serviceConfiguration
	decoder := json.NewDecoder(bytes.NewReader(startup.Configuration))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&configuration); err != nil {
		return durable.ErrInvalid
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return durable.ErrInvalid
	}
	if configuration.DSN == "" || len(configuration.Probes) < 1 || len(configuration.Probes) > 8 {
		return durable.ErrInvalid
	}
	for _, probe := range configuration.Probes {
		if probe.Request.Name != "status" || probe.Request.Namespace != startup.Namespace || probe.Request.BuildID != startup.Build {
			return durable.ErrInvalid
		}
	}
	// Each role has its own pool; reserve server capacity for authentication and control.
	driver := pgdriver.New()
	bounded, stop := context.WithTimeout(ctx, 5*time.Second)
	err = driver.Open(bounded, configuration.DSN, gdriver.WithPoolSize(8))
	stop()
	if err != nil {
		return durable.ErrQueryRetention
	}
	db, err := grove.Open(driver)
	if err != nil {
		return durable.ErrQueryRetention
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	store := postgres.New(db)
	worker, err := drt.NewWorker(store, drt.Options{Namespace: startup.Namespace, Queue: startup.Queue, BuildID: startup.Build, Owner: startup.HostID, RuntimeID: startup.RuntimeID, InstanceID: startup.InstanceID, Concurrency: 1, Retirement: &drt.RetirementOptions{InstallationID: startup.Installation, WriterProtocol: durable.RetirementWriterProtocol}, Workflows: map[string]drt.WorkflowFunc{"managed-history": func(workflow *drt.Workflow, input []byte) ([]byte, error) {
		workflow.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
		return input, nil
	}}})
	if err != nil {
		return err
	}
	return managedhost.ServeWorkerChannel(ctx, startup, worker, store, configuration.Probes)
}

// Report only a fixed category or SQLSTATE, never connection strings or payloads.
func serviceFailureClass(err error) string {
	var classified interface{ DiagnosticClass() string }
	if errors.As(err, &classified) {
		return classified.DiagnosticClass()
	}
	var database interface{ SQLState() string }
	if errors.As(err, &database) {
		code := database.SQLState()
		if len(code) == 5 {
			valid := true
			for _, c := range code {
				if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
					valid = false
				}
			}
			if valid {
				return "sql_" + code
			}
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, durable.ErrQueryRetention):
		return "query_retention"
	case errors.Is(err, durable.ErrInvalid):
		return "invalid"
	default:
		return "unavailable"
	}
}
