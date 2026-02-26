// Package main demonstrates a comprehensive Dispatch setup with jobs, workflows,
// cron scheduling, saga compensations, child workflows, and a DWP WebSocket
// server. Run it and connect via WebSocket, HTTP RPC, or the Go/TS/Python client.
//
// Usage:
//
//	go run .
//
// Then in another terminal:
//
//	# Enqueue a job via HTTP RPC
//	curl -X POST http://localhost:8080/dwp/rpc \
//	  -H "Content-Type: application/json" \
//	  -d '{
//	    "id": "req-1",
//	    "type": "request",
//	    "method": "job.enqueue",
//	    "token": "demo-token",
//	    "data": {"name":"send-email","payload":{"to":"user@example.com","subject":"Hello"}}
//	  }'
//
//	# Start a workflow via HTTP RPC
//	curl -X POST http://localhost:8080/dwp/rpc \
//	  -H "Content-Type: application/json" \
//	  -d '{
//	    "id": "req-2",
//	    "type": "request",
//	    "method": "workflow.start",
//	    "token": "demo-token",
//	    "data": {"name":"order-pipeline","input":{"order_id":"ORD-001","items":["widget","gadget"]}}
//	  }'
//
//	# Get stats
//	curl -X POST http://localhost:8080/dwp/rpc \
//	  -H "Content-Type: application/json" \
//	  -d '{"id":"req-3","type":"request","method":"stats","token":"demo-token"}'
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/cron"
	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/engine"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/store/memory"
	"github.com/xraph/dispatch/workflow"

	"github.com/xraph/forge"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// ──────────────────────────────────────────────────
	// 1. Create the Dispatch engine
	// ──────────────────────────────────────────────────

	s := memory.New()
	d, err := dispatch.New(
		dispatch.WithStore(s),
		dispatch.WithConcurrency(4),
		dispatch.WithQueues([]string{"default", "email", "images"}),
		dispatch.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create dispatcher", slog.String("error", err.Error()))
		os.Exit(1)
	}

	eng, err := engine.Build(d,
		engine.WithStreamBroker(),
	)
	if err != nil {
		logger.Error("failed to build engine", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ──────────────────────────────────────────────────
	// 2. Register jobs
	// ──────────────────────────────────────────────────

	// A simple email-sending job.
	engine.Register(eng, job.NewDefinition("send-email", func(ctx context.Context, p struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}) error {
		logger.Info("sending email", slog.String("to", p.To), slog.String("subject", p.Subject))
		time.Sleep(100 * time.Millisecond) // Simulate I/O.
		logger.Info("email sent", slog.String("to", p.To))
		return nil
	}))

	// An image processing job with retry semantics.
	engine.Register(eng, job.NewDefinition("process-image", func(ctx context.Context, p struct {
		URL    string `json:"url"`
		Width  int    `json:"width"`
		Height int    `json:"height"`
	}) error {
		logger.Info("processing image",
			slog.String("url", p.URL),
			slog.Int("width", p.Width),
			slog.Int("height", p.Height),
		)
		time.Sleep(200 * time.Millisecond) // Simulate processing.
		logger.Info("image processed", slog.String("url", p.URL))
		return nil
	}))

	// A payment charging job (used by saga workflow).
	engine.Register(eng, job.NewDefinition("charge-payment", func(_ context.Context, p struct {
		OrderID string  `json:"order_id"`
		Amount  float64 `json:"amount"`
	}) error {
		logger.Info("charging payment",
			slog.String("order_id", p.OrderID),
			slog.Float64("amount", p.Amount),
		)
		return nil
	}))

	// ──────────────────────────────────────────────────
	// 3. Register workflows
	// ──────────────────────────────────────────────────

	// (a) Multi-step order pipeline with StepWithResult and parallel steps.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("order-pipeline",
		func(wf *workflow.Workflow, input struct {
			OrderID string   `json:"order_id"`
			Items   []string `json:"items"`
		}) error {
			// Step 1: Validate the order.
			if err := wf.Step("validate", func(ctx context.Context) error {
				logger.Info("validating order", slog.String("order_id", input.OrderID))
				if len(input.Items) == 0 {
					return fmt.Errorf("order has no items")
				}
				return nil
			}); err != nil {
				return err
			}

			// Step 2: Calculate total (with typed result).
			total, err := workflow.StepWithResult[float64](wf, "calculate-total",
				func(ctx context.Context) (float64, error) {
					price := float64(len(input.Items)) * 19.99
					logger.Info("calculated total",
						slog.String("order_id", input.OrderID),
						slog.Float64("total", price),
					)
					return price, nil
				})
			if err != nil {
				return err
			}

			// Step 3: Process payment.
			if err := wf.Step("process-payment", func(ctx context.Context) error {
				logger.Info("processing payment",
					slog.String("order_id", input.OrderID),
					slog.Float64("amount", total),
				)
				return nil
			}); err != nil {
				return err
			}

			// Step 4: Send confirmation.
			return wf.Step("send-confirmation", func(ctx context.Context) error {
				logger.Info("sending order confirmation",
					slog.String("order_id", input.OrderID),
				)
				return nil
			})
		},
	))

	// (b) Saga workflow with compensation (rollback on failure).
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("booking-saga",
		func(wf *workflow.Workflow, input struct {
			TripID string `json:"trip_id"`
		}) error {
			// Step 1: Reserve hotel (with compensation).
			if err := wf.StepWithCompensation(
				"reserve-hotel",
				func(ctx context.Context) error {
					logger.Info("reserving hotel", slog.String("trip_id", input.TripID))
					return nil
				},
				func(ctx context.Context) error {
					logger.Info("cancelling hotel reservation", slog.String("trip_id", input.TripID))
					return nil
				},
			); err != nil {
				return err
			}

			// Step 2: Reserve flight (with compensation).
			if err := wf.StepWithCompensation(
				"reserve-flight",
				func(ctx context.Context) error {
					logger.Info("reserving flight", slog.String("trip_id", input.TripID))
					return nil
				},
				func(ctx context.Context) error {
					logger.Info("cancelling flight reservation", slog.String("trip_id", input.TripID))
					return nil
				},
			); err != nil {
				return err
			}

			// Step 3: Reserve car (with compensation).
			return wf.StepWithCompensation(
				"reserve-car",
				func(ctx context.Context) error {
					logger.Info("reserving rental car", slog.String("trip_id", input.TripID))
					return nil
				},
				func(ctx context.Context) error {
					logger.Info("cancelling car reservation", slog.String("trip_id", input.TripID))
					return nil
				},
			)
		},
	))

	// (c) Child workflow for processing individual items.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("process-item",
		func(wf *workflow.Workflow, input struct {
			ItemID string `json:"item_id"`
		}) error {
			return wf.Step("process", func(ctx context.Context) error {
				logger.Info("processing item", slog.String("item_id", input.ItemID))
				time.Sleep(50 * time.Millisecond)
				return nil
			})
		},
	))

	// (d) Parent workflow that spawns child workflows.
	engine.RegisterWorkflow(eng, workflow.NewWorkflow("batch-process",
		func(wf *workflow.Workflow, input struct {
			ItemIDs []string `json:"item_ids"`
		}) error {
			// Spawn child workflows for each item (fire-and-forget).
			for _, itemID := range input.ItemIDs {
				runID, err := workflow.SpawnChild(wf, "process-item", struct {
					ItemID string `json:"item_id"`
				}{ItemID: itemID})
				if err != nil {
					return fmt.Errorf("spawn child for %s: %w", itemID, err)
				}
				logger.Info("spawned child workflow",
					slog.String("item_id", itemID),
					slog.String("child_run_id", runID.String()),
				)
			}

			return wf.Step("finalize", func(ctx context.Context) error {
				logger.Info("batch processing finalized",
					slog.Int("total_items", len(input.ItemIDs)),
				)
				return nil
			})
		},
	))

	// ──────────────────────────────────────────────────
	// 4. Register cron schedules
	// ──────────────────────────────────────────────────

	ctx := context.Background()

	if err := engine.RegisterCron(ctx, eng, &cron.Definition[struct{}]{
		Name:     "daily-cleanup",
		Schedule: "0 2 * * *", // Every day at 2 AM.
		JobName:  "process-image",
		Queue:    "images",
		Payload:  struct{}{},
	}); err != nil {
		logger.Error("failed to register cron", slog.String("error", err.Error()))
	}

	// ──────────────────────────────────────────────────
	// 5. Set up DWP server
	// ──────────────────────────────────────────────────

	broker := eng.StreamBroker()
	handler := dwp.NewHandler(eng, broker, logger)
	dwpServer := dwp.NewServer(broker, handler,
		dwp.WithAuth(dwp.NewAPIKeyAuthenticator(
			dwp.APIKeyEntry{
				Token: "demo-token",
				Identity: dwp.Identity{
					Subject: "demo-user",
					AppID:   "kitchen-sink",
					OrgID:   "demo-org",
					Scopes:  []string{dwp.ScopeAll},
				},
			},
		)),
		dwp.WithLogger(logger),
	)

	// ──────────────────────────────────────────────────
	// 6. Create Forge app and register routes
	// ──────────────────────────────────────────────────

	app := forge.New(
		forge.WithAppName("dispatch-kitchen-sink"),
		forge.WithAppVersion("0.1.0"),
		forge.WithHTTPAddress(":8080"),
	)

	// Register DWP routes (WebSocket, SSE, HTTP RPC).
	dwpServer.RegisterRoutes(app.Router())

	// ──────────────────────────────────────────────────
	// 7. Start and run
	// ──────────────────────────────────────────────────

	// Start the engine (begins processing jobs and cron ticks).
	if err := eng.Start(ctx); err != nil {
		logger.Error("failed to start engine", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("Dispatch kitchen-sink example running",
		slog.String("dwp_ws", "ws://localhost:8080/dwp"),
		slog.String("dwp_rpc", "http://localhost:8080/dwp/rpc"),
		slog.String("dwp_sse", "http://localhost:8080/dwp/sse"),
	)

	// Enqueue a demo job to show it works.
	demoJob, _ := engine.Enqueue(ctx, eng, "send-email", struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}{
		To:      "hello@example.com",
		Subject: "Welcome to Dispatch!",
	})
	if demoJob != nil {
		logger.Info("demo job enqueued", slog.String("job_id", demoJob.ID.String()))
	}

	// Start a demo workflow.
	demoRun, _ := engine.StartWorkflow(ctx, eng, "order-pipeline", struct {
		OrderID string   `json:"order_id"`
		Items   []string `json:"items"`
	}{
		OrderID: "DEMO-001",
		Items:   []string{"widget", "gadget"},
	})
	if demoRun != nil {
		logger.Info("demo workflow started", slog.String("run_id", demoRun.ID.String()))
	}

	// Wait for shutdown signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := eng.Stop(shutdownCtx); err != nil {
		logger.Error("engine shutdown error", slog.String("error", err.Error()))
	}
	logger.Info("goodbye")
}
