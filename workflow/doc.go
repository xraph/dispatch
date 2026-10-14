// Package workflow defines typed workflow definitions, runs, step checkpointing,
// and the workflow store interface.
//
// Workflows are durable, multi-step functions. They survive process restarts
// by checkpointing completed steps. A step that already completed is skipped on
// resume; only the failed or pending step re-executes.
//
// # Defining a Workflow
//
//	var ProcessOrder = workflow.NewWorkflow("process_order",
//	    func(wf *workflow.Workflow, input OrderInput) error {
//	        if err := wf.Step("validate", func(ctx context.Context) error {
//	            return validateOrder(ctx, input)
//	        }); err != nil {
//	            return err
//	        }
//
//	        if err := wf.Step("charge", func(ctx context.Context) error {
//	            return chargeCard(ctx, input.PaymentToken, input.Amount)
//	        }); err != nil {
//	            return err
//	        }
//
//	        return wf.Step("fulfill", func(ctx context.Context) error {
//	            return fulfillOrder(ctx, input.OrderID)
//	        })
//	    },
//	)
//
// # Parallel Steps
//
// Use [Workflow.Parallel] to run independent steps concurrently. The first
// argument names the group, so the checkpoint survives a restart:
//
//	err := wf.Parallel("notify",
//	    func(ctx context.Context) error { return sendConfirmationEmail(ctx, input.Email) },
//	    func(ctx context.Context) error { return updateInventory(ctx, input.Items) },
//	)
//
// # Waiting for Events
//
// A workflow can pause until an external event arrives:
//
//	evt, err := wf.WaitForEvent("payment.confirmed", 24*time.Hour)
//
// # State Machine
//
// A [Run] moves through these states:
//
//	running -> completed
//	running -> failed
//
// # Key Types
//
//   - [Definition]: typed workflow descriptor with Name and Handler
//   - [Run]: a single workflow execution record
//   - [RunState]: running, completed, or failed
//   - [Registry]: maps workflow names to type-erased runner functions
package workflow
