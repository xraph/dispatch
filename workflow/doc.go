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
//	        if err := wf.Step("validate", func() error {
//	            return validateOrder(input)
//	        }); err != nil {
//	            return err
//	        }
//
//	        if err := wf.Step("charge", func() error {
//	            return chargeCard(input.PaymentToken, input.Amount)
//	        }); err != nil {
//	            return err
//	        }
//
//	        return wf.Step("fulfill", func() error {
//	            return fulfillOrder(input.OrderID)
//	        })
//	    },
//	)
//
// # Parallel Steps
//
// Use [Workflow.Parallel] to run independent steps concurrently:
//
//	wf.Parallel(
//	    func() error { return sendConfirmationEmail(input.Email) },
//	    func() error { return updateInventory(input.Items) },
//	)
//
// # Waiting for Events
//
// A workflow can pause until an external event arrives:
//
//	payload, err := wf.WaitForEvent(ctx, "payment.confirmed", 24*time.Hour)
//
// # State Machine
//
// A [Run] moves through these states:
//
//	running → completed
//	running → failed
//
// # Key Types
//
//   - [Definition] — typed workflow descriptor with Name and Handler
//   - [Run] — a single workflow execution record
//   - [RunState] — running, completed, or failed
//   - [Registry] — maps workflow names to type-erased runner functions
package workflow
