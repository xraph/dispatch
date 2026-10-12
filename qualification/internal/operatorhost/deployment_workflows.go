package operatorhost

import (
	"time"

	drt "github.com/xraph/dispatch/durable/runtime"
)

// deploymentWorkflows exposes reproducible admission and blocker scenarios using
// ordinary authorized workflow commands. No endpoint writes fixture store rows.
func deploymentWorkflows() map[string]drt.WorkflowFunc {
	return map[string]drt.WorkflowFunc{
		"deferred-child": func(w *drt.Workflow, input []byte) ([]byte, error) {
			w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
			if _, err := w.ReceiveSignal("handoff", "handoff").Get(); err != nil {
				return nil, err
			}
			return w.ChildWorkflow("child", "operator", input, drt.ChildOptions{BuildID: "operator-v2", Queue: "operator"}).Get()
		},
		"deferred-continue": func(w *drt.Workflow, input []byte) ([]byte, error) {
			w.SetQueryHandler("status", func([]byte) ([]byte, error) { return input, nil })
			if string(input) == "successor" {
				return w.ReceiveSignal("finish", "finish").Get()
			}
			if _, err := w.ReceiveSignal("handoff", "handoff").Get(); err != nil {
				return nil, err
			}
			return nil, w.ContinueAsNew([]byte("successor"), drt.ContinueOptions{BuildID: "operator-v2"})
		},
		"sleep": func(w *drt.Workflow, _ []byte) ([]byte, error) { return w.Timer("sleep", time.Hour).Get() },
		"retry": func(w *drt.Workflow, _ []byte) ([]byte, error) {
			return w.ActivityWithOptions("retry", "retry", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Second, RetryPolicy: &drt.RetryPolicy{MaximumAttempts: 3, InitialInterval: 10 * time.Minute}}).Get()
		},
	}
}
