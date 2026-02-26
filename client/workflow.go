package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/dispatch/dwp"
)

// WorkflowResult contains the result of starting a workflow.
type WorkflowResult struct {
	RunID string `json:"run_id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// StartWorkflow starts a workflow on the remote dispatch server.
func (c *Client) StartWorkflow(ctx context.Context, name string, input any) (*WorkflowResult, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	resp, reqErr := c.request(ctx, dwp.MethodWorkflowStart, dwp.WorkflowStartRequest{
		Name:  name,
		Input: raw,
	})
	if reqErr != nil {
		return nil, reqErr
	}

	var result WorkflowResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &result, nil
}

// GetWorkflow retrieves a workflow run by ID.
func (c *Client) GetWorkflow(ctx context.Context, runID string) (json.RawMessage, error) {
	resp, err := c.request(ctx, dwp.MethodWorkflowGet, dwp.WorkflowGetRequest{
		RunID: runID,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// PublishEvent publishes an event that can trigger workflow continuations.
func (c *Client) PublishEvent(ctx context.Context, name string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	_, reqErr := c.request(ctx, dwp.MethodWorkflowEvent, dwp.WorkflowEventRequest{
		Name:    name,
		Payload: raw,
	})
	return reqErr
}
