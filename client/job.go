package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/dispatch/dwp"
)

// JobResult contains the result of an enqueue operation.
type JobResult struct {
	JobID string `json:"job_id"`
	Queue string `json:"queue"`
	State string `json:"state"`
}

// Enqueue submits a job to the remote dispatch server.
func (c *Client) Enqueue(ctx context.Context, name string, payload any, opts ...EnqueueOption) (*JobResult, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	req := dwp.JobEnqueueRequest{
		Name:    name,
		Payload: raw,
	}
	for _, opt := range opts {
		opt(&req)
	}

	resp, reqErr := c.request(ctx, dwp.MethodJobEnqueue, req)
	if reqErr != nil {
		return nil, reqErr
	}

	var result JobResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &result, nil
}

// GetJob retrieves a job by ID.
func (c *Client) GetJob(ctx context.Context, jobID string) (json.RawMessage, error) {
	resp, err := c.request(ctx, dwp.MethodJobGet, dwp.JobGetRequest{
		JobID: jobID,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// CancelJob cancels a job by ID.
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	_, err := c.request(ctx, dwp.MethodJobCancel, dwp.JobCancelRequest{
		JobID: jobID,
	})
	return err
}

// EnqueueOption configures an enqueue request.
type EnqueueOption func(*dwp.JobEnqueueRequest)

// WithQueue sets the target queue.
func WithQueue(queue string) EnqueueOption {
	return func(r *dwp.JobEnqueueRequest) { r.Queue = queue }
}

// WithPriority sets the job priority.
func WithPriority(priority int) EnqueueOption {
	return func(r *dwp.JobEnqueueRequest) { r.Priority = priority }
}
