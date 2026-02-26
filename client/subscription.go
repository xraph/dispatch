package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/dispatch/dwp"
	"github.com/xraph/dispatch/stream"
)

// Subscribe subscribes to a stream topic and returns a channel of events.
// The channel is closed when the client disconnects or Unsubscribe is called.
//
// Topics follow the dispatch stream convention:
//   - "job:<jobID>"       — Events for a specific job
//   - "workflow:<runID>"  — Step-by-step events for a workflow run
//   - "queue:<name>"      — All events for a queue
//   - "jobs"              — All job lifecycle events
//   - "workflows"         — All workflow lifecycle events
//   - "firehose"          — Everything
func (c *Client) Subscribe(ctx context.Context, channel string) (<-chan *stream.Event, error) {
	// Send subscribe request.
	_, err := c.request(ctx, dwp.MethodSubscribe, dwp.SubscribeRequest{
		Channel: channel,
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe to %q: %w", channel, err)
	}

	ch := make(chan *stream.Event, 64)
	c.subs.Store(channel, ch)

	return ch, nil
}

// Unsubscribe removes a subscription.
func (c *Client) Unsubscribe(ctx context.Context, channel string) error {
	_, err := c.request(ctx, dwp.MethodUnsubscribe, dwp.UnsubscribeRequest{
		Channel: channel,
	})

	// Close and remove the local channel regardless.
	if val, ok := c.subs.LoadAndDelete(channel); ok {
		ch := val.(chan *stream.Event) //nolint:errcheck // subs map always stores chan *stream.Event
		close(ch)
	}

	return err
}

// Watch subscribes to workflow events for a specific run and returns an
// event channel. This is a convenience method that subscribes to
// "workflow:<runID>".
func (c *Client) Watch(ctx context.Context, runID string) (<-chan *stream.Event, error) {
	channel := "workflow:" + runID
	return c.Subscribe(ctx, channel)
}

// Stats retrieves broker and connection statistics from the server.
func (c *Client) Stats(ctx context.Context) (json.RawMessage, error) {
	resp, err := c.request(ctx, dwp.MethodStats, nil)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}
