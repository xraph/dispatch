package event

import (
	"time"

	"github.com/xraph/dispatch/id"
)

// Event represents a named event published to the event bus. Workflows
// can wait for events using WaitForEvent, enabling external triggers
// and inter-workflow coordination.
type Event struct {
	ID         id.EventID `json:"id"`
	Name       string     `json:"name"`
	Payload    []byte     `json:"payload,omitempty"`
	ScopeAppID string     `json:"scope_app_id,omitempty"`
	ScopeOrgID string     `json:"scope_org_id,omitempty"`
	Acked      bool       `json:"acked"`
	CreatedAt  time.Time  `json:"created_at"`
}
