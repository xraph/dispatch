package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ChildDeliveryKind identifies a lifecycle message between linked executions.
type ChildDeliveryKind string

const (
	ChildDeliveryResult                ChildDeliveryKind = "result"
	ChildDeliveryClose                 ChildDeliveryKind = "close"
	ChildDeliveryCancel                ChildDeliveryKind = "cancel"
	ChildDeliveryCancelAck             ChildDeliveryKind = "cancel_ack"
	ChildDeliveryApplied                                 = "applied"
	ChildDeliveryIgnoredClosed                           = "ignored_closed"
	ChildDeliveryIgnoredExpired                          = "ignored_expired"
	EventChildCompleted                                  = "workflow.child_completed"
	EventChildCancellationAcknowledged                   = "workflow.child_cancellation_acknowledged"
	EventWorkflowTerminated                              = "workflow.terminated"
)

// ChildMessage preserves the original relationship and terminal outcome.
// An acknowledgment's Disposition describes acceptance, never child completion.
type ChildMessage struct {
	Version        int               `json:"version"`
	CommandID      string            `json:"command_id"`
	CancellationID string            `json:"cancellation_id,omitempty"`
	Parent         Key               `json:"parent"`
	Child          Key               `json:"child"`
	Policy         ParentClosePolicy `json:"policy"`
	State          State             `json:"state,omitempty"`
	Output         []byte            `json:"output,omitempty"`
	CloseEvent     EventInput        `json:"close_event"`
	Disposition    string            `json:"disposition,omitempty"`
}

// ChildDelivery is retained independently of its source execution's lifecycle.
// Claims route by the target build and may be reclaimed after lease expiry.
type ChildDelivery struct {
	Source        Key               `json:"source"`
	ID            string            `json:"id"`
	Kind          ChildDeliveryKind `json:"kind"`
	Target        Key               `json:"target"`
	TargetBuildID string            `json:"target_build_id"`
	TargetQueue   string            `json:"target_queue"`
	Message       ChildMessage      `json:"message"`
	CreatedAt     time.Time         `json:"created_at"`
	AvailableAt   time.Time         `json:"available_at"`
	Owner         string            `json:"owner"`
	Epoch         int64             `json:"epoch"`
	Attempt       int64             `json:"attempt"`
	LeaseUntil    time.Time         `json:"lease_until"`
	Done          bool              `json:"done"`
	Disposition   string            `json:"disposition,omitempty"`
}

type ChildDeliveryClaimRequest struct {
	Namespace, BuildID, Owner string
	LeaseDuration             time.Duration
}

type ChildDeliveryRequest struct {
	Source     Key    `json:"source"`
	DeliveryID string `json:"delivery_id"`
	RequestID  string `json:"request_id"`
	Owner      string `json:"owner"`
	Epoch      int64  `json:"epoch"`
}

// ChildDeliveryReceipt remains valid after either execution closes.
type ChildDeliveryReceipt struct {
	Target Key `json:"target"`
	Receipt
	Disposition string `json:"disposition"`
}

type ChildCancellationSpec struct {
	CommandID string `json:"command_id"`
	TargetID  string `json:"target_id"`
}

// ExecutionTermination records forced closure without claiming cooperation.
type ExecutionTermination struct {
	Version   int    `json:"version"`
	RequestID string `json:"request_id"`
	Reason    string `json:"reason,omitempty"`
}

func (r ExecutionTermination) Validate() error {
	return ExecutionCancellation(r).Validate()
}

func (r ChildDeliveryClaimRequest) Validate() error {
	if !identifier(r.Namespace) || !identifier(r.Owner) || (r.BuildID != "" && !identifier(r.BuildID)) {
		return ErrInvalid
	}
	return ValidateLease(r.LeaseDuration)
}

func (r ChildDeliveryRequest) Validate() error {
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if !identifier(r.DeliveryID) || !identifier(r.RequestID) || !identifier(r.Owner) || r.Epoch < 1 {
		return ErrInvalid
	}
	return nil
}

// CheckLease rejects stale delivery grants using the store's clock.
func (d ChildDelivery) CheckLease(r ChildDeliveryRequest, now time.Time) error {
	if d.Source != r.Source || d.ID != r.DeliveryID || d.Done || d.Owner != r.Owner || d.Epoch != r.Epoch || !d.LeaseUntil.After(now) {
		return ErrLeaseLost
	}
	return nil
}

// Clone copies message bytes at the persistence boundary.
func (d ChildDelivery) Clone() ChildDelivery {
	d.Message.Output = append([]byte(nil), d.Message.Output...)
	d.Message.CloseEvent.Payload = append([]byte(nil), d.Message.CloseEvent.Payload...)
	return d
}

// Validate checks immutable routing and message shape before insertion or use.
func (d ChildDelivery) Validate() error {
	m := d.Message
	if d.Source.Validate() != nil || d.Target.Validate() != nil || m.Parent.Validate() != nil || m.Child.Validate() != nil ||
		d.Source.Namespace != d.Target.Namespace || m.Parent.Namespace != m.Child.Namespace || m.Parent.WorkflowID == m.Child.WorkflowID ||
		!identifier(d.ID) || !identifier(d.TargetBuildID) || !identifier(d.TargetQueue) || m.Version != 1 || !identifier(m.CommandID) {
		return ErrInvalid
	}
	if m.Policy != ParentCloseTerminate && m.Policy != ParentCloseRequestCancel && m.Policy != ParentCloseAbandon {
		return ErrInvalid
	}
	toParent := d.Kind == ChildDeliveryResult || d.Kind == ChildDeliveryCancelAck
	if toParent && (d.Source != m.Child || d.Target != m.Parent) || !toParent && (d.Source != m.Parent || d.Target != m.Child) {
		return ErrInvalid
	}
	switch d.Kind {
	case ChildDeliveryResult:
		if !validState(m.State) || m.State == "" || m.State == StateRunning || !identifier(m.CloseEvent.Type) || m.CancellationID != "" || m.Disposition != "" {
			return ErrInvalid
		}
	case ChildDeliveryClose:
		if m.Policy == ParentCloseAbandon || m.CancellationID != "" {
			return ErrInvalid
		}
	case ChildDeliveryCancel:
		if m.CancellationID != "" && !identifier(m.CancellationID) {
			return ErrInvalid
		}
	case ChildDeliveryCancelAck:
		if !identifier(m.CancellationID) || (m.Disposition != ChildDeliveryApplied && m.Disposition != ChildDeliveryIgnoredClosed && m.Disposition != ChildDeliveryIgnoredExpired) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if d.Kind != ChildDeliveryResult && (m.State != "" || len(m.Output) != 0 || m.CloseEvent.Type != "" || len(m.CloseEvent.Payload) != 0) {
		return ErrInvalid
	}
	if d.Kind != ChildDeliveryCancelAck && m.Disposition != "" {
		return ErrInvalid
	}
	return nil
}

func validateChildCancellations(r CommitRequest) error {
	if len(r.CancelChildren) > 1000 {
		return ErrInvalid
	}
	if len(r.CancelChildren) == 0 {
		return nil
	}
	if r.State != "" && r.State != StateRunning || r.TaskUpdate != nil && r.TaskUpdate.Action != TaskComplete {
		return ErrInvalid
	}
	seen := make(map[string]bool, len(r.CancelChildren))
	for _, c := range r.CancelChildren {
		if !identifier(c.CommandID) || !identifier(c.TargetID) || seen[c.CommandID] {
			return ErrInvalid
		}
		seen[c.CommandID] = true
	}
	return nil
}

// ValidateChildCancellationSource requires an ordinary workflow decision.
func ValidateChildCancellationSource(task Task, cancellations []ChildCancellationSpec) error {
	if len(cancellations) != 0 && (task.Kind != TaskWorkflow || task.LeaseKind != "") {
		return ErrInvalid
	}
	return nil
}

func childMessage(link ChildExecution) ChildMessage {
	return ChildMessage{Version: 1, CommandID: link.CommandID, Parent: link.Parent, Child: link.Start.Key, Policy: link.ParentClosePolicy}
}

func childDeliveryID(kind ChildDeliveryKind, parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Quote(part))
		b.WriteByte(',')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return string(kind) + ":" + hex.EncodeToString(sum[:])
}

func newChildDelivery(source Key, id string, kind ChildDeliveryKind, build, queue string, message ChildMessage, now time.Time) (ChildDelivery, error) {
	target := message.Child
	if kind == ChildDeliveryResult || kind == ChildDeliveryCancelAck {
		target = message.Parent
	}
	d := ChildDelivery{Source: source, ID: id, Kind: kind, Target: target, TargetBuildID: build, TargetQueue: queue, Message: message, CreatedAt: Timestamp(now), AvailableAt: Timestamp(now)}
	return d.Clone(), d.Validate()
}

// ChildDeliveryBatch describes the relationships visible before a decision.
// New children participate in explicit cancellation only, not the bulk fence.
type ChildDeliveryBatch struct {
	Next          Execution
	Request       CommitRequest
	Children      []ChildExecution
	Parent        *ChildExecution
	ParentBuildID string
}

// PrepareChildDeliveries computes messages without mutating either execution.
func PrepareChildDeliveries(b ChildDeliveryBatch, now time.Time) ([]ChildDelivery, error) {
	var result []ChildDelivery
	add := func(id string, kind ChildDeliveryKind, build, queue string, msg ChildMessage) error {
		d, err := newChildDelivery(b.Next.Key, id, kind, build, queue, msg, now)
		if err == nil {
			result = append(result, d)
		}
		return err
	}
	if b.Next.State != StateRunning && b.Parent != nil {
		if len(b.Request.Events) == 0 {
			return nil, ErrInvalid
		}
		msg := childMessage(*b.Parent)
		msg.State, msg.Output, msg.CloseEvent = b.Next.State, b.Next.Output, b.Request.Events[len(b.Request.Events)-1]
		if err := add("result", ChildDeliveryResult, b.ParentBuildID, b.Parent.ParentQueue, msg); err != nil {
			return nil, err
		}
	}
	targets := make(map[string]ChildExecution, len(b.Children)+len(b.Request.Children))
	for _, child := range b.Children {
		targets[child.CommandID] = child
		if child.State != StateRunning {
			continue
		}
		kind, id := ChildDeliveryClose, childDeliveryID(ChildDeliveryClose, child.CommandID)
		if b.Next.State == StateRunning {
			if !b.Request.CancelPendingTasks {
				continue
			}
			kind, id = ChildDeliveryCancel, childDeliveryID(ChildDeliveryCancel, "fence", strconv.FormatInt(b.Next.Revision, 10), child.CommandID)
		} else if child.ParentClosePolicy == ParentCloseAbandon {
			continue
		}
		if err := add(id, kind, child.Start.BuildID, child.Start.Queue, childMessage(child)); err != nil {
			return nil, err
		}
	}
	for _, child := range b.Request.Children {
		targets[child.CommandID] = ChildExecution{Parent: b.Next.Key, ChildStartSpec: child, State: StateRunning}
	}
	for _, cancel := range b.Request.CancelChildren {
		child, ok := targets[cancel.TargetID]
		if !ok {
			return nil, fmt.Errorf("%w: child cancellation target %q", ErrNotFound, cancel.TargetID)
		}
		msg := childMessage(child)
		msg.CancellationID = cancel.CommandID
		if err := add(childDeliveryID(ChildDeliveryCancel, "explicit", cancel.CommandID), ChildDeliveryCancel, child.Start.BuildID, child.Start.Queue, msg); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// CancellationRequest binds generated cancellation to the existing receipt scope.
func (d ChildDelivery) CancellationRequest() CancelExecutionRequest {
	return CancelExecutionRequest{Key: d.Target, BuildID: d.TargetBuildID, RequestID: childDeliveryID(ChildDeliveryCancel, d.Source.Namespace, d.Source.WorkflowID, d.Source.RunID, d.ID), Reason: "parent requested child cancellation"}
}

// CancellationAcknowledgment returns a separate child-to-parent delivery.
func (d ChildDelivery) CancellationAcknowledgment(build, queue, disposition string, now time.Time) (ChildDelivery, error) {
	msg := d.Message
	msg.Disposition = disposition
	id := childDeliveryID(ChildDeliveryCancelAck, msg.Parent.Namespace, msg.Parent.WorkflowID, msg.Parent.RunID, msg.CancellationID)
	return newChildDelivery(d.Target, id, ChildDeliveryCancelAck, build, queue, msg, now)
}
