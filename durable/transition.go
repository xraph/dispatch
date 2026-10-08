package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// Validate checks an execution identity before any database access.
func (k Key) Validate() error {
	if !identifier(k.Namespace) || !identifier(k.WorkflowID) || !identifier(k.RunID) {
		return fmt.Errorf("%w: namespace, workflow ID and run ID are required", ErrInvalid)
	}
	return nil
}

func identifier(s string) bool {
	return s != "" && len(s) <= 512 && strings.TrimSpace(s) == s && !strings.ContainsRune(s, 0) && utf8.ValidString(s)
}

// Validate checks the immutable start request.
func (r StartRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if !identifier(r.RequestID) || !identifier(r.WorkflowType) || !identifier(r.BuildID) || !identifier(r.Queue) {
		return fmt.Errorf("%w: request ID, workflow type, build ID and queue are required", ErrInvalid)
	}
	return nil
}

// Validate checks task routing and the requested lease duration.
func (r ClaimRequest) Validate() error {
	if !identifier(r.Namespace) || !identifier(r.Queue) || !identifier(r.Owner) || !validKind(r.Kind) {
		return fmt.Errorf("%w: namespace, queue, owner and task kind are required", ErrInvalid)
	}
	return ValidateLease(r.LeaseDuration)
}

// ValidateLease bounds ownership grants and rejects sub-microsecond durations.
func ValidateLease(ttl time.Duration) error {
	if ttl < time.Microsecond || ttl > 24*time.Hour {
		return fmt.Errorf("%w: lease duration must be between one microsecond and 24 hours", ErrInvalid)
	}
	return nil
}

// Validate checks a task token's shape; the store checks its ownership.
func (t TaskToken) Validate() error {
	if !identifier(t.TaskID) || !identifier(t.Owner) || t.Epoch <= 0 {
		return fmt.Errorf("%w: task ID, owner and positive epoch are required", ErrInvalid)
	}
	return nil
}

// Validate checks a transition before any writes occur.
func (r CommitRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if err := r.Token.Validate(); err != nil {
		return err
	}
	if !identifier(r.RequestID) || r.ExpectedRevision < 1 || len(r.Events) == 0 || len(r.Events) > 1000 || len(r.Tasks) > 1000 {
		return fmt.Errorf("%w: request ID, revision and 1..1000 events are required; at most 1000 tasks", ErrInvalid)
	}
	for _, evt := range r.Events {
		if !identifier(evt.Type) {
			return fmt.Errorf("%w: event type is required", ErrInvalid)
		}
	}
	if !validState(r.State) || (r.State != "" && r.State != StateRunning && len(r.Tasks) != 0) {
		return fmt.Errorf("%w: invalid state or tasks scheduled on closure", ErrInvalid)
	}
	if len(r.Output) != 0 && (r.State == "" || r.State == StateRunning) {
		return fmt.Errorf("%w: output requires a terminal state", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(r.Tasks))
	for _, task := range r.Tasks {
		if !identifier(task.ID) || !identifier(task.Queue) || !validKind(task.Kind) ||
			(task.Kind == TaskTimer && task.AvailableAt.IsZero()) ||
			(!task.AvailableAt.IsZero() && (task.AvailableAt.Year() < 1 || task.AvailableAt.Year() > 9999)) {
			return fmt.Errorf("%w: invalid task identity, routing or deadline", ErrInvalid)
		}
		if _, exists := seen[task.ID]; exists {
			return fmt.Errorf("%w: duplicate task ID", ErrInvalid)
		}
		seen[task.ID] = struct{}{}
	}
	return nil
}

func validKind(kind TaskKind) bool {
	return kind == TaskWorkflow || kind == TaskActivity || kind == TaskTimer
}

func validState(state State) bool {
	switch state {
	case "", StateRunning, StateCompleted, StateFailed, StateCancelled, StateTerminated, StateTimedOut, StateContinuedAsNew:
		return true
	default:
		return false
	}
}

// Fingerprint binds a receipt to the exact typed request, including its operation.
func Fingerprint(operation string, request any) (string, error) {
	data, err := json.Marshal(struct {
		Operation string `json:"operation"`
		Request   any    `json:"request"`
	}{operation, request})
	if err != nil {
		return "", fmt.Errorf("%w: encode request: %w", ErrInvalid, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// Timestamp normalizes persisted time to PostgreSQL's microsecond precision.
func Timestamp(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// CheckLease verifies ownership and expiry using the store's current time.
func CheckLease(task Task, token TaskToken, now time.Time) error {
	if task.ID != token.TaskID || task.Owner != token.Owner || task.Epoch != token.Epoch || !task.LeaseUntil.After(now) {
		return ErrLeaseLost
	}
	return nil
}

// Advance validates the revision and lease, then computes a transition's state.
// The backend must hold its mutation lock and persist the result atomically with
// events, tasks and the receipt. It separately checks that the task is unfinished.
func Advance(current Execution, task Task, r CommitRequest, now time.Time) (Execution, Receipt, error) {
	if current.State != StateRunning {
		return Execution{}, Receipt{}, ErrClosed
	}
	if err := CheckLease(task, r.Token, now); err != nil {
		return Execution{}, Receipt{}, err
	}
	if current.Revision != r.ExpectedRevision {
		return Execution{}, Receipt{}, ErrRevisionConflict
	}
	if current.Revision == math.MaxInt64 || current.LastSequence > math.MaxInt64-int64(len(r.Events)) {
		return Execution{}, Receipt{}, fmt.Errorf("%w: execution sequence exhausted", ErrInvalid)
	}
	next := current
	next.Revision++
	next.LastSequence += int64(len(r.Events))
	next.UpdatedAt = Timestamp(now)
	if r.State != "" {
		next.State = r.State
	}
	if next.State != StateRunning {
		next.Output = append([]byte(nil), r.Output...)
	}
	return next, Receipt{Revision: next.Revision, FirstSequence: current.LastSequence + 1, LastSequence: next.LastSequence}, nil
}
