// Package admission retains external writer admission independently of authority restores.
package admission

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
)

const MaxRecord = 4096
const MaxRecords = 4096
const MaxPage = 64

var ErrDenied = errors.New("writerfloor: admission denied")
var ErrConflict = errors.New("writerfloor: exact replay or revision conflict")
var ErrCorrupt = errors.New("writerfloor: corrupt ledger")
var ErrCapacity = errors.New("writerfloor: frozen ledger capacity exhausted")
var ErrUnavailable = errors.New("writerfloor: admission unavailable")

// Certificate is assigned by deployment administration after artifact-specific review.
// Protocol zero explicitly denies execution. No number is earned by this package.
type Certificate struct {
	ID            string `json:"id"`
	Artifact      string `json:"artifact"`
	Executable    string `json:"executable"`
	Modules       string `json:"modules"`
	Specification string `json:"specification"`
	Evidence      string `json:"evidence"`
	Protocol      uint64 `json:"protocol"`
}

// Binding names a single launcher-created process and its private authenticated proxy.
// Replacement processes and changed keys require new IDs, even for identical bytes.
type Binding struct {
	ID          string `json:"id"`
	Certificate string `json:"certificate"`
	RuntimeID   string `json:"runtime_id"`
	Principal   string `json:"principal"`
	PeerDigest  string `json:"peer_digest"`
	Epoch       uint64 `json:"epoch"`
	Role        string `json:"role"`
}

// Peer is produced only by the configured authentication boundary, never JSON input.
type Peer struct{ Principal, CertificateDigest string }

// Invocation carries stable existing authority identity, never a reusable grant.
// Slot and ID are checked against the existing permission by the configured verifier.
type Invocation struct {
	ID         string `json:"id"`
	Tenant     string `json:"tenant"`
	Target     string `json:"target"`
	Operation  string `json:"operation"`
	Attempt    string `json:"attempt"`
	Generation string `json:"generation"`
	Permission string `json:"permission"`
	Command    string `json:"command"`
	Kind       string `json:"kind"`
	Slot       string `json:"slot"`
}

// Observation includes an independent launcher restore incarnation. A same-marker
// authority restore must change Incarnation before the restored database is exposed.
type Observation struct {
	Floor       managed.WriterFloor `json:"floor"`
	Incarnation string              `json:"incarnation"`
}

type State struct {
	Authority    string                         `json:"authority"`
	Revision     uint64                         `json:"revision"`
	Mode         string                         `json:"mode"`
	Limit        uint64                         `json:"limit"`
	Records      uint64                         `json:"records"`
	Certificates uint64                         `json:"certificates"`
	Sequence     uint64                         `json:"sequence"`
	Frontier     string                         `json:"frontier"`
	Current      *managed.WriterFloor           `json:"current,omitempty"`
	Pending      *managed.WriterFloorActivation `json:"pending,omitempty"`
	Incarnation  string                         `json:"incarnation"`
	Executor     string                         `json:"executor"`
}

// Command is available only to the trusted administration composition, not runtime HTTP.
type Command struct {
	ID               string                         `json:"id"`
	ExpectedRevision uint64                         `json:"expected_revision"`
	Action           string                         `json:"action"`
	Certificate      *Certificate                   `json:"certificate,omitempty"`
	Binding          *Binding                       `json:"binding,omitempty"`
	Revoke           string                         `json:"revoke,omitempty"`
	Activation       *managed.WriterFloorActivation `json:"activation,omitempty"`
	Open             *Open                          `json:"open,omitempty"`
}

// Open names the exact floor, frontier and executor examined by trusted cutover
// administration. Evidence covers session revocation and restore reconciliation.
type Open struct {
	Observation Observation `json:"observation"`
	Frontier    string      `json:"frontier"`
	Sequence    uint64      `json:"sequence"`
	Executor    string      `json:"executor"`
	Evidence    string      `json:"evidence"`
}

type adminRecord struct {
	Command Command `json:"command"`
	Receipt Receipt `json:"receipt"`
}

type Receipt struct {
	ID       string `json:"id"`
	Request  string `json:"request"`
	Revision uint64 `json:"revision"`
}
type Delegation struct {
	Invocation  Invocation `json:"invocation"`
	Workload    string     `json:"workload"`
	Executor    string     `json:"executor"`
	Floor       string     `json:"floor"`
	Incarnation string     `json:"incarnation"`
	Sequence    uint64     `json:"sequence"`
	Previous    string     `json:"previous"`
}

// Observer reads the protected marker and independently retained restore incarnation.
type Observer interface {
	Observe(ctx context.Context) (Observation, error)
}

// Verifier authenticates the existing permission and fixed original invocation selection.
// It must refuse a newly invented ID for an already selected invocation slot.
type Verifier interface {
	Verify(ctx context.Context, invocation Invocation) error
}

// Delegate performs one configured exact dispatch attempt. Errors, including response
// loss, never authorize another attempt. It must not retry internally.
type Delegate interface {
	Dispatch(ctx context.Context, delegation Delegation) error
}

// CutoverVerifier authenticates affirmative shutdown, credential invalidation and
// exact-frontier reconciliation evidence. Marker equality alone cannot satisfy it.
type CutoverVerifier interface {
	VerifyOpen(ctx context.Context, state State, request Open) error
}

func nilValue(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return r.IsNil()
	}
	return false
}
func token(s string) bool {
	return s != "" && len(s) <= 128 && strings.IndexFunc(s, func(r rune) bool {
		allowed := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' || r == ':'
		return !allowed
	}) < 0
}
func digest(s string) bool {
	if len(s) != 64 {
		return false
	}
	b, e := hex.DecodeString(s)
	return e == nil && hex.EncodeToString(b) == s
}
func number(s string) (uint64, error) {
	n, e := strconv.ParseUint(s, 10, 64)
	if e != nil || n == 0 || strconv.FormatUint(n, 10) != s {
		return 0, ErrCorrupt
	}
	return n, nil
}
func operation(s string) bool {
	_, e := id.ParseWithPrefix(s, managed.PrefixOperation)
	return e == nil
}
func encode(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	if len(b) > MaxRecord {
		return nil, ErrCapacity
	}
	return b, nil
}
func decode(b []byte, v any) error {
	if len(b) == 0 || len(b) > MaxRecord {
		return ErrCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return ErrCorrupt
	}
	exact, e := encode(v)
	if e != nil || !bytes.Equal(b, exact) {
		return ErrCorrupt
	}
	return nil
}
func hash(v any) (string, error) {
	b, e := encode(v)
	if e != nil {
		return "", e
	}
	return managed.Digest(b), nil
}
func validateCertificate(c Certificate) bool {
	return token(c.ID) && !strings.Contains(c.ID, ":") && digest(c.Artifact) && digest(c.Executable) && digest(c.Modules) && digest(c.Specification) && digest(c.Evidence)
}
func validateBinding(b Binding) bool {
	return token(b.ID) && !strings.Contains(b.ID, ":") && token(b.Certificate) && token(b.RuntimeID) && token(b.Principal) && digest(b.PeerDigest) && b.Epoch > 0 && (b.Role == "client" || b.Role == "executor")
}
func validateInvocation(i Invocation) bool {
	return operation(i.ID) && token(i.Tenant) && token(i.Target) && operation(i.Operation) && token(i.Attempt) && token(i.Generation) && digest(i.Permission) && digest(i.Command) && (i.Kind == "provider" || i.Kind == "native") && token(i.Slot)
}
func validateState(s State) error {
	if !token(s.Authority) || s.Revision == 0 || s.Limit == 0 || s.Limit > MaxRecords || s.Records > s.Limit || s.Certificates > 4 || s.Sequence > s.Records || !digest(s.Frontier) {
		return ErrCorrupt
	}
	switch s.Mode {
	case "closed", "draining", "open", "quarantined", "delegating":
	default:
		return ErrCorrupt
	}
	if s.Current != nil {
		if _, e := managed.EncodeWriterFloor(*s.Current); e != nil || s.Current.AuthorityID != s.Authority {
			return ErrCorrupt
		}
	}
	if s.Pending != nil {
		if _, e := managed.EncodeWriterFloorActivation(*s.Pending); e != nil || s.Pending.AuthorityID != s.Authority {
			return ErrCorrupt
		}
	}
	if s.Current == nil {
		if s.Incarnation != "" || s.Executor != "" || s.Sequence != 0 {
			return ErrCorrupt
		}
	} else if !token(s.Incarnation) || !token(s.Executor) {
		return ErrCorrupt
	}
	if s.Pending != nil {
		prior := s
		prior.Pending = nil
		if beginActivation(&prior, *s.Pending) != nil {
			return ErrCorrupt
		}
	}
	if (s.Mode == "open" || s.Mode == "delegating") && (s.Current == nil || s.Pending != nil) {
		return ErrCorrupt
	}
	return nil
}

func sameFloor(a, b managed.WriterFloor) bool {
	x, e := managed.EncodeWriterFloor(a)
	if e != nil {
		return false
	}
	y, e := managed.EncodeWriterFloor(b)
	return e == nil && bytes.Equal(x, y)
}
