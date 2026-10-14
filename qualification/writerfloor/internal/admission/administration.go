package admission

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/ctrlplane/managed"
)

func validateCommand(c Command) bool {
	if !operation(c.ID) || c.ExpectedRevision == 0 {
		return false
	}
	expected := Command{ID: c.ID, ExpectedRevision: c.ExpectedRevision, Action: c.Action}
	switch c.Action {
	case "certify":
		expected.Certificate = c.Certificate
		if c.Certificate == nil || !validateCertificate(*c.Certificate) {
			return false
		}
	case "bind":
		expected.Binding = c.Binding
		if c.Binding == nil || !validateBinding(*c.Binding) {
			return false
		}
	case "revoke":
		expected.Revoke = c.Revoke
		p := strings.Split(c.Revoke, ":")
		if len(p) != 2 || !token(p[1]) || (p[0] != "certificate" && p[0] != "binding") {
			return false
		}
	case "begin":
		expected.Activation = c.Activation
		if c.Activation == nil {
			return false
		}
		if _, e := managed.EncodeWriterFloorActivation(*c.Activation); e != nil {
			return false
		}
	case "open":
		expected.Open = c.Open
		if c.Open == nil || !token(c.Open.Executor) || !token(c.Open.Observation.Incarnation) || !digest(c.Open.Evidence) || !digest(c.Open.Frontier) {
			return false
		}
		if _, e := managed.EncodeWriterFloor(c.Open.Observation.Floor); e != nil {
			return false
		}
	case "close":
	default:
		return false
	}
	return reflect.DeepEqual(c, expected)
}

// Apply retains an exact administrative receipt in the same transaction as all
// state/fact changes. Historical replay cannot repeat a reopen or certification.
func (a *Admin) Apply(ctx context.Context, c Command) (Receipt, error) {
	raw, e := encode(c)
	if e != nil {
		return Receipt{}, e
	}
	var owned Command
	if callErr := decode(raw, &owned); callErr != nil {
		return Receipt{}, callErr
	}
	c = owned
	if !validateCommand(c) {
		return Receipt{}, ErrDenied
	}
	request, e := hash(c)
	if e != nil {
		return Receipt{}, e
	}
	tx, s, e := a.begin(ctx)
	if e != nil {
		return Receipt{}, e
	}
	defer tx.Rollback() //nolint:errcheck // Commit or rollback owns cleanup.
	var saved adminRecord
	e = a.fact(ctx, tx, "admin", c.ID, &saved)
	if e == nil {
		old := saved.Receipt
		prior, hashErr := hash(saved.Command)
		if hashErr != nil || prior != old.Request || old.ID != c.ID || saved.Command.ID != c.ID || old.Request != request || old.Revision == 0 || old.Revision > s.Revision {
			return Receipt{}, ErrConflict
		}
		return old, tx.Commit()
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return Receipt{}, e
	}
	if c.ExpectedRevision != s.Revision {
		return Receipt{}, ErrConflict
	}
	switch c.Action {
	case "certify":
		if s.Certificates >= 4 {
			return Receipt{}, ErrCapacity
		}
		if callErr := a.insert(ctx, tx, &s, "certificate", c.Certificate.ID, *c.Certificate); callErr != nil {
			return Receipt{}, callErr
		}
		s.Certificates++
	case "bind":
		if callErr := a.bind(ctx, tx, &s, *c.Binding); callErr != nil {
			return Receipt{}, callErr
		}
	case "revoke":
		parts := strings.Split(c.Revoke, ":")
		var raw []byte
		if callErr := tx.QueryRowContext(ctx, `SELECT payload FROM writerfloor_facts WHERE authority=$1 AND kind=$2 AND id=$3`, a.authority, parts[0], parts[1]).Scan(&raw); callErr != nil {
			return Receipt{}, callErr
		}
		if callErr := a.insert(ctx, tx, &s, "revocation", c.Revoke, c.Revoke); callErr != nil {
			return Receipt{}, callErr
		}
	case "begin":
		if callErr := beginActivation(&s, *c.Activation); callErr != nil {
			return Receipt{}, callErr
		}
	case "open":
		if callErr := a.open(ctx, tx, &s, *c.Open); callErr != nil {
			return Receipt{}, callErr
		}
	case "close":
		s.Mode = "closed"
	}
	receipt := Receipt{ID: c.ID, Request: request, Revision: s.Revision + 1}
	if callErr := a.insert(ctx, tx, &s, "admin", c.ID, adminRecord{c, receipt}); callErr != nil {
		return Receipt{}, callErr
	}
	if callErr := a.save(ctx, tx, &s); callErr != nil {
		return Receipt{}, callErr
	}
	if callErr := ctx.Err(); callErr != nil {
		return Receipt{}, callErr
	}
	return receipt, tx.Commit()
}
func beginActivation(s *State, request managed.WriterFloorActivation) error {
	if request.AuthorityID != s.Authority || s.Pending != nil {
		return ErrConflict
	}
	epoch, e := number(string(request.Epoch))
	if e != nil {
		return e
	}
	protocol, e := number(string(request.MinimumProtocol))
	if e != nil {
		return e
	}
	if s.Current == nil {
		if epoch != 1 || request.ExpectedDigest != "" {
			return ErrConflict
		}
	} else {
		previous, e := number(string(s.Current.Epoch))
		if e != nil {
			return e
		}
		floor, e := number(string(s.Current.MinimumProtocol))
		if e != nil {
			return e
		}
		if previous == ^uint64(0) || epoch != previous+1 || protocol < floor || request.ExpectedDigest != s.Current.Digest {
			return ErrConflict
		}
	}
	copyRequest := request
	s.Pending = &copyRequest
	s.Mode = "draining"
	return nil
}
func (a *Admin) bind(ctx context.Context, tx *sql.Tx, s *State, b Binding) error {
	var c Certificate
	if e := a.fact(ctx, tx, "certificate", b.Certificate, &c); e != nil {
		return e
	}
	if !validateCertificate(c) || c.ID != b.Certificate {
		return ErrCorrupt
	}
	if e := a.revoked(ctx, tx, "certificate", c.ID); e != nil {
		return e
	}
	// Never reuse a runtime process ID or private proxy key, even after revocation.
	rows, e := tx.QueryContext(ctx, `SELECT id,payload FROM writerfloor_facts WHERE authority=$1 AND kind='binding' LIMIT 4097`, a.authority)
	if e != nil {
		return e
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var key string
		var raw []byte
		if e = rows.Scan(&key, &raw); e != nil {
			break
		}
		var prior Binding
		if decode(raw, &prior) != nil || prior.ID != key || !validateBinding(prior) {
			e = ErrCorrupt
			break
		}
		count++
		if count > MaxRecords {
			e = ErrCapacity
			break
		}
		if prior.RuntimeID == b.RuntimeID || prior.PeerDigest == b.PeerDigest {
			e = ErrConflict
			break
		}
	}
	if e == nil {
		e = rows.Err()
	}
	closeErr := rows.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	return a.insert(ctx, tx, s, "binding", b.ID, b)
}
func (a *Admin) open(ctx context.Context, tx *sql.Tx, s *State, request Open) error {
	if s.Mode == "open" || request.Frontier != s.Frontier || request.Sequence != s.Sequence {
		return ErrConflict
	}
	floor := request.Observation.Floor
	if floor.AuthorityID != s.Authority {
		return ErrConflict
	}
	if s.Pending != nil {
		expected, e := managed.WriterFloorActivationDigest(*s.Pending)
		if e != nil {
			return e
		}
		if floor.ActivationDigest != expected {
			return ErrConflict
		}
	} else if s.Current == nil || !sameFloor(floor, *s.Current) {
		return ErrConflict
	}
	epoch, e := number(string(floor.Epoch))
	if e != nil {
		return e
	}
	protocol, e := number(string(floor.MinimumProtocol))
	if e != nil {
		return e
	}
	if _, e = a.binding(ctx, tx, request.Executor, epoch, protocol, "executor"); e != nil {
		return e
	}
	verifyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// Both arguments are detached values. Verifier cannot mutate pending/current via pointers.
	b, e := encode(s)
	if e != nil {
		return e
	}
	var detached State
	if callErr := decode(b, &detached); callErr != nil {
		return callErr
	}
	if callErr := a.verifier.VerifyOpen(verifyCtx, detached, request); callErr != nil {
		return callErr
	}
	s.Current = &floor
	s.Pending = nil
	s.Incarnation = request.Observation.Incarnation
	s.Executor = request.Executor
	s.Mode = "open"
	return nil
}
