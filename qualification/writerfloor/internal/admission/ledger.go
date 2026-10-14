package admission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/ctrlplane/managed"
)

// Schema is installed by trusted setup using a separate administration connection.
// Runtime clients never receive any ledger credentials. Tables live in the fixed
// configured search_path, which must not contain a schema writable by clients.
const Schema = `
CREATE TABLE writerfloor_state (
 authority text PRIMARY KEY CHECK (length(authority) BETWEEN 1 AND 128),
 payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 4096)
);
CREATE TABLE writerfloor_facts (
 authority text NOT NULL REFERENCES writerfloor_state(authority),
 kind text NOT NULL CHECK (kind IN ('certificate','binding','revocation','admin','delegation')),
 id text NOT NULL CHECK (length(id) BETWEEN 1 AND 160),
 payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 4096),
 PRIMARY KEY(authority,kind,id)
);
CREATE FUNCTION writerfloor_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'writerfloor immutable facts'; END; $$;
CREATE TRIGGER writerfloor_immutable BEFORE UPDATE OR DELETE ON writerfloor_facts
 FOR EACH ROW EXECUTE FUNCTION writerfloor_immutable();
REVOKE ALL ON writerfloor_state,writerfloor_facts FROM PUBLIC;
`

type ledger struct {
	db        *sql.DB
	authority string
}

// Admin is a trusted deployment administration handle. Do not mount it on client routes.
type Admin struct {
	ledger
	verifier CutoverVerifier
}

// Runtime admits exact invocations but exposes no installation or certification API.
type Runtime struct {
	ledger
	observer Observer
	verifier Verifier
	delegate Delegate
}

func NewAdmin(db *sql.DB, authority string, verifier CutoverVerifier) (*Admin, error) {
	if db == nil || !token(authority) || nilValue(verifier) {
		return nil, ErrDenied
	}
	return &Admin{ledger{db, authority}, verifier}, nil
}
func NewRuntime(db *sql.DB, authority string, observer Observer, verifier Verifier, delegate Delegate) (*Runtime, error) {
	if db == nil || !token(authority) || nilValue(observer) || nilValue(verifier) || nilValue(delegate) {
		return nil, ErrDenied
	}
	return &Runtime{ledger{db, authority}, observer, verifier, delegate}, nil
}

// Install is pristine-only. Retrying an uncertain installation uses Read, never a reset.
func (a *Admin) Install(ctx context.Context, limit uint64) error {
	if limit == 0 || limit > MaxRecords {
		return ErrCapacity
	}
	s := State{Authority: a.authority, Revision: 1, Mode: "closed", Limit: limit, Frontier: managed.Digest([]byte("writerfloor.empty.v1"))}
	b, e := encode(s)
	if e != nil {
		return e
	}
	_, e = a.db.ExecContext(ctx, `INSERT INTO writerfloor_state(authority,payload) VALUES($1,$2)`, a.authority, b)
	return e
}
func (l ledger) begin(ctx context.Context) (*sql.Tx, State, error) {
	tx, e := l.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return nil, State{}, e
	}
	s, e := l.load(ctx, tx, true)
	if e != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			e = errors.Join(e, rollbackErr)
		}
		return nil, State{}, e
	}
	return tx, s, nil
}
func (l ledger) load(ctx context.Context, tx *sql.Tx, lock bool) (State, error) {
	q := `SELECT payload FROM writerfloor_state WHERE authority=$1`
	if lock {
		q += ` FOR UPDATE`
	}
	var b []byte
	if e := tx.QueryRowContext(ctx, q, l.authority).Scan(&b); e != nil {
		return State{}, e
	}
	var s State
	if decode(b, &s) != nil || s.Authority != l.authority || validateState(s) != nil {
		return State{}, ErrCorrupt
	}
	var count, certificates, delegations uint64
	if e := tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE kind='certificate'),count(*) FILTER (WHERE kind='delegation') FROM writerfloor_facts WHERE authority=$1`, l.authority).Scan(&count, &certificates, &delegations); e != nil {
		return State{}, e
	}
	if count != s.Records || certificates != s.Certificates || delegations != s.Sequence {
		return State{}, ErrCorrupt
	}
	if e := l.verifyFrontier(ctx, tx, s); e != nil {
		return State{}, e
	}
	return s, nil
}
func (l ledger) read(ctx context.Context) (State, error) {
	tx, e := l.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return State{}, e
	}
	defer tx.Rollback() //nolint:errcheck // Read-only snapshot cleanup.
	s, e := l.load(ctx, tx, false)
	if e != nil {
		return State{}, e
	}
	return s, tx.Commit()
}
func (a *Admin) Read(ctx context.Context) (State, error)   { return a.read(ctx) }
func (r *Runtime) Read(ctx context.Context) (State, error) { return r.read(ctx) }
func (l ledger) fact(ctx context.Context, tx *sql.Tx, kind, key string, out any) error {
	var b []byte
	e := tx.QueryRowContext(ctx, `SELECT payload FROM writerfloor_facts WHERE authority=$1 AND kind=$2 AND id=$3`, l.authority, kind, key).Scan(&b)
	if e != nil {
		return e
	}
	return decode(b, out)
}
func (l ledger) insert(ctx context.Context, tx *sql.Tx, s *State, kind, key string, v any) error {
	if s.Records >= s.Limit {
		return ErrCapacity
	}
	b, e := encode(v)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO writerfloor_facts(authority,kind,id,payload) VALUES($1,$2,$3,$4)`, l.authority, kind, key, b); e != nil {
		return e
	}
	s.Records++
	return nil
}
func (l ledger) save(ctx context.Context, tx *sql.Tx, s *State) error {
	if s.Revision >= MaxRecords*4 {
		return ErrCapacity
	}
	s.Revision++
	if e := validateState(*s); e != nil {
		return e
	}
	b, e := encode(s)
	if e != nil {
		return e
	}
	result, e := tx.ExecContext(ctx, `UPDATE writerfloor_state SET payload=$2 WHERE authority=$1`, l.authority, b)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return ErrCorrupt
	}
	return nil
}
func (l ledger) revoked(ctx context.Context, tx *sql.Tx, kind, key string) error {
	var got string
	e := l.fact(ctx, tx, "revocation", kind+":"+key, &got)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if got != kind+":"+key {
		return ErrCorrupt
	}
	return ErrDenied
}
func (l ledger) binding(ctx context.Context, tx *sql.Tx, key string, epoch, protocol uint64, role string) (Binding, error) {
	var b Binding
	if e := l.fact(ctx, tx, "binding", key, &b); e != nil {
		return b, e
	}
	if b.ID != key || !validateBinding(b) {
		return b, ErrCorrupt
	}
	if b.Epoch != epoch || b.Role != role {
		return b, ErrDenied
	}
	if e := l.revoked(ctx, tx, "binding", b.ID); e != nil {
		return b, e
	}
	var c Certificate
	if e := l.fact(ctx, tx, "certificate", b.Certificate, &c); e != nil {
		return b, e
	}
	if c.ID != b.Certificate || !validateCertificate(c) {
		return b, ErrCorrupt
	}
	if c.Protocol == 0 || c.Protocol < protocol {
		return b, ErrDenied
	}
	return b, l.revoked(ctx, tx, "certificate", c.ID)
}
func (l ledger) peer(ctx context.Context, tx *sql.Tx, p Peer, epoch, protocol uint64) (Binding, error) {
	if !token(p.Principal) || !digest(p.CertificateDigest) {
		return Binding{}, ErrDenied
	}
	rows, e := tx.QueryContext(ctx, `SELECT id,payload FROM writerfloor_facts WHERE authority=$1 AND kind='binding' ORDER BY id LIMIT 4097`, l.authority)
	if e != nil {
		return Binding{}, e
	}
	defer rows.Close()
	var selected string
	count := 0
	for rows.Next() {
		var key string
		var raw []byte
		if e = rows.Scan(&key, &raw); e != nil {
			break
		}
		var b Binding
		if decode(raw, &b) != nil || b.ID != key || !validateBinding(b) {
			e = ErrCorrupt
			break
		}
		count++
		if count > MaxRecords {
			e = ErrCapacity
			break
		}
		if b.Principal == p.Principal && b.PeerDigest == p.CertificateDigest {
			if selected != "" {
				e = ErrCorrupt
				break
			}
			selected = key
		}
	}
	if e == nil {
		e = rows.Err()
	}
	closeErr := rows.Close()
	if e != nil {
		return Binding{}, e
	}
	if closeErr != nil {
		return Binding{}, closeErr
	}
	if selected == "" {
		return Binding{}, ErrDenied
	}
	return l.binding(ctx, tx, selected, epoch, protocol, "executor")
}

// Delegate records custody before its single configured dispatch attempt. Replays
// inspect the saved fact and do not call Observe, Verify or Dispatch again.
func (r *Runtime) Delegate(ctx context.Context, p Peer, inv Invocation) (Delegation, error) {
	if !validateInvocation(inv) {
		return Delegation{}, ErrDenied
	}
	tx, s, e := r.begin(ctx)
	if e != nil {
		return Delegation{}, e
	}
	defer tx.Rollback() //nolint:errcheck // Commit or rollback ends the transaction.
	var old Delegation
	e = r.fact(ctx, tx, "delegation", inv.ID, &old)
	if e == nil {
		if old.Invocation != inv || old.Sequence == 0 || old.Sequence > s.Sequence || !digest(old.Previous) || !digest(old.Floor) {
			return Delegation{}, ErrConflict
		}
		var b Binding
		if callErr := r.fact(ctx, tx, "binding", old.Workload, &b); callErr != nil {
			return Delegation{}, callErr
		}
		if !validateBinding(b) || b.ID != old.Workload || b.Principal != p.Principal || b.PeerDigest != p.CertificateDigest {
			return Delegation{}, ErrDenied
		}
		return old, tx.Commit()
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return Delegation{}, e
	}
	if s.Mode != "open" || s.Current == nil {
		return Delegation{}, ErrDenied
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	observation, e := r.observer.Observe(checkCtx)
	if e != nil {
		cause := e
		s.Mode = "quarantined"
		if callErr := r.save(ctx, tx, &s); callErr != nil {
			return Delegation{}, callErr
		}
		if callErr := tx.Commit(); callErr != nil {
			return Delegation{}, callErr
		}
		return Delegation{}, fmt.Errorf("%w: %w", ErrUnavailable, cause)
	}
	if _, e = managed.EncodeWriterFloor(observation.Floor); e != nil || !sameFloor(observation.Floor, *s.Current) || observation.Incarnation != s.Incarnation {
		s.Mode = "quarantined"
		if callErr := r.save(ctx, tx, &s); callErr != nil {
			return Delegation{}, callErr
		}
		if callErr := tx.Commit(); callErr != nil {
			return Delegation{}, callErr
		}
		return Delegation{}, ErrDenied
	}
	epoch, e := number(string(s.Current.Epoch))
	if e != nil {
		return Delegation{}, e
	}
	protocol, e := number(string(s.Current.MinimumProtocol))
	if e != nil {
		return Delegation{}, e
	}
	b, e := r.peer(ctx, tx, p, epoch, protocol)
	if e != nil {
		return Delegation{}, e
	}
	if b.ID != s.Executor {
		return Delegation{}, ErrDenied
	}
	if _, e = r.binding(ctx, tx, s.Executor, epoch, protocol, "executor"); e != nil {
		return Delegation{}, e
	}
	if callErr := r.verifier.Verify(checkCtx, inv); callErr != nil {
		return Delegation{}, callErr
	}
	d := Delegation{Invocation: inv, Workload: b.ID, Executor: s.Executor, Floor: s.Current.Digest, Incarnation: s.Incarnation, Sequence: s.Sequence + 1, Previous: s.Frontier}
	if callErr := r.insert(ctx, tx, &s, "delegation", inv.ID, d); callErr != nil {
		return Delegation{}, callErr
	}
	s.Mode = "delegating"
	s.Sequence = d.Sequence
	s.Frontier, e = hash(d)
	if e != nil {
		return Delegation{}, e
	}
	if callErr := r.save(ctx, tx, &s); callErr != nil {
		return Delegation{}, callErr
	}
	if callErr := ctx.Err(); callErr != nil {
		return Delegation{}, callErr
	}
	if callErr := tx.Commit(); callErr != nil {
		return Delegation{}, callErr
	}
	// No returned boolean, read API or stored grant can re-enter this branch.
	if callErr := ctx.Err(); callErr != nil {
		return d, callErr
	}
	if callErr := r.delegate.Dispatch(ctx, d); callErr != nil {
		return d, callErr
	}
	return d, r.acknowledge(ctx, d, s.Revision)
}

// Page is bounded inspection. A complete page is never physical closure evidence.
func (r *Runtime) Page(ctx context.Context, after string, limit int) ([]Delegation, error) {
	if limit < 1 || limit > MaxPage || after != "" && !operation(after) {
		return nil, ErrDenied
	}
	rows, e := r.db.QueryContext(ctx, `SELECT id,payload FROM writerfloor_facts WHERE authority=$1 AND kind='delegation' AND id>$2 ORDER BY id LIMIT $3`, r.authority, after, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := make([]Delegation, 0, limit)
	for rows.Next() {
		var key string
		var raw []byte
		if callErr := rows.Scan(&key, &raw); callErr != nil {
			return nil, callErr
		}
		var d Delegation
		if decode(raw, &d) != nil || d.Invocation.ID != key || !validateInvocation(d.Invocation) || d.Sequence == 0 || !digest(d.Floor) || !digest(d.Previous) {
			return nil, ErrCorrupt
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// verifyFrontier checks the complete finite custody chain in the same snapshot.
// This detects storage damage; the independent ledger administrator stays trusted.
func (l ledger) verifyFrontier(ctx context.Context, tx *sql.Tx, s State) error {
	rows, e := tx.QueryContext(ctx, `SELECT id,payload FROM writerfloor_facts WHERE authority=$1 AND kind='delegation' ORDER BY id LIMIT 4097`, l.authority)
	if e != nil {
		return e
	}
	defer rows.Close()
	records := make(map[uint64]Delegation)
	for rows.Next() {
		if e = ctx.Err(); e != nil {
			break
		}
		var key string
		var raw []byte
		if e = rows.Scan(&key, &raw); e != nil {
			break
		}
		var d Delegation
		if decode(raw, &d) != nil || !validateInvocation(d.Invocation) || d.Invocation.ID != key || d.Sequence == 0 || d.Sequence > s.Sequence || !digest(d.Floor) || !digest(d.Previous) || !token(d.Workload) || !token(d.Executor) || !token(d.Incarnation) {
			e = ErrCorrupt
			break
		}
		if _, exists := records[d.Sequence]; exists {
			e = ErrCorrupt
			break
		}
		records[d.Sequence] = d
		if len(records) > MaxRecords {
			e = ErrCapacity
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
	frontier := managed.Digest([]byte("writerfloor.empty.v1"))
	for n := uint64(1); n <= s.Sequence; n++ {
		d, exists := records[n]
		if !exists || d.Previous != frontier {
			return ErrCorrupt
		}
		frontier, e = hash(d)
		if e != nil {
			return e
		}
	}
	if frontier != s.Frontier {
		return ErrCorrupt
	}
	return nil
}

// acknowledge reopens only this exact in-flight admission barrier. It records no
// effect completion. Close, cutover or reconciliation always wins over a late ack.
func (r *Runtime) acknowledge(ctx context.Context, d Delegation, revision uint64) error {
	tx, s, e := r.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback() //nolint:errcheck // Commit or rollback owns cleanup.
	frontier, e := hash(d)
	if e != nil {
		return e
	}
	if s.Mode != "delegating" || s.Revision != revision || s.Sequence != d.Sequence || s.Frontier != frontier || s.Executor != d.Executor || s.Incarnation != d.Incarnation || s.Current == nil || s.Current.Digest != d.Floor {
		return ErrConflict
	}
	s.Mode = "open"
	if callErr := r.save(ctx, tx, &s); callErr != nil {
		return callErr
	}
	if callErr := ctx.Err(); callErr != nil {
		return callErr
	}
	return tx.Commit()
}

// AuthorityID returns the immutable configured ledger authority for policy binding.
// It is an identity accessor and never grants admission.
func (r *Runtime) AuthorityID() string {
	if r == nil {
		return ""
	}
	return r.authority
}
