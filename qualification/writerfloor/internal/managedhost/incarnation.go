package managedhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
)

// RestoreIncarnation keeps the launcher identity outside authority database backups.
// Only trusted deployment administration may initialize or rotate it. Runtime
// receives its read method. The directory must survive every authority restore.
type RestoreIncarnation struct {
	journal   *docker.FileManagedDockerJournal
	authority string
	key       string
}

type incarnationRecord struct {
	Authority string `json:"authority"`
	Current   string `json:"current"`
	Previous  string `json:"previous"`
}

// OpenRestoreIncarnation opens protected local state without initializing it.
func OpenRestoreIncarnation(directory, authority string) (*RestoreIncarnation, error) {
	if authority == "" || len(authority) > 128 {
		return nil, managed.ErrInvalid
	}
	journal, err := docker.OpenFileManagedDockerJournal(directory)
	if err != nil {
		return nil, err
	}
	return &RestoreIncarnation{journal: journal, authority: authority, key: managed.Digest([]byte("writerfloor.restore-incarnation.v1:" + authority))}, nil
}

// Rotate changes the independent identity before a database restore is exposed.
// An empty expected value is permitted only during trusted initial installation.
// Replaying an old expected value cannot restore the former identity.
func (r *RestoreIncarnation) Rotate(ctx context.Context, expected string) (string, error) {
	raw, err := r.journal.Load(ctx, r.key)
	if err != nil {
		return "", err
	}
	if len(raw) == 0 {
		if expected != "" {
			return "", managed.ErrConflict
		}
	} else {
		var record incarnationRecord
		if exactDecode(raw, &record) != nil || record.Authority != r.authority || !objectID(record.Current) || record.Current != expected || record.Previous != "" && !objectID(record.Previous) {
			return "", managed.ErrConflict
		}
	}
	nonce := make([]byte, 32)
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	next := incarnationRecord{Authority: r.authority, Current: hex.EncodeToString(nonce), Previous: expected}
	encoded, err := json.Marshal(next)
	if err != nil {
		return "", err
	}
	changed, err := r.journal.CompareAndSwap(ctx, r.key, raw, encoded)
	if err != nil {
		return "", err
	}
	if !changed {
		return "", managed.ErrConflict
	}
	return next.Current, nil
}

// ReadIncarnation refuses missing state and authority substitution.
func (r *RestoreIncarnation) ReadIncarnation(ctx context.Context, authority string) (string, error) {
	if authority != r.authority {
		return "", managed.ErrAuthority
	}
	raw, err := r.journal.Load(ctx, r.key)
	if err != nil {
		return "", err
	}
	var record incarnationRecord
	if exactDecode(raw, &record) != nil || record.Authority != authority || !objectID(record.Current) || record.Previous != "" && !objectID(record.Previous) {
		return "", managed.ErrUnavailable
	}
	return record.Current, nil
}
