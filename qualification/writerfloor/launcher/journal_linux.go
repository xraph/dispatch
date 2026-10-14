//go:build linux

package launcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/xraph/ctrlplane/managed"
)

// Record contains references only; private keys and bearer secrets are separate files.
type Record struct {
	ID              string `json:"id"`
	Manifest        string `json:"manifest"`
	Certificate     string `json:"certificate"`
	Phase           string `json:"phase"`
	Container       string `json:"container"`
	Account         string `json:"account"`
	Key             string `json:"key"`
	Peer            string `json:"peer"`
	Enrollment      string `json:"enrollment"`
	Readiness       string `json:"readiness"`
	BindOperation   string `json:"bind_operation"`
	RevokeOperation string `json:"revoke_operation"`
	RevokeRevision  uint64 `json:"revoke_revision"`
}

type journal struct {
	root string
	lock *os.File
}

func secureDirectory(root string) error {
	if os.Geteuid() != 0 || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return ErrRefused
	}
	resolved, e := filepath.EvalSymlinks(root)
	if e != nil || resolved != root {
		return ErrRefused
	}
	info, e := os.Lstat(root)
	if e != nil {
		return e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return ErrRefused
	}
	return nil
}
func openJournal(root, id string) (*journal, error) {
	if !token(id) || len(id) > 32 || len(filepath.Join(root, id+"-control.sock")) > 100 {
		return nil, ErrRefused
	}
	return openLock(root, id+".lock")
}
func openLock(root, name string) (*journal, error) {
	if operationErr := secureDirectory(root); operationErr != nil {
		return nil, operationErr
	}
	fd, e := syscall.Open(filepath.Join(root, name), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), "launcher.lock")
	info, e := f.Stat()
	if e != nil {
		_ = f.Close()
		return nil, e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = f.Close()
		return nil, ErrRefused
	}
	if e = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		_ = f.Close()
		return nil, e
	}
	return &journal{root, f}, nil
}
func (j *journal) close() error { return j.lock.Close() }
func readPrivate(path string, limit int) ([]byte, error) {
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() < 1 || info.Size() > int64(limit) {
		return nil, ErrRefused
	}
	b := make([]byte, info.Size())
	if _, e = f.ReadAt(b, 0); e != nil {
		return nil, e
	}
	return b, nil
}
func syncDir(root string) error {
	f, e := os.Open(root)
	if e != nil {
		return e
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(syncErr, closeErr)
}
func writePrivate(root, name string, data []byte, exclusive bool) error {
	if len(data) == 0 || len(data) > MaxManifest {
		return ErrRefused
	}
	target := filepath.Join(root, name)
	if exclusive {
		f, e := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if e != nil {
			return e
		}
		_, e = f.Write(data)
		e = errors.Join(e, f.Sync(), f.Close())
		if e != nil {
			return e
		}
		return syncDir(root)
	}
	// Only existing private, canonical records may be replaced under the held lock.
	if _, e := readPrivate(target, MaxRecord); e != nil {
		return e
	}
	f, e := os.CreateTemp(root, ".record-")
	if e != nil {
		return e
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, e = f.Write(data); e != nil {
		_ = f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		_ = f.Close()
		return e
	}
	if operationErr := f.Close(); operationErr != nil {
		return operationErr
	}
	if operationErr := os.Rename(temp, target); operationErr != nil {
		return operationErr
	}
	return syncDir(root)
}
func (j *journal) read(id string) (Record, error) {
	if !token(id) {
		return Record{}, ErrRefused
	}
	b, e := readPrivate(filepath.Join(j.root, id+".json"), MaxRecord)
	if e != nil {
		return Record{}, e
	}
	var r Record
	if canonical(b, &r, MaxRecord) != nil || r.ID != id || !hexadecimal(r.Manifest) || !token(r.Certificate) {
		return Record{}, ErrRefused
	}
	switch r.Phase {
	case "prepared", "enrolling", "account", "key", "creating", "created", "verified", "binding", "bound", "policy", "enrolled", "starting", "running", "stopping", "removed", "stopped":
	default:
		return Record{}, ErrRefused
	}
	if r.Container != "" && !hexadecimal(r.Container) {
		return Record{}, ErrRefused
	}
	return r, nil
}
func (j *journal) create(id string, m Manifest, a Artifact) (Record, error) {
	if !token(id) {
		return Record{}, ErrRefused
	}
	capacity, e := openLock(j.root, ".capacity.lock")
	if e != nil {
		return Record{}, e
	}
	defer capacity.close()
	entries, e := os.ReadDir(j.root)
	if e != nil {
		return Record{}, e
	}
	count := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".json" {
			count++
		}
	}
	if count >= MaxLaunches {
		return Record{}, ErrRefused
	}
	b, e := json.Marshal(m)
	if e != nil {
		return Record{}, e
	}
	r := Record{ID: id, Manifest: managed.Digest(b), Certificate: a.Certificate, Phase: "prepared"}
	data, e := json.Marshal(r)
	if e != nil {
		return Record{}, e
	}
	return r, writePrivate(j.root, id+".json", data, true)
}
func (j *journal) save(r Record, phase string) error {
	r.Phase = phase
	b, e := json.Marshal(r)
	if e != nil || len(b) > MaxRecord {
		return ErrRefused
	}
	return writePrivate(j.root, r.ID+".json", b, false)
}

func verifyStartup(path string, expected []byte) error {
	if len(expected) == 0 || len(expected) > MaxRecord {
		return ErrRefused
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 10001 || stat.Gid != 10001 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 || info.Size() != int64(len(expected)) {
		return ErrRefused
	}
	actual := make([]byte, len(expected))
	if _, err = file.ReadAt(actual, 0); err != nil {
		return err
	}
	if !bytes.Equal(actual, expected) {
		return ErrRefused
	}
	return nil
}
