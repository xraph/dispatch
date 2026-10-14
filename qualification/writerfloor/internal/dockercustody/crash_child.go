//go:build linux

package dockercustody

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/xraph/ctrlplane/managed"
	provider "github.com/xraph/ctrlplane/provider/docker"
)

type childConfig struct {
	Directory string
	Socket    string
	EngineID  string
	Mode      string
	Request   provider.ManagedDockerRequest
}
type trace struct {
	Sends      int
	ResourceID string
	Kind       managed.ProviderStepKind
}

func storeJSON(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return errors.New("fixture evidence bound exceeded")
	}
	file, err := os.CreateTemp(filepath.Dir(path), "pending-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	defer file.Close()
	if _, err = file.Write(raw); err != nil {
		return err
	}
	if callErr := file.Sync(); callErr != nil {
		return callErr
	}
	if callErr := file.Close(); callErr != nil {
		return callErr
	}
	if callErr := os.Rename(name, path); callErr != nil {
		return callErr
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
func loadJSON(path string, value any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return errors.New("fixture evidence bound exceeded")
	}
	return json.Unmarshal(raw, value)
}

type crashJournal struct {
	provider.ManagedDockerJournal
	mode string
}

func (j *crashJournal) CompareAndSwap(ctx context.Context, key string, old, next []byte) (bool, error) {
	ok, err := j.ManagedDockerJournal.CompareAndSwap(ctx, key, old, next)
	if err != nil || !ok {
		return ok, err
	}
	var record provider.ManagedDockerRecord
	if callErr := json.Unmarshal(next, &record); callErr != nil {
		return false, callErr
	}
	// A real child process exits after durable CAS acknowledgement, before the next
	// executor instruction. Restart/replay occurs in a different child process.
	if j.mode == "crash-delegated" && record.State == "delegated" {
		os.Exit(81)
	}
	if j.mode == "crash-observed" && record.State == "observed" {
		os.Exit(82)
	}
	return ok, nil
}

type tracedTransport struct {
	base   *provider.SDKManagedDockerTransport
	config childConfig
}

func (t *tracedTransport) Execute(ctx context.Context, command provider.ManagedDockerCommand) (string, error) {
	path := filepath.Join(t.config.Directory, "trace.json")
	var current trace
	if err := loadJSON(path, &current); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	current.Sends++
	current.Kind = command.Kind
	if err := storeJSON(path, current); err != nil {
		return "", err
	}
	id, err := t.base.Execute(ctx, command)
	if err != nil {
		return "", err
	}
	current.ResourceID = id
	// This side channel is exclusively an exact-ID cleanup ledger. It never repairs
	// the executor's uncertain journal or authorizes replay after lost response.
	if callErr := storeJSON(path, current); callErr != nil {
		return "", fmt.Errorf("possible unrecorded fixture resource %s: %w", id, callErr)
	}
	if t.config.Mode == "lost-response" {
		return "", errors.New("qualification intentionally dropped a successful Docker response")
	}
	return id, nil
}

func openExecutor(config childConfig) (*provider.ManagedDockerExecutor, *provider.SDKManagedDockerTransport, error) {
	if config.EngineID != "4452e8d6-a916-4fdc-b62a-49f9b3bec30a" || config.Request.BackendAuthority != config.EngineID {
		return nil, nil, errors.New("unexpected task provider authority")
	}
	if err := privateDirectory(config.Directory); err != nil {
		return nil, nil, err
	}
	observer, err := newObserver(config.Socket)
	if err != nil {
		return nil, nil, err
	}
	key, err := os.ReadFile(filepath.Join(config.Directory, "fixture.key"))
	if err != nil {
		return nil, nil, err
	}
	if len(key) != 32 {
		return nil, nil, errors.New("fixture key length mismatch")
	}
	authority := &fixtureAuthority{request: config.Request, key: key, observer: observer}
	journal, err := provider.OpenFileManagedDockerJournal(filepath.Join(config.Directory, "journal"))
	if err != nil {
		return nil, nil, err
	}
	transport, err := provider.NewSDKManagedDockerTransport("unix://"+config.Socket, apiVersion, &http.Transport{}, &fixtureStartup{})
	if err != nil {
		return nil, nil, err
	}
	executor, err := provider.NewManagedDockerExecutor(&crashJournal{ManagedDockerJournal: journal, mode: config.Mode}, &tracedTransport{base: transport, config: config}, authority, authority, config.EngineID)
	if err != nil {
		_ = transport.Close()
		return nil, nil, err
	}
	return executor, transport, nil
}

// Child runs one isolated executor invocation. Only the campaign creates its config.
func Child(ctx context.Context, path string) error {
	var config childConfig
	if err := loadJSON(path, &config); err != nil {
		return err
	}
	switch config.Mode {
	case "normal", "crash-delegated", "crash-observed", "lost-response":
	default:
		return errors.New("unsupported child mode")
	}
	executor, transport, err := openExecutor(config)
	if err != nil {
		return err
	}
	defer transport.Close()
	result, executeErr := executor.Execute(ctx, config.Request)
	if callErr := storeJSON(filepath.Join(config.Directory, "child-result.json"), result); callErr != nil {
		return callErr
	}
	return executeErr
}
