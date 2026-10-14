//go:build linux

package dockercustody

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"

	"github.com/xraph/ctrlplane/managed"
	provider "github.com/xraph/ctrlplane/provider/docker"
)

// cleanup is fixture maintenance, not a recovery result. An absent exact ID is
// enough to skip maintenance; it never repairs a delegated or uncertain journal.
func (c *campaign) cleanup(ctx context.Context) error {
	if _, err := c.observer.engine(ctx, c.options.EngineID); err != nil {
		return err
	}

	transport, err := provider.NewSDKManagedDockerTransport("unix://"+c.options.Socket, apiVersion, &http.Transport{}, &fixtureStartup{})
	if err != nil {
		return err
	}
	defer transport.Close()

	var failures []error
	for _, config := range slices.Backward(c.configs) {
		kind := config.Request.Command.Kind
		if kind != managed.StepCreateNetwork && kind != managed.StepCreateContainer {
			continue
		}

		observed, readErr := readTrace(config)
		if readErr != nil {
			failures = append(failures, readErr)
			continue
		}
		if observed.ResourceID == "" {
			// The final inventory detects an effect whose ID was never observed.
			continue
		}
		if observed.Kind != kind || observed.Sends != 1 || !hexID(observed.ResourceID) {
			failures = append(failures, errors.New("invalid cleanup ledger"))
			continue
		}

		path := "/networks/" + observed.ResourceID
		removeKind := managed.StepRemoveNetwork
		if kind == managed.StepCreateContainer {
			path = "/containers/" + observed.ResourceID + "/json"
			removeKind = managed.StepRemoveContainer
		}

		var inspected struct{ ID string }
		status, inspectErr := c.observer.get(ctx, path, &inspected)
		if status == http.StatusNotFound {
			continue
		}
		if inspectErr != nil || inspected.ID != observed.ResourceID {
			failures = append(failures, fmt.Errorf("cleanup inspection refused %s: %w", observed.ResourceID, inspectErr))
			continue
		}

		id, removeErr := transport.Execute(ctx, provider.ManagedDockerCommand{Kind: removeKind, ResourceID: observed.ResourceID})
		if removeErr != nil || id != observed.ResourceID {
			failures = append(failures, fmt.Errorf("cleanup exact-ID removal failed %s: %w", observed.ResourceID, removeErr))
			continue
		}

		c.report.Cleanup = append(c.report.Cleanup, string(removeKind)+":"+observed.ResourceID)
		status, inspectErr = c.observer.get(ctx, path, &inspected)
		if status != http.StatusNotFound {
			failures = append(failures, fmt.Errorf("cleanup absence not observed %s: %w", observed.ResourceID, inspectErr))
		}
	}

	return errors.Join(failures...)
}

func evidenceSize(directory string) (int64, error) {
	var total int64
	err := filepath.WalkDir(directory, func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return errors.New("unexpected evidence symlink")
		}
		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("unexpected evidence file type")
		}

		total += info.Size()
		if total > 64<<20 {
			return errors.New("qualification evidence exceeds 64 MiB budget")
		}

		return nil
	})

	return total, err
}
