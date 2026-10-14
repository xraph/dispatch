//go:build linux

package dockercustody

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"time"

	"github.com/xraph/ctrlplane/managed"
	provider "github.com/xraph/ctrlplane/provider/docker"
)

// Options pins the task VM and published dependency before any fixture mutation.
type Options struct {
	Socket           string
	EngineID         string
	Image            string
	Directory        string
	CtrlplaneVersion string
}

// Report preserves bounded fixture observations and cleanup outcomes.
type Report struct {
	QualificationOnly bool
	Directory         string
	Engine            any
	Image             any
	BinarySHA256      string
	CtrlplaneVersion  string
	Cases             []Case
	Cleanup           []string
	InitialInventory  inventory
	FinalInventory    inventory
	EvidenceBytes     int64
	Failure           string
}

// Case reports a retained journal state and independently counted executor sends.
type Case struct {
	Name            string
	State           string
	Sends           int
	ResourceID      string
	ReplayUnchanged bool
}
type campaign struct {
	options      Options
	directory    string
	executable   string
	observer     *observer
	report       *Report
	configs      []childConfig
	childOrdinal int
}

type boundedOutput struct{ raw []byte }

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (64 << 10) - len(b.raw)
	if remaining > len(p) {
		remaining = len(p)
	}
	if remaining > 0 {
		b.raw = append(b.raw, p[:remaining]...)
	}
	return n, nil
}

func publishedIdentity(expected string) (publishedVersion, binaryDigest string, err error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", "", errors.New("missing build identity")
	}
	version := ""
	for _, dependency := range info.Deps {
		if dependency.Replace != nil {
			return "", "", errors.New("qualification binary contains a module replacement")
		}
		if dependency.Path == "github.com/xraph/ctrlplane" {
			version = dependency.Version
		}
	}
	if expected == "" || version != expected {
		return "", "", errors.New("published Ctrlplane version does not match required pin")
	}
	path, err := os.Executable()
	if err != nil {
		return "", "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", "", err
	}
	return version, hex.EncodeToString(hash.Sum(nil)), nil
}

// Run creates at most one fixture network and container at a time. All resource
// cleanup uses recorded creation IDs. It cannot clean an unobserved Docker effect.
func Run(ctx context.Context, options Options) (report Report, err error) {
	report.QualificationOnly = true
	if callErr := privateDirectory(options.Directory); callErr != nil {
		return report, callErr
	}
	version, binaryHash, identityErr := publishedIdentity(options.CtrlplaneVersion)
	if identityErr != nil {
		return report, identityErr
	}
	report.CtrlplaneVersion = version
	report.BinarySHA256 = binaryHash
	observer, err := newObserver(options.Socket)
	if err != nil {
		return report, err
	}
	engine, err := observer.engine(ctx, options.EngineID)
	if err != nil {
		return report, err
	}
	report.Engine = engine
	image, err := observer.image(ctx, options.Image)
	if err != nil {
		return report, err
	}
	report.Image = image
	// Empty provider runtime is the explicit campaign precondition; preloaded images
	// are preserved. No unrelated container may be stopped or removed to satisfy it.
	initial, err := observer.inventory(ctx)
	if err != nil {
		return report, err
	}
	report.InitialInventory = initial
	if engine.Containers != 0 || len(initial.Containers) != 0 {
		return report, errors.New("provider engine is not empty")
	}
	directory, err := os.MkdirTemp(options.Directory, "custody-live-")
	if err != nil {
		return report, err
	}
	report.Directory = directory
	executable, err := os.Executable()
	if err != nil {
		return report, err
	}
	c := &campaign{options: options, directory: directory, executable: executable, observer: observer, report: &report}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		cleanupErr := c.cleanup(cleanupCtx)
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
		final, inventoryErr := observer.inventory(cleanupCtx)
		report.FinalInventory = final
		if inventoryErr != nil {
			err = errors.Join(err, inventoryErr)
		} else if !reflect.DeepEqual(initial, final) {
			err = errors.Join(err, errors.New("final inventory differs; no undisclosed resources were removed"))
		}
		size, sizeErr := evidenceSize(directory)
		report.EvidenceBytes = size
		if sizeErr != nil {
			err = errors.Join(err, sizeErr)
		}
		if err != nil {
			report.Failure = err.Error()
		}
		if saveErr := storeJSON(filepath.Join(directory, "report.json"), report); saveErr != nil {
			err = errors.Join(err, saveErr)
		}
	}()
	if callErr := c.lifecycle(ctx, image.ID); callErr != nil {
		return report, callErr
	}
	for _, mode := range []string{"crash-delegated", "crash-observed", "lost-response"} {
		if callErr := c.boundary(ctx, mode); callErr != nil {
			return report, callErr
		}
	}
	return report, nil
}

func (c *campaign) configure(name, mode string, r provider.ManagedDockerRequest) (childConfig, error) {
	directory := filepath.Join(c.directory, name)
	if err := os.Mkdir(directory, 0o700); err != nil {
		return childConfig{}, err
	}
	if err := os.Mkdir(filepath.Join(directory, "journal"), 0o700); err != nil {
		return childConfig{}, err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return childConfig{}, err
	}
	if err := os.WriteFile(filepath.Join(directory, "fixture.key"), key, 0o600); err != nil {
		return childConfig{}, err
	}
	config := childConfig{Directory: directory, Socket: c.options.Socket, EngineID: c.options.EngineID, Mode: mode, Request: r}
	c.configs = append(c.configs, config)
	return config, nil
}
func (c *campaign) invoke(ctx context.Context, config childConfig, expected int) error {
	c.childOrdinal++
	path := filepath.Join(config.Directory, "child-config.json")
	if err := storeJSON(path, config); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, c.executable, "--child", path) //nolint:gosec // The running executable and private generated fixture config are fixed by the campaign.
	command.Env = []string{"GOMAXPROCS=2"}
	command.WaitDelay = 5 * time.Second
	output := &boundedOutput{}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			return err
		}
		code = exit.ExitCode()
	}
	if writeErr := os.WriteFile(filepath.Join(config.Directory, "child-output-"+ordinal(c.childOrdinal)+".log"), output.raw, 0o600); writeErr != nil {
		return writeErr
	}
	if code != expected {
		return fmt.Errorf("child %s returned %d, expected %d: %s", config.Mode, code, expected, string(output.raw))
	}
	return nil
}
func readTrace(config childConfig) (trace, error) {
	var result trace
	err := loadJSON(filepath.Join(config.Directory, "trace.json"), &result)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	return result, err
}
func recordFor(ctx context.Context, config childConfig) (provider.ManagedDockerRecord, error) {
	e, transport, err := openExecutor(config)
	if err != nil {
		return provider.ManagedDockerRecord{}, err
	}
	defer transport.Close()
	return e.Read(ctx, config.Request)
}

func (c *campaign) normal(ctx context.Context, name string, r provider.ManagedDockerRequest) (string, error) {
	config, err := c.configure(name, "normal", r)
	if err != nil {
		return "", err
	}
	if callErr := c.invoke(ctx, config, 0); callErr != nil {
		return "", callErr
	}
	first, err := recordFor(ctx, config)
	if err != nil || first.State != "complete" || first.Response == nil {
		return "", fmt.Errorf("normal result incomplete: %w", err)
	}
	if callErr := storeJSON(filepath.Join(config.Directory, "record-before-replay.json"), first); callErr != nil {
		return "", callErr
	}
	before, err := readTrace(config)
	if err != nil {
		return "", err
	}
	if before.Sends != 1 {
		return "", errors.New("normal command send count mismatch")
	}
	if callErr := c.invoke(ctx, config, 0); callErr != nil {
		return "", callErr
	}
	replayed, readErr := recordFor(ctx, config)
	if readErr != nil || !reflect.DeepEqual(first, replayed) {
		return "", errors.New("reopened replay changed retained record")
	}
	after, err := readTrace(config)
	if err != nil || after != before {
		return "", errors.New("replay changed send trace")
	}
	if callErr := storeJSON(filepath.Join(config.Directory, "record-after-replay.json"), replayed); callErr != nil {
		return "", callErr
	}
	c.report.Cases = append(c.report.Cases, Case{Name: name, State: first.State, Sends: after.Sends, ResourceID: first.Response.ResourceID, ReplayUnchanged: true})
	return first.Response.ResourceID, nil
}

func (c *campaign) lifecycle(ctx context.Context, imageID string) error {
	name := filepath.Base(c.directory)
	network, err := c.normal(ctx, "network-create", requestFor(managed.StepCreateNetwork, c.options.EngineID, c.options.Image, name, "", ""))
	if err != nil {
		return err
	}
	container, err := c.normal(ctx, "container-create", requestFor(managed.StepCreateContainer, c.options.EngineID, c.options.Image, name, network, ""))
	if err != nil {
		return err
	}
	actual, err := c.observer.container(ctx, container)
	if err != nil {
		return err
	}
	if actual.ID != container || actual.Image != imageID || actual.State.Running || actual.HostConfig.NanoCpus != 100000000 || actual.HostConfig.Memory != 32<<20 || actual.HostConfig.PidsLimit == nil || *actual.HostConfig.PidsLimit != 16 || actual.HostConfig.RestartPolicy.Name != "no" || actual.HostConfig.NetworkMode != network {
		return errors.New("actual container identity/limits/restart/network configuration mismatch")
	}
	if callErr := storeJSON(filepath.Join(c.directory, "container-created-inspection.json"), actual); callErr != nil {
		return callErr
	}
	if _, err = c.normal(ctx, "container-start", requestFor(managed.StepStartContainer, c.options.EngineID, c.options.Image, name, "", container)); err != nil {
		return err
	}
	actual, err = c.observer.container(ctx, container)
	if err != nil || !actual.State.Running {
		return errors.New("helper did not remain running")
	}
	if callErr := storeJSON(filepath.Join(c.directory, "container-started-inspection.json"), actual); callErr != nil {
		return callErr
	}
	if _, err = c.normal(ctx, "container-remove", requestFor(managed.StepRemoveContainer, c.options.EngineID, c.options.Image, name, "", container)); err != nil {
		return err
	}
	if _, err = c.normal(ctx, "network-remove", requestFor(managed.StepRemoveNetwork, c.options.EngineID, c.options.Image, name, "", network)); err != nil {
		return err
	}
	return c.cleanup(ctx)
}

func (c *campaign) boundary(ctx context.Context, mode string) error {
	config, err := c.configure(mode, mode, requestFor(managed.StepCreateNetwork, c.options.EngineID, c.options.Image, filepath.Base(c.directory)+"-"+mode, "", ""))
	if err != nil {
		return err
	}
	expected := 1
	if mode == "crash-delegated" {
		expected = 81
	}
	if mode == "crash-observed" {
		expected = 82
	}
	if callErr := c.invoke(ctx, config, expected); callErr != nil {
		return callErr
	}
	first, err := recordFor(ctx, config)
	if err != nil {
		return err
	}
	before, err := readTrace(config)
	if err != nil {
		return err
	}
	if callErr := storeJSON(filepath.Join(config.Directory, "record-before-replay.json"), first); callErr != nil {
		return callErr
	}
	expectedState := "delegated"
	expectedSends := 1
	replayExit := 1
	if mode == "crash-delegated" {
		expectedSends = 0
	}
	if mode == "crash-observed" {
		expectedState = "observed"
		replayExit = 0
	}
	if first.State != expectedState || before.Sends != expectedSends {
		return fmt.Errorf("boundary %s state=%s sends=%d", mode, first.State, before.Sends)
	}
	config.Mode = "normal"
	if callErr := c.invoke(ctx, config, replayExit); callErr != nil {
		return callErr
	}
	second, err := recordFor(ctx, config)
	if err != nil {
		return err
	}
	after, err := readTrace(config)
	if err != nil || after != before {
		return errors.New("boundary replay sent again")
	}
	if mode == "crash-observed" {
		if second.State != "complete" || second.Response == nil || second.Response.ResourceID != before.ResourceID {
			return errors.New("observed response did not recover exact custody")
		}
	} else if second.State != "delegated" || second.Response != nil {
		return errors.New("uncertainty was converted to success")
	}
	if callErr := storeJSON(filepath.Join(config.Directory, "record-after-replay.json"), second); callErr != nil {
		return callErr
	}
	c.report.Cases = append(c.report.Cases, Case{Name: mode, State: second.State, Sends: after.Sends, ResourceID: before.ResourceID, ReplayUnchanged: true})
	return c.cleanup(ctx)
}
