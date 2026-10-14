//go:build linux

// Package dockercustody contains test-only live qualification, not host enrollment.
package dockercustody

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const apiVersion = "1.56"
const launchEngineID = "0f344610-72ca-491a-b99d-4b9fd57d2a40"

type observer struct {
	client *http.Client
	socket string
}
type engineInfo struct {
	ID              string
	ServerVersion   string
	SecurityOptions []string
	Containers      int
	Images          int
}
type imageInfo struct {
	ID           string
	RepoDigests  []string
	Size         int64
	Architecture string
	Config       struct{ Volumes map[string]json.RawMessage }
}
type containerInfo struct {
	ID    string
	Image string
	State struct {
		Running   bool
		Pid       int
		StartedAt string
	}
	HostConfig struct {
		NanoCpus      int64
		Memory        int64
		PidsLimit     *int64
		RestartPolicy struct{ Name string }
		NetworkMode   string
	}
}

func newObserver(socket string) (*observer, error) {
	if os.Geteuid() != 1000 || socket != "/run/user/1000/docker.sock" {
		return nil, errors.New("qualification requires the dedicated provider UID and exact Unix socket")
	}
	stat, err := os.Lstat(socket)
	if err != nil {
		return nil, err
	}
	owner, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 1000 || stat.Mode()&os.ModeSocket == 0 || stat.Mode().Perm() != 0o700 {
		return nil, errors.New("provider socket ownership or mode mismatch")
	}
	for _, path := range []string{"/var/run/docker.sock", "/run/containerd/containerd.sock"} {
		conn, connectErr := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "unix", path)
		if conn != nil {
			_ = conn.Close()
		}
		if !errors.Is(connectErr, os.ErrPermission) {
			return nil, fmt.Errorf("expected permission denial on %s: %w", path, connectErr)
		}
	}
	if file, openErr := os.Open("/home/rexraphael.guest"); !errors.Is(openErr, os.ErrPermission) {
		if file != nil {
			_ = file.Close()
		}
		return nil, errors.New("launcher home denial was not proven")
	}
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	return &observer{socket: socket, client: &http.Client{Transport: transport, Timeout: 15e9, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}}, nil
}

func (o *observer) get(ctx context.Context, path string, out any) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://provider/v"+apiVersion+path, http.NoBody)
	if err != nil {
		return 0, err
	}
	response, err := o.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, fmt.Errorf("inspection status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	if err != nil {
		return response.StatusCode, err
	}
	if len(raw) > 1<<20 {
		return response.StatusCode, errors.New("inspection bound exceeded")
	}
	return response.StatusCode, json.Unmarshal(raw, out)
}

func (o *observer) engine(ctx context.Context, expected string) (engineInfo, error) {
	var result engineInfo
	_, err := o.get(ctx, "/info", &result)
	if err != nil {
		return result, err
	}
	rootless := false
	for _, option := range result.SecurityOptions {
		if option == "name=rootless" {
			rootless = true
		}
	}
	if !rootless || expected != "4452e8d6-a916-4fdc-b62a-49f9b3bec30a" || expected == launchEngineID || result.ID != expected || result.ServerVersion != "29.9.0" {
		return result, errors.New("provider engine identity/version/rootless mismatch")
	}
	return result, nil
}

func (o *observer) image(ctx context.Context, ref string) (imageInfo, error) {
	var result imageInfo
	parts := strings.Split(ref, "@sha256:")
	if len(parts) != 2 || (parts[0] != "alpine" && parts[0] != "docker.io/library/alpine") || !hexID(parts[1]) {
		return result, errors.New("a real preloaded digest-qualified Alpine image is required")
	}
	_, err := o.get(ctx, "/images/"+url.PathEscape(ref)+"/json", &result)
	if err != nil {
		return result, err
	}
	exact := false
	for _, digest := range result.RepoDigests {
		if digest == ref {
			exact = true
		}
	}
	if !exact || result.Architecture != "arm64" || result.Size <= 0 || result.Size > 16<<20 || len(result.Config.Volumes) != 0 {
		return result, errors.New("preloaded image digest/architecture/size mismatch")
	}
	return result, nil
}

func (o *observer) container(ctx context.Context, id string) (containerInfo, error) {
	var result containerInfo
	if !hexID(id) {
		return result, errors.New("invalid container ID")
	}
	_, err := o.get(ctx, "/containers/"+id+"/json", &result)
	return result, err
}
func hexID(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
func privateDirectory(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || resolved != absolute {
		return errors.New("noncanonical private directory")
	}
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	owner, ok := s.Sys().(*syscall.Stat_t)
	if !ok || !s.IsDir() || s.Mode().Perm() != 0o700 || int(owner.Uid) != os.Geteuid() {
		return errors.New("directory must belong to provider and have mode0700")
	}
	return nil
}
func ordinal(n int) string { return strconv.Itoa(n) }

// inventory is read-only and detects any unrecorded effect or unrelated change.
// Such a mismatch is reported; it never authorizes removal by name or discovery.
type inventory struct {
	Containers []string
	Networks   []string
	Images     []string
}

func (o *observer) inventory(ctx context.Context) (inventory, error) {
	var result inventory
	for _, entry := range []struct {
		path string
		ids  *[]string
	}{{"/containers/json?all=true", &result.Containers}, {"/networks", &result.Networks}, {"/images/json?all=true", &result.Images}} {
		var rows []struct{ ID string }
		if _, err := o.get(ctx, entry.path, &rows); err != nil {
			return result, err
		}
		if len(rows) > 64 {
			return result, errors.New("fixture inventory bound exceeded")
		}
		*entry.ids = make([]string, 0, len(rows))
		for _, row := range rows {
			if row.ID == "" {
				return result, errors.New("inventory has missing ID")
			}
			*entry.ids = append(*entry.ids, row.ID)
		}
		slices.Sort(*entry.ids)
	}
	return result, nil
}
