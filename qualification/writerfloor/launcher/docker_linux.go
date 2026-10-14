//go:build linux

package launcher

import (
	"archive/tar"
	"bytes"
	"context"
	"debug/buildinfo"
	"debug/elf"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/xraph/ctrlplane/managed"
)

type dockerRuntime struct {
	api       *client.Client
	transport *http.Transport
}
type strictTransport struct{ base *http.Transport }

func (t strictTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, e := t.base.RoundTrip(r)
	if e != nil {
		return nil, e
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		_ = response.Body.Close()
		return nil, ErrUncertain
	}
	return response, nil
}
func newDocker(ctx context.Context, m Manifest) (*dockerRuntime, error) {
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "unix", "/var/run/docker.sock")
	}}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	api, e := client.New(client.WithHost("unix:///var/run/docker.sock"), client.WithAPIVersion(m.API), client.WithHTTPClient(&http.Client{Transport: strictTransport{transport}, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrRefused }}))
	if e != nil {
		return nil, e
	}
	result, e := api.Info(ctx, client.InfoOptions{})
	if e != nil {
		_ = api.Close()
		return nil, e
	}
	if result.Info.ID != m.LauncherDaemonID || result.Info.ID == m.ProviderDaemonID || result.Info.OSType != "linux" || result.Info.DefaultRuntime != "runc" {
		_ = api.Close()
		return nil, ErrRefused
	}
	hasSeccomp := false
	for _, option := range result.Info.SecurityOptions {
		if strings.Contains(option, "rootless") || strings.Contains(option, "userns") {
			_ = api.Close()
			return nil, ErrRefused
		}
		if strings.Contains(option, "seccomp") {
			hasSeccomp = true
		}
	}
	if !hasSeccomp {
		_ = api.Close()
		return nil, ErrRefused
	}
	return &dockerRuntime{api, transport}, nil
}
func (d *dockerRuntime) close() error { d.transport.CloseIdleConnections(); return d.api.Close() }
func (d *dockerRuntime) verifyImage(ctx context.Context, a Artifact) error {
	image, e := d.api.ImageInspect(ctx, a.ImageID)
	if e != nil {
		return e
	}
	if image.ID != a.ImageID || !slices.Contains(image.RepoDigests, a.RepositoryDigest) || image.Os != "linux" || image.Architecture != runtime.GOARCH || image.Config == nil || len(image.Config.Volumes) != 0 || image.Config.Healthcheck != nil || len(image.Config.Env) != 0 {
		return ErrRefused
	}
	for _, networkID := range a.Specification.Networks {
		result, e := d.api.NetworkInspect(ctx, networkID, client.NetworkInspectOptions{})
		if e != nil {
			return e
		}
		if result.Network.ID != networkID || result.Network.Driver != "bridge" || !result.Network.Internal || result.Network.Scope != "local" {
			return ErrRefused
		}
	}
	return nil
}
func fixedContainer(a Artifact, root, id string) client.ContainerCreateOptions {
	pids := int64(64)
	init := false
	config := &container.Config{Image: a.ImageID, User: "10001:10001", Entrypoint: []string{a.Specification.Executable}, Cmd: slices.Clone(a.Specification.Arguments), Env: []string{"HOME=/tmp", "WRITERFLOOR_STARTUP=/etc/writerfloor/startup.json"}, WorkingDir: "/", NetworkDisabled: a.Specification.Role == "client", Healthcheck: &container.HealthConfig{Test: []string{"NONE"}}}
	host := &container.HostConfig{ReadonlyRootfs: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}, NetworkMode: "none", IpcMode: "none", CgroupnsMode: "private", Runtime: "runc", Init: &init, ShmSize: 65536, LogConfig: container.LogConfig{Type: "none"}, Resources: container.Resources{Memory: 256 << 20, MemorySwap: 256 << 20, NanoCPUs: 500000000, PidsLimit: &pids}, Tmpfs: map[string]string{"/tmp": "rw,noexec,nosuid,nodev,size=16m,mode=1777"}, Mounts: []mount.Mount{{Type: mount.TypeBind, Source: filepath.Join(root, id+"-socket"), Target: "/run/writerfloor", ReadOnly: true, BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate}}, {Type: mount.TypeBind, Source: filepath.Join(root, id+"-startup"), Target: "/etc/writerfloor/startup.json", ReadOnly: true, BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate}}}}
	networking := &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{}}
	if len(a.Specification.Networks) > 0 {
		host.NetworkMode = container.NetworkMode(a.Specification.Networks[0])
		for _, networkID := range a.Specification.Networks {
			networking.EndpointsConfig[networkID] = &network.EndpointSettings{NetworkID: networkID}
		}
	}
	return client.ContainerCreateOptions{Config: config, HostConfig: host, NetworkingConfig: networking}
}
func (d *dockerRuntime) create(ctx context.Context, a Artifact, root, id string) (string, error) {
	result, e := d.api.ContainerCreate(ctx, fixedContainer(a, root, id))
	if e != nil {
		return "", e
	}
	if !hexadecimal(result.ID) || len(result.Warnings) != 0 {
		return result.ID, ErrRefused
	}
	return result.ID, nil
}
func verifyStaticExecutable(raw []byte, want string) error {
	if len(raw) == 0 || len(raw) > MaxExecutable || managed.Digest(raw) != want {
		return ErrRefused
	}
	file, e := elf.NewFile(bytes.NewReader(raw))
	if e != nil {
		return e
	}
	defer file.Close()
	if file.Type != elf.ET_EXEC && file.Type != elf.ET_DYN {
		return ErrRefused
	}
	if runtime.GOARCH == "amd64" && file.Machine != elf.EM_X86_64 || runtime.GOARCH == "arm64" && file.Machine != elf.EM_AARCH64 {
		return ErrRefused
	}
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		return ErrRefused
	}
	for _, program := range file.Progs {
		if program.Type == elf.PT_INTERP {
			return ErrRefused
		}
	}
	imports, e := file.ImportedLibraries()
	if e != nil || len(imports) != 0 {
		return ErrRefused
	}
	return nil
}
func (d *dockerRuntime) verifyExecutable(ctx context.Context, id string, a Artifact) error {
	result, e := d.api.CopyFromContainer(ctx, id, client.CopyFromContainerOptions{SourcePath: a.Specification.Executable})
	if e != nil {
		return e
	}
	defer result.Content.Close()
	if !result.Stat.Mode.IsRegular() || result.Stat.Mode.Perm()&0o111 == 0 || result.Stat.Size <= 0 || result.Stat.Size > MaxExecutable || result.Stat.LinkTarget != "" {
		return ErrRefused
	}
	reader := tar.NewReader(io.LimitReader(result.Content, MaxExecutable+65536))
	header, e := reader.Next()
	if e != nil || header.Typeflag != tar.TypeReg || header.Size != result.Stat.Size || header.Name != filepath.Base(a.Specification.Executable) || header.Linkname != "" || header.Mode&0o6000 != 0 {
		return ErrRefused
	}
	raw, e := io.ReadAll(io.LimitReader(reader, MaxExecutable+1))
	if e != nil {
		return e
	}
	if operationErr := verifyStaticExecutable(raw, a.ExecutableDigest); operationErr != nil {
		return operationErr
	}
	info, e := buildinfo.Read(bytes.NewReader(raw))
	if e != nil || managed.Digest([]byte(info.String())) != a.ModulesDigest {
		return ErrRefused
	}
	if _, e = reader.Next(); !errors.Is(e, io.EOF) {
		return ErrRefused
	}
	return nil
}
func (d *dockerRuntime) verifyStopped(ctx context.Context, id string, a Artifact, root, launch string) error {
	inspected, e := d.api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if e != nil {
		return e
	}
	got := inspected.Container
	want := fixedContainer(a, root, launch)
	if got.ID != id || got.Image != a.ImageID || got.State == nil || got.State.Running || got.State.Paused || got.State.Restarting || got.Config == nil || got.HostConfig == nil || len(got.ExecIDs) != 0 {
		return ErrRefused
	}
	c, h := got.Config, got.HostConfig
	w := want.HostConfig
	if c.User != want.Config.User || !slices.Equal(c.Entrypoint, want.Config.Entrypoint) || !slices.Equal(c.Cmd, want.Config.Cmd) || !slices.Equal(c.Env, want.Config.Env) || c.WorkingDir != "/" || c.NetworkDisabled != want.Config.NetworkDisabled || len(c.Volumes) != 0 || c.Healthcheck == nil || !slices.Equal(c.Healthcheck.Test, []string{"NONE"}) || len(c.ExposedPorts) != 0 {
		return ErrRefused
	}
	if !h.ReadonlyRootfs || h.Privileged || h.AutoRemove || h.PublishAllPorts || len(h.CapAdd) != 0 || !slices.Equal(h.CapDrop, w.CapDrop) || !slices.Equal(h.SecurityOpt, w.SecurityOpt) || len(h.Binds) != 0 || len(h.VolumesFrom) != 0 || len(h.Devices) != 0 || len(h.DeviceRequests) != 0 || len(h.GroupAdd) != 0 || len(h.ExtraHosts) != 0 || len(h.Links) != 0 || len(h.PortBindings) != 0 || h.PidMode != "" || h.UTSMode != "" || h.UsernsMode != "" || h.IpcMode != w.IpcMode || h.CgroupnsMode != w.CgroupnsMode || h.NetworkMode != w.NetworkMode || h.Runtime != "runc" || h.Init == nil || *h.Init || h.RestartPolicy.Name != container.RestartPolicyDisabled {
		return ErrRefused
	}
	if h.Memory != w.Memory || h.MemorySwap != w.MemorySwap || h.NanoCPUs != w.NanoCPUs || h.PidsLimit == nil || *h.PidsLimit != *w.PidsLimit || !reflect.DeepEqual(h.Tmpfs, w.Tmpfs) || !reflect.DeepEqual(h.Mounts, w.Mounts) {
		return ErrRefused
	}
	if got.NetworkSettings == nil {
		return ErrRefused
	}
	if a.Specification.Role == "client" {
		if len(got.NetworkSettings.Networks) != 0 && (len(got.NetworkSettings.Networks) != 1 || got.NetworkSettings.Networks["none"] == nil) {
			return ErrRefused
		}
	} else {
		if len(got.NetworkSettings.Networks) != len(a.Specification.Networks) {
			return ErrRefused
		}
		for _, endpoint := range got.NetworkSettings.Networks {
			if endpoint == nil || !slices.Contains(a.Specification.Networks, endpoint.NetworkID) {
				return ErrRefused
			}
		}
	}
	return nil
}
func (d *dockerRuntime) start(ctx context.Context, id string) error {
	if !hexadecimal(id) {
		return ErrRefused
	}
	_, e := d.api.ContainerStart(ctx, id, client.ContainerStartOptions{})
	return e
}
func (d *dockerRuntime) remove(ctx context.Context, id string) error {
	if !hexadecimal(id) {
		return ErrRefused
	}
	timeout := 2
	result, e := d.api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if e != nil {
		return e
	}
	if result.Container.ID != id || result.Container.State == nil {
		return ErrRefused
	}
	if result.Container.State.Running {
		if _, e = d.api.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: &timeout}); e != nil {
			return e
		}
		result, e = d.api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if e != nil {
			return e
		}
		if result.Container.ID != id || result.Container.State == nil || result.Container.State.Running {
			return ErrRefused
		}
	}
	_, e = d.api.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: false, RemoveVolumes: false})
	return e
}
