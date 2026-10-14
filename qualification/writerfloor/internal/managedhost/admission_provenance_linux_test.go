//go:build linux

package managedhost

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/mod/semver"

	"github.com/xraph/ctrlplane/managed"
)

const liveProvenanceLimit = 1 << 20
const liveProvenanceSchema = "writerfloor.test-build-provenance.v1"

type liveBuildProvenance struct {
	Schema     string            `json:"schema"`
	Executable string            `json:"executable_sha256"`
	Modules    []json.RawMessage `json:"modules"`
}

type liveBuildModule struct {
	Path    string
	Version string
	Sum     string
	Main    bool
	Replace json.RawMessage
	Error   json.RawMessage
}

// The fallback is private qualification evidence, never a production binary
// identity substitute. The trusted build runner records its real published graph
// and binds that graph to the exact binary transferred into this isolated host.
func liveAdmissionModules(info *debug.BuildInfo, executableDigest, provenancePath string) (string, error) {
	if info == nil || !objectID(executableDigest) {
		return "", managed.ErrAuthority
	}
	if len(info.Deps) > 0 {
		if len(info.Deps) > 1024 || info.Main.Replace != nil {
			return "", managed.ErrAuthority
		}
		modules := make([]json.RawMessage, 0, len(info.Deps))
		for _, module := range info.Deps {
			if module == nil || module.Replace != nil {
				return "", managed.ErrAuthority
			}
			raw, err := json.Marshal(module)
			if err != nil {
				return "", err
			}
			modules = append(modules, raw)
		}
		if err := validateLiveBuildModules(modules, false); err != nil {
			return "", err
		}
		raw, err := json.Marshal(info)
		if err != nil {
			return "", err
		}
		return managed.Digest(raw), nil
	}
	if !testing.Testing() || provenancePath == "" {
		return "", managed.ErrAuthority
	}
	raw, err := readLiveBuildProvenance(provenancePath)
	if err != nil {
		return "", err
	}
	return validateLiveBuildProvenance(raw, executableDigest)
}

func readLiveBuildProvenance(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, managed.ErrAuthority
	}
	// Every path component is inspected; the final open refuses a substituted link.
	// A root-owned sticky /tmp ancestor is permitted, but the immediate directory
	// must be private root custody and may not be replaced by another principal.
	for current := filepath.Dir(path); ; current = filepath.Dir(current) {
		stat, err := os.Lstat(current)
		if err != nil {
			return nil, err
		}
		owner, ok := stat.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != 0 || !stat.IsDir() || stat.Mode()&os.ModeSymlink != 0 {
			return nil, managed.ErrAuthority
		}
		if current == filepath.Dir(path) {
			if stat.Mode().Perm() != 0700 {
				return nil, managed.ErrAuthority
			}
		} else if stat.Mode().Perm()&0022 != 0 && stat.Mode()&os.ModeSticky == 0 {
			return nil, managed.ErrAuthority
		}
		if current == "/" {
			break
		}
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	owner, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || !stat.Mode().IsRegular() || stat.Mode().Perm() != 0600 || stat.Size() <= 0 || stat.Size() > liveProvenanceLimit {
		return nil, managed.ErrAuthority
	}
	raw, err := io.ReadAll(io.LimitReader(file, liveProvenanceLimit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > liveProvenanceLimit {
		return nil, managed.ErrCapacity
	}
	return raw, nil
}

func validateLiveBuildProvenance(raw []byte, executableDigest string) (string, error) {
	if len(raw) == 0 || len(raw) > liveProvenanceLimit || !objectID(executableDigest) {
		return "", managed.ErrAuthority
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueLiveJSON(decoder); err != nil {
		return "", err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return "", managed.ErrAuthority
	}
	var record liveBuildProvenance
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return "", err
	}
	if record.Schema != liveProvenanceSchema || record.Executable != executableDigest {
		return "", managed.ErrAuthority
	}
	if err := validateLiveBuildModules(record.Modules, true); err != nil {
		return "", err
	}
	var graph any
	graphRaw, err := json.Marshal(record.Modules)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(graphRaw, &graph); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(graph)
	if err != nil {
		return "", err
	}
	return managed.Digest(canonical), nil
}

func validateLiveBuildModules(modules []json.RawMessage, requireMain bool) error {
	if len(modules) == 0 || len(modules) > 1024 {
		return managed.ErrAuthority
	}
	foundCore, foundDispatch, mainCount := false, false, 0
	seen := map[string]bool{}
	for _, raw := range modules {
		var module liveBuildModule
		if json.Unmarshal(raw, &module) != nil || module.Path == "" || seen[module.Path] || len(module.Replace) != 0 && !bytes.Equal(module.Replace, []byte("null")) || len(module.Error) != 0 && !bytes.Equal(module.Error, []byte("null")) {
			return managed.ErrAuthority
		}
		seen[module.Path] = true
		if module.Main {
			if !requireMain || module.Path != "github.com/xraph/dispatch/qualification/writerfloor" {
				return managed.ErrAuthority
			}
			mainCount++
		}
		if module.Path == "github.com/xraph/ctrlplane" || module.Path == "github.com/xraph/dispatch" {
			sum, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(module.Sum, "h1:"))
			if module.Main || !semver.IsValid(module.Version) || !strings.HasPrefix(module.Sum, "h1:") || err != nil || len(sum) != 32 || base64.StdEncoding.EncodeToString(sum) != strings.TrimPrefix(module.Sum, "h1:") {
				return managed.ErrAuthority
			}
			foundCore = foundCore || module.Path == "github.com/xraph/ctrlplane"
			foundDispatch = foundDispatch || module.Path == "github.com/xraph/dispatch"
		}
	}
	if !foundCore || !foundDispatch || requireMain && mainCount != 1 {
		return managed.ErrAuthority
	}
	return nil
}

// Duplicate keys are refused throughout the exact go-list graph, including
// unknown metadata fields. An earlier Replace cannot be hidden by a later key.
func uniqueLiveJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, readErr := decoder.Token()
			if readErr != nil {
				return readErr
			}
			name, valid := key.(string)
			if !valid || seen[strings.ToLower(name)] {
				return managed.ErrAuthority
			}
			seen[strings.ToLower(name)] = true
			if err = uniqueLiveJSON(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err = uniqueLiveJSON(decoder); err != nil {
				return err
			}
		}
	default:
		return managed.ErrAuthority
	}
	_, err = decoder.Token()
	return err
}

func TestLiveBuildProvenanceAuthenticatesExactBinaryAndPublishedGraph(t *testing.T) {
	digest := strings.Repeat("a", 64)
	sum := "h1:" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	modules := []map[string]any{{"Path": "github.com/xraph/dispatch/qualification/writerfloor", "Main": true}, {"Path": "github.com/xraph/ctrlplane", "Version": "v1.7.1-0.20261015000000-0123456789ab", "Sum": sum}, {"Path": "github.com/xraph/dispatch", "Version": "v1.2.3", "Sum": sum}}
	encode := func(values []map[string]any) []byte {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"schema": liveProvenanceSchema, "executable_sha256": digest, "modules": values})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	baseline := encode(modules)
	if hash, err := validateLiveBuildProvenance(baseline, digest); err != nil || !objectID(hash) {
		t.Fatal("valid measured graph refused", err)
	}
	for _, name := range []string{"binary-mismatch", "replace", "missing-sum", "unpublished-version", "duplicate-module", "missing-main", "wrong-main", "missing-core", "module-error", "duplicate-json-key", "oversize", "trailing-json"} {
		t.Run(name, func(t *testing.T) {
			var changed []map[string]any
			raw, err := json.Marshal(modules)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			expected := digest
			switch name {
			case "binary-mismatch":
				expected = strings.Repeat("b", 64)
			case "replace":
				changed[1]["Replace"] = map[string]any{"Path": "/tmp/local"}
			case "missing-sum":
				delete(changed[1], "Sum")
			case "unpublished-version":
				changed[1]["Version"] = "(devel)"
			case "duplicate-module":
				changed = append(changed, changed[1])
			case "missing-main":
				changed = changed[1:]
			case "wrong-main":
				changed[0]["Path"] = "other"
			case "missing-core":
				changed = append(changed[:1], changed[2:]...)
			case "module-error":
				changed[1]["Error"] = map[string]string{"Err": "unavailable"}
			}
			raw = encode(changed)
			switch name {
			case "duplicate-json-key":
				raw = bytes.Replace(raw, []byte(`"modules":`), []byte(`"modules":[],"modules":`), 1)
			case "oversize":
				raw = append(raw, bytes.Repeat([]byte(" "), liveProvenanceLimit)...)
			case "trailing-json":
				raw = append(raw, []byte(`{}`)...)
			}
			if _, err = validateLiveBuildProvenance(raw, expected); err == nil {
				t.Fatal("invalid provenance accepted")
			}
		})
	}
}

func TestLiveBuildProvenanceFileCustodyRefuses(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "provenance.json")
	if err := os.WriteFile(path, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readLiveBuildProvenance(path); err == nil {
		t.Fatal("public provenance permissions accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readLiveBuildProvenance(link); err == nil {
		t.Fatal("symlink provenance accepted")
	}
	if _, err := readLiveBuildProvenance(directory); err == nil {
		t.Fatal("nonregular provenance accepted")
	}
	if os.Geteuid() == 0 {
		if err := os.Chown(path, 65534, 65534); err != nil {
			t.Fatal(err)
		}
		if _, err := readLiveBuildProvenance(path); err == nil {
			t.Fatal("untrusted file owner accepted")
		}
	}
}

func TestLiveBuildProvenanceDoesNotOverrideEmbeddedDependencies(t *testing.T) {
	sum := "h1:" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	info := &debug.BuildInfo{Deps: []*debug.Module{{Path: "github.com/xraph/ctrlplane", Version: "v1.7.1", Sum: sum}, {Path: "github.com/xraph/dispatch", Version: "v1.2.3", Sum: sum}}}
	if _, err := liveAdmissionModules(info, strings.Repeat("a", 64), "/missing/provenance.json"); err != nil {
		t.Fatal("embedded dependencies did not take precedence", err)
	}
	info.Deps[0].Replace = &debug.Module{Path: "/tmp/local"}
	if _, err := liveAdmissionModules(info, strings.Repeat("a", 64), "/missing/provenance.json"); err == nil {
		t.Fatal("replacement fell back to external provenance")
	}
}
