// Package launcher implements the fixed isolated Linux qualification launch boundary.
package launcher

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strings"

	"github.com/xraph/ctrlplane/managed"
)

// MaxManifest bounds canonical trusted manifest bytes.
const MaxManifest = 32768

// MaxLaunches bounds retained launch records per isolated campaign.
const MaxLaunches = 16

// MaxRecord bounds one local machine or transition record.
const MaxRecord = 8192

// MaxExecutable bounds the stopped-container executable bytes.
const MaxExecutable = 64 << 20

// ErrRefused reports a failed fixed-topology check.
var ErrRefused = errors.New("writerfloor launcher: fixed launch refused")

// ErrUncertain requires retained inspection without a new send.
var ErrUncertain = errors.New("writerfloor launcher: retained uncertainty requires inspection")

// Specification is a reviewed immutable runtime configuration. There is no request override.
// Networks are actual Docker IDs from trusted isolated-host setup, never names or labels.
type Specification struct {
	Role                string   `json:"role"`
	Executable          string   `json:"executable"`
	Arguments           []string `json:"arguments"`
	Networks            []string `json:"networks"`
	Upstream            string   `json:"upstream"`
	ServerName          string   `json:"server_name"`
	ConfigurationDigest string   `json:"configuration_digest"`
}

// Artifact binds one already-certified immutable image and runtime specification.
type Artifact struct {
	Certificate         string        `json:"certificate"`
	ImageID             string        `json:"image_id"`
	RepositoryDigest    string        `json:"repository_digest"`
	ExecutableDigest    string        `json:"executable_digest"`
	ModulesDigest       string        `json:"modules_digest"`
	EvidenceDigest      string        `json:"evidence_digest"`
	SpecificationDigest string        `json:"specification_digest"`
	Specification       Specification `json:"specification"`
}

// Manifest is read from a root-owned local file. Its origin is the trusted
// certification administrator, not an HTTP request or an OCI image label.
type Manifest struct {
	Authority        string     `json:"authority"`
	Epoch            uint64     `json:"epoch"`
	LauncherDaemonID string     `json:"launcher_daemon_id"`
	ProviderDaemonID string     `json:"provider_daemon_id"`
	API              string     `json:"api"`
	Artifacts        []Artifact `json:"artifacts"`
}

func hexadecimal(s string) bool {
	if len(s) != 64 {
		return false
	}
	b, e := hex.DecodeString(s)
	return e == nil && hex.EncodeToString(b) == s
}
func token(s string) bool {
	return s != "" && len(s) <= 128 && strings.IndexFunc(s, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-'
	}) < 0
}
func canonical(raw []byte, out any, limit int) error {
	if len(raw) == 0 || len(raw) > limit {
		return ErrRefused
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return ErrRefused
	}
	exact, e := json.Marshal(out)
	if e != nil || !bytes.Equal(raw, exact) {
		return ErrRefused
	}
	return nil
}

// DecodeManifest accepts only the bounded canonical trusted encoding.
func DecodeManifest(raw []byte) (Manifest, error) {
	var m Manifest
	if canonical(raw, &m, MaxManifest) != nil {
		return m, ErrRefused
	}
	if operationErr := m.Validate(); operationErr != nil {
		return m, operationErr
	}
	return m, nil
}

// Validate refuses configurable isolation or transport escape paths.
func (m Manifest) Validate() error {
	if !token(m.Authority) || m.Epoch == 0 || m.LauncherDaemonID == "" || m.ProviderDaemonID == "" || m.LauncherDaemonID == m.ProviderDaemonID || m.API != "1.56" || len(m.Artifacts) == 0 || len(m.Artifacts) > 4 {
		return ErrRefused
	}
	seen := map[string]bool{}
	for _, a := range m.Artifacts {
		if !token(a.Certificate) || seen[a.Certificate] || !strings.HasPrefix(a.ImageID, "sha256:") || !hexadecimal(strings.TrimPrefix(a.ImageID, "sha256:")) || !hexadecimal(a.ExecutableDigest) || !hexadecimal(a.ModulesDigest) || !hexadecimal(a.EvidenceDigest) {
			return ErrRefused
		}
		seen[a.Certificate] = true
		_, digest, ok := strings.Cut(a.RepositoryDigest, "@sha256:")
		if !ok || !hexadecimal(digest) || len(a.RepositoryDigest) > 256 {
			return ErrRefused
		}
		s := a.Specification
		// Static image-owned executables live outside every writable or mounted subtree.
		if s.Executable != "/app/workload" || len(s.Arguments) > 8 || !hexadecimal(s.ConfigurationDigest) {
			return ErrRefused
		}
		for _, arg := range s.Arguments {
			if len(arg) > 256 || strings.ContainsRune(arg, 0) {
				return ErrRefused
			}
		}
		switch s.Role {
		case "client":
			if len(s.Networks) != 0 {
				return ErrRefused
			}
		case "executor":
			if len(s.Networks) != 2 || s.Networks[0] == s.Networks[1] {
				return ErrRefused
			}
		default:
			return ErrRefused
		}
		for _, network := range s.Networks {
			if !hexadecimal(network) {
				return ErrRefused
			}
		}
		u, e := url.Parse(s.Upstream)
		if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || s.ServerName == "" || len(s.ServerName) > 253 {
			return ErrRefused
		}
		if (s.Role == "executor" && u.Path != "/delegate") || (s.Role == "client" && u.Path != "/execute") || path.Clean(u.Path) != u.Path {
			return ErrRefused
		}
		b, e := json.Marshal(s)
		if e != nil || managed.Digest(b) != a.SpecificationDigest {
			return ErrRefused
		}
	}
	return nil
}
