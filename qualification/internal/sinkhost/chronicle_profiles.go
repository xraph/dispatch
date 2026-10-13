package sinkhost

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	aid "github.com/xraph/authsome/id"

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
)

const maxChronicleBundleBytes = 64 << 10

// ErrInvalidProfile identifies an invalid private outbound profile.
var ErrInvalidProfile = errors.New("qualification: invalid Chronicle profile")

// ChronicleRoute contains one fixed outbound binding and its own credential.
type ChronicleRoute struct {
	Binding    ecosystem.Binding    `json:"binding"`
	Address    string               `json:"address"`
	Credential authority.Credential `json:"credential"`
}

// ChronicleProfiles is the private, fixed three-namespace fixture bundle.
type ChronicleProfiles struct {
	Version int              `json:"version"`
	Routes  []ChronicleRoute `json:"routes"`
}

// ChronicleProfile extracts the existing single-bound process configuration.
func ChronicleProfile(c Config) ChronicleRoute {
	return ChronicleRoute{Binding: c.Binding, Address: c.Addresses["chronicle"], Credential: c.Credentials["chronicle"]}
}

func validChronicleAddress(address string) bool {
	if len(address) > 64 {
		return false
	}
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	n, portErr := strconv.Atoi(port)
	return err == nil && ip != nil && ip.IsLoopback() && portErr == nil && n > 0 && n <= 65535 && strconv.Itoa(n) == port && net.JoinHostPort(ip.String(), port) == address
}

// Validate checks identity, credentials and endpoint uniqueness before use.
func (p ChronicleProfiles) Validate() error {
	if p.Version != 1 || len(p.Routes) != 3 {
		return ErrInvalidProfile
	}
	return ValidateChronicleRoutes(p.Routes)
}

// ValidateChronicleRoutes also permits one legacy route for coverage refusal.
func ValidateChronicleRoutes(routes []ChronicleRoute) error {
	if len(routes) == 0 || len(routes) > 3 {
		return ErrInvalidProfile
	}
	namespaces, addresses, keys, accounts := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	base := routes[0].Binding
	for _, route := range routes {
		b, c := route.Binding, route.Credential
		if b.Validate() != nil || !validChronicleAddress(route.Address) || len(c.Secret) < 1 || len(c.Secret) > 4096 || strings.IndexFunc(c.Secret, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			return ErrInvalidProfile
		}
		if _, err := aid.ParseAppID(b.AppID); err != nil {
			return ErrInvalidProfile
		}
		if b.OrgID != "" {
			if _, err := aid.ParseOrgID(b.OrgID); err != nil {
				return ErrInvalidProfile
			}
		}
		if _, err := aid.ParseAPIKeyID(c.KeyID.String()); err != nil || c.KeyID.IsNil() {
			return ErrInvalidProfile
		}
		if _, err := aid.ParseServiceAccountID(c.AccountID.String()); err != nil || c.AccountID.IsNil() {
			return ErrInvalidProfile
		}
		if b.InstallationID != base.InstallationID || b.AppID != base.AppID || b.Producer != base.Producer || b.OrgID != base.OrgID {
			return ErrInvalidProfile
		}
		if namespaces[b.Namespace] || addresses[route.Address] || keys[c.KeyID.String()] || accounts[c.AccountID.String()] {
			return ErrInvalidProfile
		}
		namespaces[b.Namespace], addresses[route.Address], keys[c.KeyID.String()], accounts[c.AccountID.String()] = true, true, true, true
	}
	return nil
}

// LoadChronicleProfiles opens a bounded private regular file without following symlinks.
func LoadChronicleProfiles(path string) (ChronicleProfiles, error) {
	var p ChronicleProfiles
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return p, ErrInvalidProfile
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	var stat syscall.Stat_t
	if err = syscall.Fstat(fd, &stat); err != nil || int64(stat.Uid) != int64(os.Geteuid()) {
		return p, ErrInvalidProfile
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maxChronicleBundleBytes {
		return p, ErrInvalidProfile
	}
	decoder := json.NewDecoder(io.LimitReader(file, maxChronicleBundleBytes+1))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&p); err != nil {
		return ChronicleProfiles{}, ErrInvalidProfile
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ChronicleProfiles{}, ErrInvalidProfile
	}
	return p, p.Validate()
}

// SaveChronicleProfiles creates a new owner-only bundle in a private directory.
func SaveChronicleProfiles(path string, p ChronicleProfiles) error {
	if err := p.Validate(); err != nil {
		return err
	}
	directory, err := os.Lstat(filepath.Dir(path))
	if err != nil || !directory.IsDir() || directory.Mode().Perm() != 0o700 {
		return ErrInvalidProfile
	}
	owner, ok := directory.Sys().(*syscall.Stat_t)
	if !ok || int64(owner.Uid) != int64(os.Geteuid()) {
		return ErrInvalidProfile
	}
	data, err := json.Marshal(p)
	if err != nil || len(data) > maxChronicleBundleBytes {
		return ErrInvalidProfile
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return ErrInvalidProfile
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}
