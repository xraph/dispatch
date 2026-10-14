//go:build linux

package launcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	aid "github.com/xraph/authsome/id"
	"github.com/xraph/ctrlplane/managed"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/policy"
	"github.com/xraph/warden/wardenerr"

	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/qualification/writerfloor/internal/admission"
)

// Enrollment is immutable trusted startup metadata. It contains no bearer or TLS key.
// The host loads these exact credential pairs before listening, never from a request.
type Enrollment struct {
	Authority   string            `json:"authority"`
	Manifest    string            `json:"manifest"`
	App         string            `json:"app"`
	Environment string            `json:"environment"`
	Tenant      string            `json:"tenant"`
	Binding     admission.Binding `json:"binding"`
	Key         string            `json:"key"`
	Policy      policy.Policy     `json:"policy"`
}

// Credential returns only the exact configured IDs required by the real Authsome provider.
func (e Enrollment) Credential() (authority.Credential, error) {
	account, err := aid.ParseServiceAccountID(e.Binding.Principal)
	if err != nil {
		return authority.Credential{}, err
	}
	key, err := aid.ParseAPIKeyID(e.Key)
	if err != nil {
		return authority.Credential{}, err
	}
	return authority.Credential{AccountID: account, KeyID: key}, nil
}

func (h *Host) readEnrollment(r Record) (Enrollment, error) {
	raw, e := readPrivate(filepath.Join(h.root, r.ID+"-enrollment"), MaxRecord)
	if e != nil {
		return Enrollment{}, e
	}
	var v Enrollment
	manifest, e := json.Marshal(h.manifest)
	if e != nil {
		return v, e
	}
	if canonical(raw, &v, MaxRecord) != nil || managed.Digest(raw) != r.Enrollment || r.Manifest != managed.Digest(manifest) || v.Manifest != r.Manifest || v.Authority != h.manifest.Authority || v.App != h.app.String() || v.Environment != h.environment.String() || v.Tenant != h.tenant || v.Key != r.Key {
		return v, ErrRefused
	}
	a, e := h.manifest.artifact(r.Certificate)
	if e != nil {
		return v, e
	}
	want := admission.Binding{ID: r.ID, Certificate: r.Certificate, RuntimeID: r.Container, Principal: r.Account, PeerDigest: r.Peer, Epoch: h.manifest.Epoch, Role: a.Specification.Role}
	if v.Binding != want {
		return v, ErrRefused
	}
	return v, nil
}

// Enroll installs only the exact retained per-launch Warden policy. A lost insert
// response is reconciled by that policy ID and complete content before success.
func (h *Host) Enroll(ctx context.Context, launchID string) (Enrollment, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	j, e := openJournal(h.root, launchID)
	if e != nil {
		return Enrollment{}, e
	}
	defer j.close()
	r, e := j.read(launchID)
	if e != nil {
		return Enrollment{}, e
	}
	if r.Phase != "bound" && r.Phase != "policy" && r.Phase != "enrolled" {
		return Enrollment{}, ErrUncertain
	}
	if r.Phase == "bound" {
		a, err := h.manifest.artifact(r.Certificate)
		if err != nil {
			return Enrollment{}, err
		}
		if operationErr := h.certificate(ctx, a); operationErr != nil {
			return Enrollment{}, operationErr
		}
		action := "writerfloor.request"
		if a.Specification.Role == "executor" {
			action = "writerfloor.delegate"
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		v := Enrollment{Authority: h.manifest.Authority, Manifest: r.Manifest, App: h.app.String(), Environment: h.environment.String(), Tenant: h.tenant, Key: r.Key, Binding: admission.Binding{ID: r.ID, Certificate: r.Certificate, RuntimeID: r.Container, Principal: r.Account, PeerDigest: r.Peer, Epoch: h.manifest.Epoch, Role: a.Specification.Role}, Policy: policy.Policy{ID: wid.NewPolicyID(), AppID: h.app.String(), TenantID: h.tenant, Name: "writerfloor-" + r.ID, Effect: policy.EffectAllow, IsActive: true, Version: 1, Subjects: []policy.SubjectMatch{{Kind: "service_acct", ID: r.Account}}, Actions: []string{"writerfloor.ready", action}, Resources: []string{"writerfloor_authority:" + h.manifest.Authority}, CreatedAt: now, UpdatedAt: now}}
		raw, err := json.Marshal(v)
		if err != nil || len(raw) > MaxRecord {
			return Enrollment{}, ErrRefused
		}
		r.Enrollment = managed.Digest(raw)
		if operationErr := writePrivate(h.root, launchID+"-enrollment", raw, true); operationErr != nil {
			return Enrollment{}, operationErr
		}
		if operationErr := j.save(r, "policy"); operationErr != nil {
			return Enrollment{}, operationErr
		}
		r.Phase = "policy"
	}
	v, e := h.readEnrollment(r)
	if e != nil {
		return Enrollment{}, e
	}
	saved, e := h.policies.GetPolicy(ctx, h.tenant, v.Policy.ID)
	if errors.Is(e, wardenerr.ErrPolicyNotFound) && r.Phase == "policy" {
		// The policy ID and exact body were durable before this unique insert.
		if e = h.policies.CreatePolicy(ctx, &v.Policy); e != nil {
			return Enrollment{}, ErrUncertain
		}
		saved, e = h.policies.GetPolicy(ctx, h.tenant, v.Policy.ID)
	}
	if e != nil || saved == nil {
		return Enrollment{}, ErrUncertain
	}
	want, e := json.Marshal(v.Policy)
	if e != nil {
		return Enrollment{}, e
	}
	actual, e := json.Marshal(saved)
	if e != nil || !bytes.Equal(want, actual) {
		return Enrollment{}, ErrRefused
	}
	if operationErr := j.save(r, "enrolled"); operationErr != nil {
		return Enrollment{}, operationErr
	}
	return v, nil
}

// Enrollment reads the immutable prepared machine metadata without enrolling or starting it.
// Callers still verify current Authsome, policy and ledger state at their own boundary.
func (h *Host) Enrollment(launchID string) (Enrollment, error) {
	r, e := h.Inspect(launchID)
	if e != nil {
		return Enrollment{}, e
	}
	switch r.Phase {
	case "enrolled", "starting", "running", "stopping", "removed", "stopped":
	default:
		return Enrollment{}, ErrUncertain
	}
	return h.readEnrollment(r)
}
