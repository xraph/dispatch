package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/ctrlplane/id"
	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
)

type creationTestJournal struct {
	mu        sync.Mutex
	rows      map[string][]byte
	loseClaim bool
}

func (j *creationTestJournal) Load(_ context.Context, key string) ([]byte, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return bytes.Clone(j.rows[key]), nil
}
func (j *creationTestJournal) CompareAndSwap(_ context.Context, key string, old, next []byte) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !bytes.Equal(j.rows[key], old) {
		return false, nil
	}
	j.rows[key] = bytes.Clone(next)
	if j.loseClaim && bytes.Contains(next, []byte(`"state":"delegated"`)) {
		return false, managed.ErrUnavailable
	}
	return true, nil
}

type creationTestAcceptances struct {
	authorization managed.ProviderStepAuthorization
	acceptance    managed.ProviderStepAcceptance
	target        *managed.Target
	refuse        bool
}

func (a *creationTestAcceptances) ReadManagedProviderStepAcceptance(_ context.Context, q managed.ProviderStepAuthorization) (managed.ProviderStepAcceptance, error) {
	if a.refuse || !reflect.DeepEqual(q, a.authorization) {
		return managed.ProviderStepAcceptance{}, managed.ErrUnavailable
	}
	return a.acceptance, nil
}
func (a *creationTestAcceptances) ReadManagedTarget(_ context.Context, key managed.Key) (*managed.Target, error) {
	if a.target.Key != key {
		return nil, managed.ErrUnavailable
	}
	return a.target, nil
}

type creationTestTransport struct {
	calls int
	fail  bool
}

func (t *creationTestTransport) Execute(_ context.Context, _ docker.ManagedDockerCommand) (string, error) {
	t.calls++
	if t.fail {
		return "", managed.ErrUnavailable
	}
	return strings.Repeat("a", 64), nil
}

func newCreationTest(t *testing.T) (*CreationTransport, *creationTestJournal, *creationTestAcceptances, *creationTestTransport, docker.ManagedDockerCommand) {
	t.Helper()
	now := time.Date(2026, 10, 13, 1, 2, 3, 0, time.UTC)
	target := &managed.Target{Identity: managed.Identity{Key: managed.Key{TenantID: "tenant", InstanceID: id.New(id.PrefixInstance)}, Physical: managed.Physical{Provider: "docker", Placement: "fixed", Project: "test", Generation: "g1"}}}
	service := managed.ProvisionService{Name: "worker", Image: "example/worker@sha256:" + strings.Repeat("b", 64), Role: "main", CPUMillis: "100", MemoryMiB: "64", PIDLimit: "32", Startup: managed.StartupBindingRef{BindingID: "startup", BindingGeneration: "1", BindingDigest: strings.Repeat("c", 64)}}
	provision, err := json.Marshal(managed.ProvisionSpec{Version: "1", Services: []managed.ProvisionService{service}})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := json.Marshal(managed.ManagedSpec{Profile: "ctrlplane.managed.v1", RuntimeRoles: []managed.RuntimeRole{managed.Polling}, Provision: provision})
	if err != nil {
		t.Fatal(err)
	}
	target.Manifest = managed.Manifest{Version: 1, Members: []managed.Member{{Name: "worker", Service: "worker"}}, Spec: spec, SpecDigest: managed.Digest(spec)}
	step := managed.ProviderStepKey{OperationKey: managed.OperationKey{Key: target.Key, ActionID: id.New(managed.PrefixOperation)}, ProviderOperationID: id.New(managed.PrefixOperation), Kind: managed.StepCreateNetwork}
	commandID := id.New(managed.PrefixOperation)
	auth := managed.ProviderStepAuthorization{Command: managed.Command{Key: target.Key, ID: commandID, Revision: 4, LeaseEpoch: 1, Owner: "owner"}, Step: step}
	permission := managed.ProviderStepPermission{Step: step, CommandID: commandID, Digest: strings.Repeat("d", 64), SpecDigest: target.Manifest.SpecDigest, CreateProfile: "ctrlplane.managed.create.v1", Physical: target.Physical, IssuedAt: now, Revision: 5}
	receipt := managed.Receipt{ID: commandID, Digest: strings.Repeat("e", 64), Revision: 5, LeaseEpoch: 1, AcceptedAt: now.Add(time.Millisecond), LeaseUntil: now.Add(time.Minute)}
	accepted := &creationTestAcceptances{authorization: auth, acceptance: managed.ProviderStepAcceptance{Permission: permission, Receipt: receipt}, target: target}
	journal := &creationTestJournal{rows: map[string][]byte{}}
	transport := &creationTestTransport{}
	custodian := &CreationTransport{journal: journal, transport: transport, acceptances: accepted, backend: "docker-fixed", key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32)), identity: managed.RecoveryProducerIdentity{ProducerID: "fixed-custodian", Profile: docker.ManagedDockerProfile, Version: "1", VerifierID: "fixed-custodian"}}
	command := docker.ManagedDockerCommand{Kind: managed.StepCreateNetwork, Name: "managed-" + target.InstanceID.String() + "-network"}
	return custodian, journal, accepted, transport, command
}

func TestCreationCustodyResponseSurvivesReopenWithoutResend(t *testing.T) {
	c, _, accepted, transport, command := newCreationTest(t)
	ctx := context.Background()
	if err := c.PrepareCreation(ctx, accepted.authorization, command); err != nil {
		t.Fatal(err)
	}
	first, err := c.Execute(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	reopened := *c
	second, err := reopened.Execute(ctx, command)
	if err != nil || second != first || transport.calls != 1 {
		t.Fatalf("replay %q %v calls=%d", second, err, transport.calls)
	}
	custody, err := reopened.ReadCreation(ctx, command)
	if err != nil || custody.ResourceID != first || !objectID(custody.Incarnation) || custody.OriginalProcess != nil {
		t.Fatalf("custody %#v %v", custody, err)
	}
	accepted.refuse = true
	if _, err = reopened.ReadCreation(ctx, command); !errors.Is(err, managed.ErrUnavailable) {
		t.Fatalf("missing receipt accepted: %v", err)
	}
}

func TestCreationCustodyAmbiguityNeverResends(t *testing.T) {
	for _, lostClaim := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "claim"}[lostClaim], func(t *testing.T) {
			c, journal, accepted, transport, command := newCreationTest(t)
			ctx := context.Background()
			if err := c.PrepareCreation(ctx, accepted.authorization, command); err != nil {
				t.Fatal(err)
			}
			journal.loseClaim = lostClaim
			transport.fail = !lostClaim
			if _, err := c.Execute(ctx, command); err == nil {
				t.Fatal("ambiguous call succeeded")
			}
			for range 2 {
				if _, err := c.Execute(ctx, command); !errors.Is(err, managed.ErrUnavailable) {
					t.Fatalf("replay: %v", err)
				}
			}
			want := 1
			if lostClaim {
				want = 0
			}
			if transport.calls != want {
				t.Fatalf("calls=%d want=%d", transport.calls, want)
			}
		})
	}
}

func TestCreationCustodyTamperedResponseAndIndexRefuse(t *testing.T) {
	for _, field := range []string{"resource", "incarnation", "index", "permission"} {
		t.Run(field, func(t *testing.T) {
			c, journal, accepted, _, command := newCreationTest(t)
			ctx := context.Background()
			if err := c.PrepareCreation(ctx, accepted.authorization, command); err != nil {
				t.Fatal(err)
			}
			resource, err := c.Execute(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			key, err := creationKey(c.backend, command)
			if err != nil {
				t.Fatal(err)
			}
			if field == "index" {
				indexKey := managed.Digest([]byte(creationProfile + "/resource/" + c.backend + "/" + resource))
				delete(journal.rows, indexKey)
				if _, _, err = c.readResource(ctx, resource, managed.StepCreateNetwork); err == nil {
					t.Fatal("missing index accepted")
				}
				return
			}
			var record creation
			if err = json.Unmarshal(journal.rows[key], &record); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "resource":
				record.ResourceID = strings.Repeat("f", 64)
			case "incarnation":
				record.Incarnation = strings.Repeat("f", 64)
			case "permission":
				record.Permission.Digest = strings.Repeat("f", 64)
			}
			journal.rows[key], err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.ReadCreation(ctx, command); !errors.Is(err, managed.ErrAuthority) {
				t.Fatalf("forged row accepted: %v", err)
			}
		})
	}
}

func TestCreationCustodyCompleteRowCapIncludesBase64AndEnvelope(t *testing.T) {
	for _, extra := range []int{0, 1} {
		payload := struct {
			Value   []byte `json:"value"`
			Padding string `json:"padding"`
		}{Value: bytes.Repeat([]byte{3}, 100)}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		payload.Padding = strings.Repeat("x", docker.ManagedDockerJournalLimit-len(raw)+extra)
		raw, err = bounded(payload)
		if extra == 0 && (err != nil || len(raw) != docker.ManagedDockerJournalLimit) {
			t.Fatalf("cap len=%d err=%v", len(raw), err)
		}
		if extra == 1 && !errors.Is(err, managed.ErrCapacity) {
			t.Fatalf("cap+1: %v", err)
		}
	}
}

func TestCreationCustodyProducesExactRemovalBindingWithoutExecutorPrepare(t *testing.T) {
	c, journal, accepted, transport, command := newCreationTest(t)
	ctx := context.Background()
	if err := c.PrepareCreation(ctx, accepted.authorization, command); err != nil {
		t.Fatal(err)
	}
	resource, err := c.Execute(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	permission := accepted.acceptance.Permission
	permission.Step.Kind = managed.StepRemoveNetwork
	permission.Step.ActionID = id.New(managed.PrefixOperation)
	permission.Step.ProviderOperationID = id.New(managed.PrefixOperation)
	permission.CommandID = id.New(managed.PrefixOperation)
	permission.ResourceID = resource
	permission.Seal = 1
	permission.FenceDigest = strings.Repeat("f", 64)
	receipt := accepted.acceptance.Receipt
	receipt.ID = permission.CommandID
	receipt.Digest = permission.Digest
	receipt.AcceptedAt = permission.IssuedAt
	attempt := managed.AttemptKey{Key: permission.Step.Key, AttemptID: id.New(managed.PrefixOperation), Generation: "1"}
	original := managed.ProviderEffectOriginal{Key: managed.ProviderEffectBindingKey{AttemptKey: attempt, Step: permission.Step, OriginalCommandID: permission.CommandID}, AttemptRef: managed.AttemptRef{AttemptID: attempt.AttemptID, Generation: "1", DescriptorDigest: strings.Repeat("e", 64)}, Permission: managed.ProviderPermissionWire(permission), Receipt: managed.ReceiptWire(receipt)}
	facts, err := c.RecoverProviderEffectBinding(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.VerifyProviderEffectBinding(ctx, original, facts); err != nil {
		t.Fatal(err)
	}
	indexKey := managed.Digest([]byte(creationProfile + "/resource/" + c.backend + "/" + resource))
	delete(journal.rows, indexKey)
	if _, err = c.RecoverProviderEffectBinding(ctx, original); err == nil {
		t.Fatal("missing index recovered without repair")
	}
	if _, err = c.ReadCreation(ctx, command); err != nil {
		t.Fatalf("repair: %v", err)
	}
	repaired, err := c.RecoverProviderEffectBinding(ctx, original)
	if err != nil || !reflect.DeepEqual(repaired, facts) || transport.calls != 1 {
		t.Fatalf("repair changed custody or resent: %v calls=%d", err, transport.calls)
	}
	decoded, err := managed.DecodeProviderEffectCommand(facts.Command)
	if err != nil || decoded.ResourceID != resource || decoded.BackendAuthority != c.backend || !objectID(decoded.Incarnation) {
		t.Fatalf("command %#v %v", decoded, err)
	}
	replaced := original
	replaced.Permission.ResourceID = strings.Repeat("9", 64)
	if _, err = c.RecoverProviderEffectBinding(ctx, replaced); err == nil {
		t.Fatal("replacement resource inferred")
	}
	facts.Attestation[len(facts.Attestation)-2] ^= 1
	if err = c.VerifyProviderEffectBinding(ctx, original, facts); !errors.Is(err, managed.ErrAuthority) {
		t.Fatalf("forged evidence: %v", err)
	}
}
