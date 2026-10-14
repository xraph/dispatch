package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"reflect"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
)

const creationProfile = "writerfloor.docker.creation.v1"

// ProvisionAcceptances rereads both independently authenticated acceptance domains.
// The original authorization, including its lease and revision, remains mandatory.
type ProvisionAcceptances interface {
	ReadManagedProviderStepAcceptance(context.Context, managed.ProviderStepAuthorization) (managed.ProviderStepAcceptance, error)
	ReadManagedTarget(context.Context, managed.Key) (*managed.Target, error)
}

type creation struct {
	Profile       string                             `json:"profile"`
	Backend       string                             `json:"backend"`
	Authorization managed.ProviderStepAuthorization  `json:"authorization"`
	Permission    managed.WireProviderStepPermission `json:"permission"`
	Receipt       managed.WireReceipt                `json:"receipt"`
	Command       docker.ManagedDockerCommand        `json:"command"`
	Incarnation   string                             `json:"incarnation"`
	State         string                             `json:"state"`
	ResourceID    string                             `json:"resource_id"`
	Signature     []byte                             `json:"signature"`
}

type resourceIndex struct {
	Profile    string `json:"profile"`
	Backend    string `json:"backend"`
	ResourceID string `json:"resource_id"`
	Creation   string `json:"creation"`
}

// CreationTransport retains the exact create response before exposing its resource ID.
// Its private journal and signing key belong to the configured resource custodian.
// Invoke it only below the authenticated admission and Docker executor boundaries.
// It does not grant transport, enroll a process, or implement resource deletion.
type CreationTransport struct {
	journal     docker.ManagedDockerJournal
	transport   docker.ManagedDockerTransport
	acceptances ProvisionAcceptances
	backend     string
	key         ed25519.PrivateKey
	identity    managed.RecoveryProducerIdentity
}

// NewCreationTransport fixes the SDK connection, protected journal and custody key.
// The launcher must quarantine journal rollback before exposing this composition.
func NewCreationTransport(journal *docker.FileManagedDockerJournal, transport *docker.SDKManagedDockerTransport, acceptances ProvisionAcceptances, backend string, key ed25519.PrivateKey, identity managed.RecoveryProducerIdentity) (*CreationTransport, error) {
	if journal == nil || transport == nil || nilInterface(acceptances) || backend == "" || len(backend) > 256 || len(key) != ed25519.PrivateKeySize || identity.ProducerID == "" || identity.Profile != docker.ManagedDockerProfile || identity.Version != "1" || identity.VerifierID == "" {
		return nil, managed.ErrUnsupported
	}
	return &CreationTransport{journal: journal, transport: transport, acceptances: acceptances, backend: backend, key: bytes.Clone(key), identity: identity}, nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

func bounded(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) > docker.ManagedDockerJournalLimit {
		return nil, managed.ErrCapacity
	}
	return raw, nil
}

func exactDecode(raw []byte, value any) error {
	if len(raw) == 0 || len(raw) > docker.ManagedDockerJournalLimit {
		return managed.ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil {
		return managed.ErrUnavailable
	}
	canonical, err := bounded(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return managed.ErrUnavailable
	}
	return nil
}

func creationKey(backend string, command docker.ManagedDockerCommand) (string, error) {
	raw, err := bounded(struct {
		Domain  string
		Backend string
		Command docker.ManagedDockerCommand
	}{creationProfile, backend, command})
	if err != nil {
		return "", err
	}
	return managed.Digest(raw), nil
}

func objectID(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 32 && hex.EncodeToString(raw) == value
}

func (c *CreationTransport) sign(record creation) (creation, error) {
	record.Signature = nil
	raw, err := bounded(record)
	if err != nil {
		return record, err
	}
	record.Signature = ed25519.Sign(c.key, raw)
	_, err = bounded(record)
	return record, err
}

func (c *CreationTransport) verify(ctx context.Context, record creation) error {
	signature := bytes.Clone(record.Signature)
	record.Signature = nil
	raw, err := bounded(record)
	publicKey, keyOK := c.key.Public().(ed25519.PublicKey)
	if err != nil || !keyOK || record.Profile != creationProfile || record.Backend != c.backend || !objectID(record.Incarnation) || !ed25519.Verify(publicKey, raw, signature) {
		return managed.ErrAuthority
	}
	if record.Command.Kind != managed.StepCreateNetwork && record.Command.Kind != managed.StepCreateContainer {
		return managed.ErrUnsupported
	}
	if record.Command.ResourceID != "" || record.Command.Name == "" || len(record.Command.Name) > 256 {
		return managed.ErrInvalid
	}
	if record.Command.Kind == managed.StepCreateNetwork && (record.Command.Service != nil || record.Command.NetworkID != "") {
		return managed.ErrInvalid
	}
	if record.Command.Kind == managed.StepCreateContainer && (record.Command.Service == nil || !objectID(record.Command.NetworkID)) {
		return managed.ErrInvalid
	}
	if record.State != "prepared" && record.State != "delegated" && record.State != "complete" {
		return managed.ErrUnavailable
	}
	if record.State == "complete" {
		if !objectID(record.ResourceID) {
			return managed.ErrUnavailable
		}
	} else if record.ResourceID != "" {
		return managed.ErrUnavailable
	}
	accepted, err := c.acceptances.ReadManagedProviderStepAcceptance(ctx, record.Authorization)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(record.Permission, managed.ProviderPermissionWire(accepted.Permission)) || !reflect.DeepEqual(record.Receipt, managed.ReceiptWire(accepted.Receipt)) || record.Command.Kind != accepted.Permission.Step.Kind || accepted.Permission.Physical.Provider != "docker" || accepted.Permission.ResourceID != "" || record.Command.Service != nil && record.Command.Service.Name != accepted.Permission.Step.Service {
		return managed.ErrAuthority
	}
	target, err := c.acceptances.ReadManagedTarget(ctx, accepted.Permission.Step.Key)
	if err != nil {
		return err
	}
	if target == nil || target.Physical != accepted.Permission.Physical || target.Manifest.SpecDigest != accepted.Permission.SpecDigest {
		return managed.ErrAuthority
	}
	spec, err := managed.ParseProvisionSpec(target.Manifest)
	if err != nil {
		return err
	}
	suffix := "network"
	if record.Command.Service != nil {
		suffix = record.Command.Service.Name
		found := false
		for _, service := range spec.Services {
			if reflect.DeepEqual(service, *record.Command.Service) {
				found = true
			}
		}
		if !found {
			return managed.ErrAuthority
		}
		network, _, readErr := c.readResource(ctx, record.Command.NetworkID, managed.StepCreateNetwork)
		if readErr != nil {
			return readErr
		}
		if network.Command.Kind != managed.StepCreateNetwork || network.Permission.Step.Key != accepted.Permission.Step.Key || network.Permission.Physical != accepted.Permission.Physical {
			return managed.ErrAuthority
		}
	}
	if record.Command.Name != "managed-"+accepted.Permission.Step.InstanceID.String()+"-"+suffix {
		return managed.ErrAuthority
	}
	return ctx.Err()
}

// PrepareCreation retains the accepted original authorization before any transport.
// Reusing identical command bytes for a different permission refuses permanently.
func (c *CreationTransport) PrepareCreation(ctx context.Context, authorization managed.ProviderStepAuthorization, command docker.ManagedDockerCommand) error {
	accepted, err := c.acceptances.ReadManagedProviderStepAcceptance(ctx, authorization)
	if err != nil {
		return err
	}
	key, err := creationKey(c.backend, command)
	if err != nil {
		return err
	}
	old, err := c.journal.Load(ctx, key)
	if err != nil {
		return err
	}
	if len(old) > 0 {
		var saved creation
		if operationErr := exactDecode(old, &saved); operationErr != nil {
			return operationErr
		}
		if operationErr := c.verify(ctx, saved); operationErr != nil {
			return operationErr
		}
		if !reflect.DeepEqual(saved.Authorization, authorization) || !reflect.DeepEqual(saved.Command, command) {
			return managed.ErrConflict
		}
		return nil
	}
	nonce := make([]byte, 32)
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	record, err := c.sign(creation{Profile: creationProfile, Backend: c.backend, Authorization: authorization, Permission: managed.ProviderPermissionWire(accepted.Permission), Receipt: managed.ReceiptWire(accepted.Receipt), Command: command, Incarnation: hex.EncodeToString(nonce), State: "prepared"})
	if err != nil {
		return err
	}
	if operationErr := c.verify(ctx, record); operationErr != nil {
		return operationErr
	}
	maximum := record
	maximum.State = "complete"
	maximum.ResourceID = hex.EncodeToString(make([]byte, 32))
	if _, err = bounded(maximum); err != nil {
		return err
	}
	raw, err := bounded(record)
	if err != nil {
		return err
	}
	changed, err := c.journal.CompareAndSwap(ctx, key, nil, raw)
	if err != nil {
		return err
	}
	if !changed {
		return managed.ErrConflict
	}
	return nil
}

// Execute claims custody before the SDK call. Any ambiguous claim or response leaves
// the invocation unresolved; reopening a delegated record never calls Docker again.
func (c *CreationTransport) Execute(ctx context.Context, command docker.ManagedDockerCommand) (string, error) {
	key, err := creationKey(c.backend, command)
	if err != nil {
		return "", err
	}
	raw, err := c.journal.Load(ctx, key)
	if err != nil {
		return "", err
	}
	var saved creation
	if operationErr := exactDecode(raw, &saved); operationErr != nil {
		return "", operationErr
	}
	if operationErr := c.verify(ctx, saved); operationErr != nil {
		return "", operationErr
	}
	if !reflect.DeepEqual(saved.Command, command) {
		return "", managed.ErrAuthority
	}
	if saved.State == "complete" {
		return c.index(ctx, key, saved)
	}
	if saved.State != "prepared" {
		return "", managed.ErrUnavailable
	}
	saved.State = "delegated"
	saved, err = c.sign(saved)
	if err != nil {
		return "", err
	}
	claimed, err := bounded(saved)
	if err != nil {
		return "", err
	}
	changed, err := c.journal.CompareAndSwap(ctx, key, raw, claimed)
	if err != nil {
		return "", err
	}
	if !changed {
		return "", managed.ErrConflict
	}
	resource, err := c.transport.Execute(ctx, command)
	if err != nil {
		return "", err
	}
	if !objectID(resource) {
		return "", managed.ErrUnavailable
	}
	saved.ResourceID = resource
	saved.State = "complete"
	saved, err = c.sign(saved)
	if err != nil {
		return "", err
	}
	complete, err := bounded(saved)
	if err != nil {
		return "", err
	}
	changed, err = c.journal.CompareAndSwap(ctx, key, claimed, complete)
	if err != nil {
		return "", err
	}
	if !changed {
		return "", managed.ErrConflict
	}
	return c.index(ctx, key, saved)
}

func (c *CreationTransport) index(ctx context.Context, key string, saved creation) (string, error) {
	index := resourceIndex{Profile: creationProfile, Backend: c.backend, ResourceID: saved.ResourceID, Creation: key}
	raw, err := bounded(index)
	if err != nil {
		return "", err
	}
	indexKey := managed.Digest([]byte(creationProfile + "/resource/" + c.backend + "/" + saved.ResourceID))
	existing, err := c.journal.Load(ctx, indexKey)
	if err != nil {
		return "", err
	}
	if len(existing) > 0 {
		if !bytes.Equal(raw, existing) {
			return "", managed.ErrAuthority
		}
		return saved.ResourceID, nil
	}
	changed, err := c.journal.CompareAndSwap(ctx, indexKey, nil, raw)
	if err != nil {
		return "", err
	}
	if !changed {
		return "", managed.ErrConflict
	}
	return saved.ResourceID, nil
}

// ReadCreation recovers an authenticated response and repairs only its derived index.
// Missing response evidence remains unavailable, regardless of current Docker state.
func (c *CreationTransport) ReadCreation(ctx context.Context, command docker.ManagedDockerCommand) (docker.ManagedDockerCustody, error) {
	key, err := creationKey(c.backend, command)
	if err != nil {
		return docker.ManagedDockerCustody{}, err
	}
	raw, err := c.journal.Load(ctx, key)
	if err != nil {
		return docker.ManagedDockerCustody{}, err
	}
	var saved creation
	if operationErr := exactDecode(raw, &saved); operationErr != nil {
		return docker.ManagedDockerCustody{}, operationErr
	}
	if operationErr := c.verify(ctx, saved); operationErr != nil {
		return docker.ManagedDockerCustody{}, operationErr
	}
	if saved.State != "complete" || !reflect.DeepEqual(saved.Command, command) {
		return docker.ManagedDockerCustody{}, managed.ErrUnavailable
	}
	if _, err = c.index(ctx, key, saved); err != nil {
		return docker.ManagedDockerCustody{}, err
	}
	return docker.ManagedDockerCustody{ResourceID: saved.ResourceID, Incarnation: saved.Incarnation, Attestation: bytes.Clone(saved.Signature)}, nil
}
