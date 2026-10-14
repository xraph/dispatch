package managedhost

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"reflect"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"
)

func (c *CreationTransport) readResource(ctx context.Context, resource string, kind managed.ProviderStepKind) (creation, string, error) {
	var empty creation
	if !objectID(resource) {
		return empty, "", managed.ErrInvalid
	}
	indexKey := managed.Digest([]byte(creationProfile + "/resource/" + c.backend + "/" + resource))
	raw, err := c.journal.Load(ctx, indexKey)
	if err != nil {
		return empty, "", err
	}
	var index resourceIndex
	if operationErr := exactDecode(raw, &index); operationErr != nil {
		return empty, "", operationErr
	}
	if index.Profile != creationProfile || index.Backend != c.backend || index.ResourceID != resource || !objectID(index.Creation) {
		return empty, "", managed.ErrAuthority
	}
	raw, err = c.journal.Load(ctx, index.Creation)
	if err != nil {
		return empty, "", err
	}
	var record creation
	if operationErr := exactDecode(raw, &record); operationErr != nil {
		return empty, "", operationErr
	}
	key, err := creationKey(c.backend, record.Command)
	if err != nil || key != index.Creation || record.ResourceID != resource || record.State != "complete" || record.Command.Kind != kind {
		return empty, "", managed.ErrAuthority
	}
	if operationErr := c.verify(ctx, record); operationErr != nil {
		return empty, "", operationErr
	}
	return record, managed.Digest(raw), nil
}

type effectAttestation struct {
	Profile   string `json:"profile"`
	Creation  string `json:"creation"`
	Original  string `json:"original"`
	Command   string `json:"command"`
	Signature []byte `json:"signature"`
}

// RecoverProviderEffectBinding authenticates a removal identity from an actual saved
// create response. It performs no Docker inspection, process enrollment or mutation.
// Core supplies the exact independently accepted original permission and receipt.
func (c *CreationTransport) RecoverProviderEffectBinding(ctx context.Context, original managed.ProviderEffectOriginal) (managed.ProviderEffectBindingFacts, error) {
	var empty managed.ProviderEffectBindingFacts
	permission, err := original.Permission.Value()
	if err != nil {
		return empty, err
	}
	createKind := managed.StepCreateContainer
	if permission.Step.Kind == managed.StepRemoveNetwork {
		createKind = managed.StepCreateNetwork
	}
	if permission.Step.Kind != managed.StepRemoveNetwork && permission.Step.Kind != managed.StepRemoveContainer {
		return empty, managed.ErrUnsupported
	}
	saved, creationDigest, err := c.readResource(ctx, permission.ResourceID, createKind)
	if err != nil {
		return empty, err
	}
	if permission.Step.Key != saved.Permission.Step.Key || permission.Physical != saved.Permission.Physical || permission.SpecDigest != saved.Permission.SpecDigest || permission.CreateProfile != saved.Permission.CreateProfile || permission.Step.Service != saved.Permission.Step.Service {
		return empty, managed.ErrAuthority
	}
	kind, method := "container", "container_remove"
	options := []managed.ProviderEffectOption{{Name: "force", Value: "true"}, {Name: "remove_volumes", Value: "false"}}
	switch permission.Step.Kind {
	case managed.StepRemoveContainer:
		if saved.Command.Kind != managed.StepCreateContainer {
			return empty, managed.ErrAuthority
		}
	case managed.StepRemoveNetwork:
		if saved.Command.Kind != managed.StepCreateNetwork {
			return empty, managed.ErrAuthority
		}
		kind, method = "network", "network_remove"
		options = []managed.ProviderEffectOption{}
	default:
		return empty, managed.ErrUnsupported
	}
	command, err := managed.EncodeProviderEffectCommand(managed.ProviderEffectCommand{SchemaVersion: "1", Profile: docker.ManagedDockerProfile, BackendAuthority: c.backend, ResourceKind: kind, ResourceID: permission.ResourceID, Incarnation: saved.Incarnation, Method: method, Options: options})
	if err != nil {
		return empty, err
	}
	digest, err := managed.ProviderEffectCommandDigest(command)
	if err != nil {
		return empty, err
	}
	originalRaw, err := bounded(original)
	if err != nil {
		return empty, err
	}
	attestation := effectAttestation{Profile: creationProfile, Creation: creationDigest, Original: managed.Digest(originalRaw), Command: digest}
	signing, err := bounded(attestation)
	if err != nil {
		return empty, err
	}
	attestation.Signature = ed25519.Sign(c.key, signing)
	evidence, err := bounded(attestation)
	if err != nil {
		return empty, err
	}
	facts := managed.ProviderEffectBindingFacts{Original: original, Producer: c.identity, Command: command, ProviderCommandDigest: digest, Attestation: evidence}
	if operationErr := managed.ValidateProviderEffectBindingFacts(facts); operationErr != nil {
		return empty, operationErr
	}
	return facts, nil
}

// VerifyProviderEffectBinding rereads the immutable signed creation and both original
// provision acceptance domains. A rewritten index or substituted resource refuses.
func (c *CreationTransport) VerifyProviderEffectBinding(ctx context.Context, original managed.ProviderEffectOriginal, facts managed.ProviderEffectBindingFacts) error {
	if !reflect.DeepEqual(facts.Original, original) || facts.Producer != c.identity {
		return managed.ErrAuthority
	}
	expected, err := c.RecoverProviderEffectBinding(ctx, original)
	if err != nil {
		return err
	}
	if !bytes.Equal(facts.Command, expected.Command) || facts.ProviderCommandDigest != expected.ProviderCommandDigest || !bytes.Equal(facts.Attestation, expected.Attestation) {
		return managed.ErrAuthority
	}
	return nil
}

var (
	_ managed.ProviderEffectBindingProducer = (*CreationTransport)(nil)
	_ managed.ProviderEffectBindingVerifier = (*CreationTransport)(nil)
	_ docker.ManagedDockerTransport         = (*CreationTransport)(nil)
)
