//go:build linux

package managedhost

import (
	"context"
	"reflect"

	"github.com/xraph/ctrlplane/managed"
	"github.com/xraph/ctrlplane/provider/docker"

	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// ReadServiceCreation exports only the exact accepted parent identity from signed
// complete creation custody. Resource names never authorize service adoption.
func (c *CreationTransport) ReadServiceCreation(ctx context.Context, command docker.ManagedDockerCommand) (launcher.ServiceCreation, error) {
	var empty launcher.ServiceCreation
	if command.Kind != managed.StepCreateContainer {
		return empty, managed.ErrUnsupported
	}
	custody, err := c.ReadCreation(ctx, command)
	if err != nil {
		return empty, err
	}
	record, _, err := c.readResource(ctx, custody.ResourceID, managed.StepCreateContainer)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(record.Command, command) || record.Incarnation != custody.Incarnation {
		return empty, managed.ErrAuthority
	}
	return launcher.ServiceCreation{Target: record.Permission.Step.Key, ProviderOperationID: record.Permission.Step.ProviderOperationID, Command: record.Command, Custody: custody}, nil
}

var _ launcher.ServiceCreationReader = (*CreationTransport)(nil)
