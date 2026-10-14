package managedhost

import (
	"context"
	"reflect"

	"github.com/xraph/ctrlplane/managed"
)

// DiscoveryReader combines exact accepted references with their retained commands.
type DiscoveryReader interface {
	managed.WorkDiscoveryStore
	managed.PhaseCommandStore
	ReadManagedOperation(context.Context, managed.OperationKey) (managed.OperationState, error)
}

// Discovered retains the authoritative reference beside the exact original command.
// Presence never grants a call or proves that unresolved membership has settled.
type Discovered struct {
	Reference managed.WorkReference
	Action    *managed.OperationState
	Phase     *managed.PhaseCommandRecord
}

// Sweep is one finite inspection window. Keep NextCursor only after handling every
// returned item. Restarting from the beginning remains safe and preserves old work.
type Sweep struct {
	Head       managed.WorkHead
	Items      []Discovered
	NextCursor string
}

// InspectWork performs at most four bounded pages and exact rereads every reference.
// It neither clears membership nor invokes remote calls from desired selection alone.
func InspectWork(ctx context.Context, store DiscoveryReader, key managed.Key, cursor string, pages int) (Sweep, error) {
	var empty Sweep
	if nilInterface(store) || pages < 1 || pages > 4 || len(cursor) > managed.MaxWorkCursor {
		return empty, managed.ErrInvalid
	}
	head, err := store.ReadManagedWorkHead(ctx, key)
	if err != nil {
		return empty, err
	}
	if _, err = managed.EncodeWorkHead(head); err != nil || head.Key != key {
		return empty, managed.ErrUnavailable
	}
	result := Sweep{Head: head, Items: []Discovered{}}
	seen := map[string]bool{}
	var previous uint64
	for range pages {
		if operationErr := ctx.Err(); operationErr != nil {
			return empty, operationErr
		}
		page, readErr := store.ListManagedUnresolvedWork(ctx, managed.WorkPageRequest{Key: key, Cursor: cursor, Limit: managed.MaxWorkPageItems})
		if readErr != nil {
			return empty, readErr
		}
		if _, err = managed.EncodeWorkPage(page); err != nil || page.Key != key {
			return empty, managed.ErrUnavailable
		}
		for _, ref := range page.Items {
			ordinal, parseErr := ref.Ordinal.Uint64()
			if parseErr != nil || ordinal <= previous {
				return empty, managed.ErrUnavailable
			}
			previous = ordinal
			exact, exactErr := store.ReadManagedWork(ctx, ref)
			if exactErr != nil {
				return empty, exactErr
			}
			if !reflect.DeepEqual(exact, ref) {
				return empty, managed.ErrAuthority
			}
			item := Discovered{Reference: ref}
			if ref.Kind == "action" {
				action, actionErr := store.ReadManagedOperation(ctx, ref.Operation)
				if actionErr != nil {
					return empty, actionErr
				}
				if action.Key != ref.Operation || action.Receipt.CanonicalDigest != ref.Digest {
					return empty, managed.ErrAuthority
				}
				item.Action = &action
			} else {
				phase, phaseErr := store.ReadManagedPhaseCommand(ctx, ref)
				if phaseErr != nil {
					return empty, phaseErr
				}
				if !reflect.DeepEqual(phase.Work, ref) {
					return empty, managed.ErrAuthority
				}
				item.Phase = &phase
			}
			result.Items = append(result.Items, item)
		}
		result.NextCursor = page.NextCursor
		if page.NextCursor == "" {
			return result, nil
		}
		if page.NextCursor == cursor || seen[page.NextCursor] {
			return empty, managed.ErrUnavailable
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
	return result, nil
}
