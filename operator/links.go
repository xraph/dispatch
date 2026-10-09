package operator

import (
	"context"
	"errors"

	"github.com/xraph/dispatch/security"
)

type Chain struct {
	Page[Execution]
	Restricted bool `json:"restricted"`
}

func (s *Service) Chain(ctx context.Context, p security.Principal, in RunInput) (Chain, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := Chain{Page: observed([]Execution{})}
	if _, err := s.resolve(ctx, p, ReadChain, in.Key); err != nil {
		return out, err
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	in.Limit = limit
	token := in.Cursor
	in.Cursor = ""
	bind := binding(p, s.installation, ReadChain, in)
	state, err := s.cursors.open(token, bind)
	if err != nil {
		return out, err
	}
	key := in.Key
	if state.Position != "" {
		key.RunID = state.Position
	}
	for len(out.Items) < limit {
		if err = s.check(ctx, p, ReadChain, key); err != nil {
			if onlyDenied(err) {
				out.Restricted = true
				out.Complete = false
				return out, nil
			}
			return out, err
		}
		e, readErr := s.resolve(ctx, p, ReadExecution, key)
		if readErr != nil {
			if onlyDenied(readErr) {
				out.Restricted = true
				out.Complete = false
				return out, nil
			}
			return out, readErr
		}
		out.Items = append(out.Items, s.project(e))
		if e.NextRunID == "" {
			return out, nil
		}
		key.RunID = e.NextRunID
	}
	// The next target remains encrypted and is reauthorized before use.
	out.Complete = false
	out.Cursor, err = s.cursors.seal(cursorState{Position: key.RunID}, bind)
	return out, err
}
func onlyDenied(err error) bool {
	return errors.Is(err, security.ErrForbidden) && !errors.Is(err, security.ErrUnavailable)
}

type Children struct {
	Page[Link]
	Restricted bool `json:"restricted"`
}

func (s *Service) Children(ctx context.Context, p security.Principal, in RunInput) (Children, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	out := Children{Page: observed([]Link{})}
	if _, err := s.resolve(ctx, p, ReadChain, in.Key); err != nil {
		return out, err
	}
	limit, err := pageLimit(in.Limit)
	if err != nil {
		return out, err
	}
	in.Limit = limit
	token := in.Cursor
	in.Cursor = ""
	bind := binding(p, s.installation, "children", in)
	state, err := s.cursors.open(token, bind)
	if err != nil {
		return out, err
	}
	rows, err := s.store.ListChildExecutions(ctx, in.Key, state.Position, DiscoveryBudget)
	if err != nil {
		return out, safeError(err)
	}
	out.Complete = len(rows) < DiscoveryBudget
	last := state.Position
	for i, child := range rows {
		last = child.CommandID
		if _, err = s.resolve(ctx, p, ReadExecution, child.CurrentKey); err != nil {
			if onlyDenied(err) {
				out.Restricted = true
				continue
			}
			return out, err
		}
		out.Items = append(out.Items, Link{Kind: "child", Namespace: child.CurrentKey.Namespace, WorkflowID: child.CurrentKey.WorkflowID, RunID: child.CurrentKey.RunID})
		if len(out.Items) == limit {
			out.Complete = out.Complete && i == len(rows)-1
			break
		}
	}
	err = nil
	if !out.Complete {
		out.Cursor, err = s.cursors.seal(cursorState{Position: last}, bind)
	}
	return out, err
}
