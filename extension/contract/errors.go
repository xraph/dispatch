package contract

import (
	"context"
	"errors"

	"github.com/xraph/forge"
	fc "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/dispatch"
	"github.com/xraph/dispatch/artifact"
	"github.com/xraph/dispatch/paging"
	"github.com/xraph/dispatch/resource"
	"github.com/xraph/dispatch/workflow"
)

func badRequest(message string) error { return &fc.Error{Code: fc.CodeBadRequest, Message: message} }
func notFound(message string) error   { return &fc.Error{Code: fc.CodeNotFound, Message: message} }
func stateConflict(state string) error {
	return &fc.Error{Code: fc.CodeConflict, Message: "the current state does not allow this action", Details: map[string]any{"state": nullable(state)}}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var known *fc.Error
	if errors.As(err, &known) {
		return known
	}
	switch {
	case errors.Is(err, dispatch.ErrJobNotFound), errors.Is(err, dispatch.ErrRunNotFound),
		errors.Is(err, dispatch.ErrWorkflowNotFound), errors.Is(err, dispatch.ErrCronNotFound),
		errors.Is(err, dispatch.ErrDLQNotFound), errors.Is(err, dispatch.ErrWorkerNotFound),
		errors.Is(err, artifact.ErrNotFound):
		return notFound("resource not found")
	case errors.Is(err, paging.ErrInvalidCursor):
		return badRequest("cursor is not valid for this list")
	case errors.Is(err, dispatch.ErrInvalidState), errors.Is(err, dispatch.ErrDLQAlreadyReplayed),
		errors.Is(err, resource.ErrUnschedulable), errors.Is(err, dispatch.ErrDuplicateCron):
		return &fc.Error{Code: fc.CodeConflict, Message: "the resource changed or cannot run with its current configuration"}
	case errors.Is(err, workflow.ErrRunnerShutdown), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return &fc.Error{Code: fc.CodeUnavailable, Message: "the operation is unavailable or timed out", Retryable: true}
	case errors.Is(err, artifact.ErrPermissionDenied):
		return &fc.Error{Code: fc.CodePermissionDenied, Message: "the artifact backend refused access"}
	default:
		return &fc.Error{Code: fc.CodeInternal, Message: "an internal error occurred"}
	}
}

func (d Deps) mapError(intent string, err error) error {
	mapped := mapError(err)
	if d.Logger != nil && errors.Is(mapped, fc.ErrInternal) {
		d.Logger.Error("dispatch/contract: internal error answering intent", forge.F("intent", intent), forge.F("error", err))
	}
	return mapped
}
