package operator

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/xraph/dispatch/durable"
)

type preparedAbortHostFixture struct {
	*queryHostFixture
	requestID, digest string
	calls             int
	denied            bool
}

func (h *preparedAbortHostFixture) VerifyPreparedAbort(ctx context.Context, binding durable.QueryRuntimeBinding, fence durable.QueryRemovalFence, requestID, digest string) (durable.QueryRemovalSettlement, durable.QueryRuntimeVerification, error) {
	h.calls++
	if h.denied || requestID != h.requestID || digest != h.digest {
		return durable.QueryRemovalSettlement{}, durable.QueryRuntimeVerification{}, errors.New("prepared request mismatch")
	}
	// Reuse the existing native proof fixture, without invoking the legacy fallback.
	fixture := *h.queryHostFixture
	return fixture.VerifyAbort(ctx, binding, fence)
}
func TestQueryPreparedAbortExactInputAndNoLegacyFallback(t *testing.T) {
	service, _, legacy, _ := queryFixture(t)
	input := reserveQueryFixture(t, service)
	input.RequestID = "prepared-abort"
	digest, err := durable.Fingerprint("operator.query_runtime.abort.v1", input)
	if err != nil {
		t.Fatal(err)
	}
	legacy.settled = true
	host := &preparedAbortHostFixture{queryHostFixture: legacy, requestID: input.RequestID, digest: digest, denied: true}
	service.queryHost = host
	if _, err = service.AbortQueryRuntimeRemoval(t.Context(), reader(), input); err == nil {
		t.Fatal("prepared refusal fell through to legacy")
	}
	if legacy.aborts != 0 || host.calls != 1 {
		t.Fatal("legacy verifier called after prepared denial")
	}
	binding, err := service.QueryRuntime(t.Context(), reader(), input.QueryRuntimeInput)
	if err != nil || binding.State != durable.QueryRuntimeRemoving {
		t.Fatal("refusal changed native state", err)
	}
	host.denied = false
	changed := input
	changed.RequestID = "other-request"
	if _, err = service.AbortQueryRuntimeRemoval(t.Context(), reader(), changed); err == nil {
		t.Fatal("changed input accepted")
	}
	accepted, err := service.AbortQueryRuntimeRemoval(t.Context(), reader(), input)
	if err != nil || accepted.Binding.State != durable.QueryRuntimeActive {
		t.Fatal("exact prepared input refused", err)
	}
	calls := host.calls
	host.denied = true
	replay, err := service.AbortQueryRuntimeRemoval(t.Context(), reader(), input)
	if err != nil || !reflect.DeepEqual(accepted, replay) || host.calls != calls || legacy.aborts != 0 {
		t.Fatal("exact replay recaptured proof", err)
	}
}
