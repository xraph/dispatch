//go:build linux

package managedhost

import (
	"context"
	"testing"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

// livePairRuntime routes only fixed exact targets to their real enrolled service
// channels. Neither caller labels nor absence can select a replacement process.
type livePairRuntime struct {
	hosts map[durable.QueryRuntimeTarget]*RuntimeHost
}

func newLivePairRuntime(t *testing.T, fixtures ...*liveServiceFixture) *livePairRuntime {
	t.Helper()
	if len(fixtures) < 1 || len(fixtures) > 16 {
		t.Fatal("bounded fixture set")
	}
	out := &livePairRuntime{hosts: map[durable.QueryRuntimeTarget]*RuntimeHost{}}
	for _, fixture := range fixtures {
		target := fixture.NativeIdentity.QueryRuntimeTarget
		if fixture.Runtime == nil || out.hosts[target] != nil {
			t.Fatal("duplicate runtime")
		}
		out.hosts[target] = fixture.Runtime
	}
	return out
}
func (p *livePairRuntime) EnrolledIdentity(target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	h := p.hosts[target]
	if h == nil {
		return durable.QueryRuntimeIdentity{}, operator.ErrRuntimeUnavailable
	}
	return h.EnrolledIdentity(target)
}
func (p *livePairRuntime) ResolveBinding(ctx context.Context, target durable.QueryRuntimeTarget) (durable.QueryRuntimeIdentity, error) {
	h := p.hosts[target]
	if h == nil {
		return durable.QueryRuntimeIdentity{}, operator.ErrRuntimeUnavailable
	}
	return h.ResolveBinding(ctx, target)
}
func (p *livePairRuntime) Verify(ctx context.Context, binding durable.QueryRuntimeBinding) (durable.QueryRuntimeVerification, error) {
	h := p.hosts[binding.QueryRuntimeTarget]
	if h == nil {
		return durable.QueryRuntimeVerification{}, operator.ErrRuntimeUnavailable
	}
	return h.Verify(ctx, binding)
}
func (p *livePairRuntime) WorkerControl(ctx context.Context, target durable.QueryRuntimeTarget) (operator.WorkerControl, error) {
	h := p.hosts[target]
	if h == nil {
		return nil, operator.ErrRuntimeUnavailable
	}
	return h.WorkerControl(ctx, target)
}

var _ launcher.QualificationOperatorRuntime = (*livePairRuntime)(nil)
