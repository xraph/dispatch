package resource

import (
	"context"
	"fmt"
	"strings"
)

// Spec is the resolved, immutable resource contract for one job. It is
// produced once at enqueue and written to the job row, so scheduling
// reads columns and never calls user code.
//
// Spec is also the contract track C consumes to build a pod: Requests
// map to resource requests, Limits to limits, and Class to whatever
// scheduling class the isolation backend uses.
type Spec struct {
	// Requests is what admission accounts for and what a pod requests.
	Requests Set `json:"requests,omitempty"`
	// Limits is the enforcement ceiling. Memory defaults to Requests
	// (guaranteed); CPU is left unset (burstable), because overrunning
	// CPU makes a job slow while overrunning memory makes it dead.
	Limits Set `json:"limits,omitempty"`
	// Class is an opaque scheduling class for the isolation backend.
	Class string `json:"class,omitempty"`
}

// IsZero reports whether the spec constrains nothing.
func (s Spec) IsZero() bool {
	return s.Requests.IsZero() && s.Limits.IsZero() && s.Class == ""
}

// InputSize describes one declared input at enqueue time.
//
// It is plain data rather than an artifact.Ref because resource is a
// leaf package (see doc.go). The engine translates bindings into these,
// which also makes an Estimator testable with a struct literal.
type InputSize struct {
	// Name is the declared input slot name.
	Name string `json:"name"`
	// Bytes is the input's size.
	Bytes int64 `json:"bytes"`
	// Hash may be empty: the artifact plane fills content_hash
	// opportunistically at first staging, not at registration.
	Hash string `json:"hash,omitempty"`
}

// Request is everything an estimator may consider.
type Request struct {
	JobName    string
	Queue      string
	Payload    []byte
	Inputs     []InputSize
	InputBytes int64
	Declared   Set
	Attempt    int
	ScopeOrgID string
}

// ResourceFunc computes a requirement from the enqueue-time request. It
// runs once, in the enqueuing process, never on the scheduling path.
//
//nolint:revive // ResourceFunc is the name job definitions register under (see Tasks 7-8); Func would collide with ResolveInput.Func.
type ResourceFunc func(ctx context.Context, r Request) (Set, error)

// Estimator infers a requirement from historical measurement. The
// rollup estimator is the built-in implementation; a learned predictor
// slots in behind this same interface with nothing else moving.
type Estimator interface {
	Estimate(ctx context.Context, r Request) (Set, error)
}

// ResolveInput carries every source a requirement can come from,
// lowest precedence first.
type ResolveInput struct {
	GlobalDefault Set
	QueueDefault  Set
	Declared      Set
	Func          ResourceFunc
	Estimator     Estimator
	Override      Set

	DeclaredLimits Set
	OverrideLimits Set

	Class   string
	Request Request

	// MaxCapacity is the largest single-worker capacity known to the
	// engine. Empty disables the unschedulable check, which is correct
	// for a single-process engine with no registered workers.
	MaxCapacity Set
}

// Resolve collapses every source into one Spec.
//
// Precedence is a per-key overlay, lowest first:
//
//	global default → queue default → declaration → func → estimator → override
//
// Per-key rather than whole-set replacement, so an estimator that
// predicts only memory leaves a declared CPU value intact.
//
// Neither the func nor the estimator may fail enqueue: a failure there
// is logged by the caller and the lower-precedence value stands. The
// one error Resolve does return is ErrUnschedulable, because a job no
// worker can run must fail loudly and immediately.
func Resolve(ctx context.Context, in ResolveInput) (Spec, error) {
	req := make(Set)

	overlay := func(o Set) {
		for k, v := range o {
			req[k] = v
		}
	}

	overlay(in.GlobalDefault)
	overlay(in.QueueDefault)
	overlay(in.Declared)

	// Both dynamic sources see the declaration, so either can choose to
	// defer to it by returning it unchanged.
	r := in.Request
	r.Declared = req.Clone()

	if in.Func != nil {
		if out, err := in.Func(ctx, r); err == nil {
			overlay(out)
		}
	}

	if in.Estimator != nil {
		if out, err := in.Estimator.Estimate(ctx, r); err == nil {
			overlay(out)
		}
	}

	overlay(in.Override)

	spec := Spec{
		Requests: req,
		Limits:   defaultLimits(req, in.DeclaredLimits, in.OverrideLimits),
		Class:    in.Class,
	}

	if err := checkSchedulable(spec.Requests, in.MaxCapacity); err != nil {
		return Spec{}, err
	}

	return spec, nil
}

// defaultLimits gives every incompressible key a limit equal to its
// request and leaves CPU unset. Explicit limits override both.
func defaultLimits(requests, declared, override Set) Set {
	limits := make(Set, len(requests))

	for k, v := range requests {
		if k == CPU || v == 0 {
			continue
		}

		limits[k] = v
	}

	for k, v := range declared {
		limits[k] = v
	}

	for k, v := range override {
		limits[k] = v
	}

	return limits
}

// checkSchedulable rejects a requirement no worker could ever satisfy.
// An empty maxCapacity means capacity is unknown, which disables the
// check rather than rejecting everything.
func checkSchedulable(requests, maxCapacity Set) error {
	if len(maxCapacity) == 0 {
		return nil
	}

	over := requests.Exceeds(maxCapacity)
	if len(over) == 0 {
		return nil
	}

	parts := make([]string, 0, len(over))
	for _, k := range over {
		parts = append(parts, fmt.Sprintf("%s: need %d, largest worker has %d",
			k, requests[k], maxCapacity[k]))
	}

	return fmt.Errorf("%w (%s)", ErrUnschedulable, strings.Join(parts, "; "))
}
