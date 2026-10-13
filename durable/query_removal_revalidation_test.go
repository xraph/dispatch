package durable_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
)

func TestQueryRemovalCompatibleBuildTimeLocation(t *testing.T) {
	prior := durable.BuildAdmission{State: durable.BuildAccepting, Epoch: 1, Version: 2, ChangedAt: time.Date(2030, 1, 2, 3, 4, 5, 123000, time.UTC)}
	next := prior
	next.ChangedAt = prior.ChangedAt.In(time.FixedZone("driver-local", 3600))
	if !durable.QueryRemovalCompatibleBuild(prior, next) {
		t.Fatal("same build and instant rejected after location-only round trip")
	}
	next.ChangedAt = next.ChangedAt.Add(time.Nanosecond)
	if durable.QueryRemovalCompatibleBuild(prior, next) {
		t.Fatal("changed instant accepted")
	}
	next = prior
	next.Version++
	if durable.QueryRemovalCompatibleBuild(prior, next) {
		t.Fatal("changed build version accepted")
	}
}
