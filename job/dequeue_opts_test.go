package job_test

import (
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
	"github.com/xraph/dispatch/resource"
)

func TestDequeueOptsIsUnbounded(t *testing.T) {
	reserved := id.NewJobID()

	tests := []struct {
		name string
		opts job.DequeueOpts
		want bool
	}{
		{"zero value", job.DequeueOpts{}, true},
		{"queues and limit only", job.DequeueOpts{Queues: []string{"default"}, Limit: 8}, true},
		{"nil budget map", job.DequeueOpts{Budget: nil}, true},
		{"empty budget map", job.DequeueOpts{Budget: resource.Set{}}, true},
		// An explicit zero is a worker with nothing free, not an absent
		// constraint. Treating it as unbounded would hand that worker the
		// whole queue.
		{"explicit zero budget key", job.DequeueOpts{Budget: resource.Set{resource.Memory: 0}}, false},
		{"budget", job.DequeueOpts{Budget: resource.Set{resource.CPU: 1000}}, false},
		{"custom keys", job.DequeueOpts{CustomKeys: []string{"fpga"}}, false},
		// Locality answers "should I order?", never "should I filter?".
		// Counting it here would make PreferHashes-only opts bounded, and
		// the empty-CustomKeys rule would then reject custom-resource jobs
		// on its behalf.
		{"prefer hashes alone", job.DequeueOpts{PreferHashes: []string{"blake3:a"}}, true},
		{
			"prefer hashes with a budget",
			job.DequeueOpts{
				Budget:       resource.Set{resource.CPU: 1000},
				PreferHashes: []string{"blake3:a"},
			},
			false,
		},
		{"reserved for", job.DequeueOpts{ReservedFor: &reserved}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.IsUnbounded(); got != tt.want {
				t.Errorf("IsUnbounded() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDequeueOptsAllows(t *testing.T) {
	const gib = int64(1) << 30

	other := id.NewJobID()

	newJob := func(res resource.Set) *job.Job {
		return &job.Job{ID: id.NewJobID(), Resources: res}
	}

	tests := []struct {
		name string
		opts job.DequeueOpts
		j    *job.Job
		want bool
	}{
		{
			"zero opts allow an oversized job",
			job.DequeueOpts{},
			newJob(resource.Set{resource.Memory: 512 * gib, "fpga": 4}),
			true,
		},
		{
			"exact fit is allowed",
			job.DequeueOpts{Budget: resource.Set{resource.Memory: 4 * gib}},
			newJob(resource.Set{resource.Memory: 4 * gib}),
			true,
		},
		{
			"one byte over is not",
			job.DequeueOpts{Budget: resource.Set{resource.Memory: 4 * gib}},
			newJob(resource.Set{resource.Memory: 4*gib + 1}),
			false,
		},
		{
			"absent budget key is unconstrained",
			job.DequeueOpts{Budget: resource.Set{resource.Memory: 4 * gib}},
			newJob(resource.Set{resource.GPU: 8000}),
			true,
		},
		{
			"explicit zero budget key filters",
			job.DequeueOpts{Budget: resource.Set{resource.GPU: 0}},
			newJob(resource.Set{resource.GPU: 1}),
			false,
		},
		{
			"zero requirement fits a zero budget",
			job.DequeueOpts{Budget: resource.Set{resource.GPU: 0}},
			newJob(nil),
			true,
		},
		{
			"custom key not offered",
			job.DequeueOpts{CustomKeys: []string{"tpu"}},
			newJob(resource.Set{"fpga": 1}),
			false,
		},
		{
			"custom key offered",
			job.DequeueOpts{CustomKeys: []string{"tpu"}},
			newJob(resource.Set{"tpu": 4}),
			true,
		},
		{
			"custom quantity is not compared at dequeue",
			job.DequeueOpts{CustomKeys: []string{"tpu"}, Budget: resource.Set{"tpu": 1}},
			newJob(resource.Set{"tpu": 64}),
			true,
		},
		{
			"prefix does not match a longer offered key",
			job.DequeueOpts{CustomKeys: []string{"fpga-large"}},
			newJob(resource.Set{"fpga": 1}),
			false,
		},
		{
			"prefix does not match a shorter offered key",
			job.DequeueOpts{CustomKeys: []string{"fpga"}},
			newJob(resource.Set{"fpga-large": 1}),
			false,
		},
		{
			"subset of an interleaved offer",
			job.DequeueOpts{CustomKeys: []string{"fpga", "nvme", "tpu"}},
			newJob(resource.Set{"fpga": 1, "tpu": 1}),
			true,
		},
		{
			"partial offer is not enough",
			job.DequeueOpts{CustomKeys: []string{"fpga"}},
			newJob(resource.Set{"fpga": 1, "tpu": 1}),
			false,
		},
		{
			// Bounded opts with an empty offer: the caller is
			// resource-aware and has no custom resources.
			"empty offer on bounded opts rejects a custom requirement",
			job.DequeueOpts{Budget: resource.Set{resource.Memory: 4 * gib}},
			newJob(resource.Set{"fpga": 1}),
			false,
		},
		{
			// Unbounded opts with an empty offer: the caller does not use
			// the resource model, and must keep claiming what it always did.
			"empty offer on unbounded opts claims a custom requirement",
			job.DequeueOpts{},
			newJob(resource.Set{"fpga": 1}),
			true,
		},
		{
			"a zero-quantity custom key is not a requirement",
			job.DequeueOpts{CustomKeys: []string{"tpu"}},
			newJob(resource.Set{"fpga": 0}),
			true,
		},
		{
			// PreferHashes must never filter, not even transitively through
			// the empty-CustomKeys rule.
			"prefer hashes alone claims a custom requirement",
			job.DequeueOpts{PreferHashes: []string{"blake3:a"}},
			newJob(resource.Set{"fpga": 1, resource.Memory: 1 << 40}),
			true,
		},
		{
			"reserved for another job",
			job.DequeueOpts{ReservedFor: &other},
			newJob(nil),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.Allows(tt.j); got != tt.want {
				t.Errorf("Allows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDequeueOptsAllowsReservedJob(t *testing.T) {
	j := &job.Job{ID: id.NewJobID(), Resources: resource.Set{resource.Memory: 8}}
	opts := job.DequeueOpts{ReservedFor: &j.ID}

	if !opts.Allows(j) {
		t.Error("Allows(reserved job) = false, want true")
	}

	// A reservation must not be able to bypass the budget, or the one path
	// a scheduler uses to place a large job is the path with no ceiling.
	opts.Budget = resource.Set{resource.Memory: 4}
	if opts.Allows(j) {
		t.Error("Allows(reserved job over budget) = true, want false")
	}
}

func TestDequeueOptsLess(t *testing.T) {
	base := time.Now().UTC()

	mk := func(priority int, offset time.Duration, hash string) *job.Job {
		return &job.Job{
			ID:               id.NewJobID(),
			Priority:         priority,
			RunAt:            base.Add(offset),
			PrimaryInputHash: hash,
		}
	}

	opts := job.DequeueOpts{PreferHashes: []string{"blake3:local"}}

	highRemote := mk(9, time.Minute, "blake3:remote")
	lowLocal := mk(1, 0, "blake3:local")
	lowRemoteEarly := mk(1, time.Second, "")
	lowRemoteLate := mk(1, time.Minute, "")

	// Priority outranks locality: a stream of locally cached work must not
	// be able to starve a high-priority job.
	if !opts.Less(highRemote, lowLocal) {
		t.Error("Less(high priority remote, low priority local) = false, want true")
	}

	// Within a priority band, locality wins even against an earlier RunAt.
	if !opts.Less(lowLocal, lowRemoteEarly) {
		t.Error("Less(local, earlier remote) = false, want true")
	}

	// Beyond that, RunAt ascending.
	if !opts.Less(lowRemoteEarly, lowRemoteLate) {
		t.Error("Less(earlier, later) = false, want true")
	}

	if opts.Prefers(mk(1, 0, "")) {
		t.Error("Prefers(job with no hash) = true, want false")
	}
}

func TestDequeueOptsOfferedCustomKeys(t *testing.T) {
	opts := job.DequeueOpts{CustomKeys: []string{"tpu", "fpga", "tpu", ""}}

	got := opts.OfferedCustomKeys()
	want := []string{"fpga", "tpu"}

	if len(got) != len(want) {
		t.Fatalf("OfferedCustomKeys() = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("OfferedCustomKeys() = %v, want %v", got, want)
		}
	}

	if none := (job.DequeueOpts{}).OfferedCustomKeys(); none != nil {
		t.Errorf("OfferedCustomKeys() on zero opts = %v, want nil", none)
	}
}
