package stream

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/dispatch/id"
	"github.com/xraph/dispatch/job"
)

func TestBrokerJobCancelledReachesJobTopics(t *testing.T) {
	t.Parallel()

	b := NewBroker(testLogger())
	j := &job.Job{
		ID:         id.NewJobID(),
		Name:       "send-email",
		Queue:      "default",
		ScopeAppID: "app-1",
		ScopeOrgID: "org-1",
	}

	// The same topics job.failed reaches: the job's own topic, all jobs,
	// and the firehose.
	subs := []*Subscriber{
		b.Subscribe("job-sub", JobTopic(j.ID.String())),
		b.Subscribe("jobs-sub", TopicJobs),
		b.Subscribe("firehose-sub", TopicFirehose),
	}

	if err := b.OnJobCancelled(context.Background(), j); err != nil {
		t.Fatalf("OnJobCancelled: %v", err)
	}

	for _, sub := range subs {
		select {
		case evt := <-sub.C():
			if evt.Type != EventJobCancelled {
				t.Errorf("%s: Type = %q, want %q", sub.ID(), evt.Type, EventJobCancelled)
			}
			if evt.Topic != JobTopic(j.ID.String()) {
				t.Errorf("%s: Topic = %q, want %q", sub.ID(), evt.Topic, JobTopic(j.ID.String()))
			}
			var data JobEventData
			if err := json.Unmarshal(evt.Data, &data); err != nil {
				t.Fatalf("%s: decode data: %v", sub.ID(), err)
			}
			if data.JobID != j.ID.String() || data.JobName != "send-email" || data.Queue != "default" {
				t.Errorf("%s: data = %+v", sub.ID(), data)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: timed out waiting for job.cancelled", sub.ID())
		}
	}
}
