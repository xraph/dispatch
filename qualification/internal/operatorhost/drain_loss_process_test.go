package operatorhost

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/dispatch/operator"
)

func TestNativeDrainLossWindows(t *testing.T) {
	t.Setenv("DISPATCH_OPERATOR_DSN", isolatedPostgresScenario(t))
	for _, phase := range []string{"before", "after"} {
		for _, kill := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/kill=%t", phase, kill), func(t *testing.T) {
				marker := filepath.Join(t.TempDir(), "drain.json")
				n := startNativeLifecycle(t, "loss-window", "--drain-pause-"+phase+"-file", marker)
				c := n.client
				identity := n.identities[0]
				c.command("durable.retirementEnroll", operator.EnrollmentInput{NamespaceLifecycleInput: operator.NamespaceLifecycleInput{Namespace: "production"}, RequestID: "loss-enroll"}, 200)
				build := operator.BuildInput{Namespace: identity.Namespace, BuildID: identity.BuildID}
				c.command("durable.buildRegister", operator.RegisterBuildInput{BuildInput: build, RequestID: "loss-build", ExpectedVersion: "0"}, 200)
				requestID := fmt.Sprintf("loss-%s-%t", phase, kill)
				in := operator.WorkerDrainInput{WorkerInput: operator.WorkerInput{BuildInput: build, RuntimeID: identity.RuntimeID}, RequestID: requestID, OperationID: requestID, Deadline: time.Now().UTC().Add(time.Minute)}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.server.URL+"/api/dashboard/v1", bytes.NewReader(c.envelope("durable.workerDrain", in)))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Authorization", "Bearer "+c.host.Credentials["commander"].Token)
				request.Header.Set("Content-Type", "application/json")
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					response, callErr := http.DefaultClient.Do(request)
					if callErr == nil {
						_, _ = io.Copy(io.Discard, response.Body)
						_ = response.Body.Close()
					}
				}()
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					if info, statErr := os.Stat(marker); statErr == nil && info.Size() > 0 {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if info, statErr := os.Stat(marker); statErr != nil || info.Mode().Perm() != 0600 {
					t.Fatal("private invocation marker missing")
				}
				receipt := data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrainReceipt", in, 200))
				if receipt.Status != "requested" || receipt.RuntimeID != identity.RuntimeID || receipt.OperationID != in.OperationID {
					t.Fatal("barrier preceded durable exact receipt")
				}
				status := data[operator.WorkerObservation](t, c.command("durable.workerStatus", in.WorkerInput, 200))
				if status.AdmissionClosed != (phase == "after") {
					t.Fatal("barrier at wrong side of process invocation")
				}
				if kill {
					n.kill(t)
				}
				cancel()
				<-finished
				if kill {
					replacement := startNativeLifecycle(t, "loss-window")
					replay := data[operator.WorkerDrainAcceptance](t, replacement.client.command("durable.workerDrain", in, 200))
					if replay.Process != "unknown" || replay.Complete || replay.RuntimeID != identity.RuntimeID || !replay.Deadline.Equal(in.Deadline) {
						t.Fatal("lost original process was retargeted or inferred complete")
					}
					var currentRuntime string
					for _, current := range replacement.identities {
						if current.BuildID == identity.BuildID {
							currentRuntime = current.RuntimeID
						}
					}
					if currentRuntime == "" || currentRuntime == identity.RuntimeID {
						t.Fatal("replacement did not create a fresh runtime")
					}
					current := in.WorkerInput
					current.RuntimeID = currentRuntime
					currentStatus := data[operator.WorkerObservation](t, replacement.client.command("durable.workerStatus", current, 200))
					if currentStatus.RuntimeID != currentRuntime || currentStatus.AdmissionClosed || currentStatus.State != "not_started" {
						t.Fatalf("old drain mutated replacement: %+v", currentStatus)
					}
					t.Logf("replacement=%s admission_closed=%t state=%s", currentRuntime, currentStatus.AdmissionClosed, currentStatus.State)
					replacement.kill(t)
				} else {
					replay := data[operator.WorkerDrainAcceptance](t, c.command("durable.workerDrain", in, 200))
					if !replay.Complete || replay.RuntimeID != identity.RuntimeID || !replay.Deadline.Equal(in.Deadline) {
						t.Fatalf("same-incarnation reconciliation failed: %+v", replay)
					}
					n.kill(t)
				}
				t.Logf("phase=%s kill=%t request=%s original=%s receipt=requested admission_at_barrier=%t", phase, kill, requestID, identity.RuntimeID, status.AdmissionClosed)
			})
		}
	}
}
