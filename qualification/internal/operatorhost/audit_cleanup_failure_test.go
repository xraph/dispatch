package operatorhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func TestNativeAuditCleanupAfterRestartFailure(t *testing.T) {
	if os.Getenv("DISPATCH_OPERATOR_DSN") == "" || os.Getenv("DISPATCH_SINK_HOST_BINARY") == "" {
		if os.Getenv("DISPATCH_OPERATOR_REQUIRED") == "1" {
			t.Fatal("native sink and dedicated PostgreSQL required")
		}
		t.Skip("native sink and dedicated PostgreSQL required")
	}
	if os.Getenv("DISPATCH_AUDIT_CLEANUP_CHILD") == "1" {
		profiles, db := lifecycleChronicleProfiles(t)
		sinks := profiles.start(t)
		database, err := sinkhost.Open(t.Context(), profiles.configs["production"].DSNs["dispatch"])
		if err != nil {
			t.Fatal(err)
		}
		tracked := &auditTrackingStore{Store: pgstore.New(database)}
		c := newConfiguredCommandClient(t, tracked, &LifecycleOptions{InstanceID: "cleanup-after-restart", SkipSampleExecutions: true}, profiles.supervisor)
		if err = c.host.StartChroniclePublisher(t.Context(), profiles.routes.Routes); err != nil {
			t.Fatal(err)
		}
		sinks["audit"].kill(t)
		denyAuditRead(t, c)
		pending := readAuditSource(t, db["dispatch"], true)
		deadline, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err = c.host.Close(deadline)
		cancel()
		if !errors.Is(err, delivery.ErrIncompleteShutdown) || tracked.closes.Load() != 0 {
			t.Fatal("failure fixture did not retain storage")
		}
		sinks["audit"] = profiles.startSink(t, "audit")
		profiles.supervisor.afterHosts = append(profiles.supervisor.afterHosts, func() error {
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			if tracked.closes.Load() != 1 || tracked.afterClose.Load() != 0 {
				return errors.New("cleanup did not close storage exactly once")
			}
			var raw []byte
			if queryErr := db["dispatch"].QueryRow(ctx, `SELECT receipt FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NOT NULL`, pending.ID).Scan(&raw); queryErr != nil {
				return errors.New("cleanup failed to recover original source")
			}
			var receipt durable.SinkReceipt
			if json.Unmarshal(raw, &receipt) != nil || receipt.Verify(pending) != nil {
				return errors.New("cleanup recovered invalid receipt")
			}
			var accepted int
			if queryErr := db["chronicle"].QueryRow(ctx, `SELECT count(*) FROM chronicle_acceptances WHERE receipt->>'source_key'=$1`, pending.ID).Scan(&accepted); queryErr != nil || accepted != 1 {
				return errors.New("cleanup native acceptance missing or duplicated")
			}
			for _, profile := range profiles.configs {
				request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+profile.Addresses["chronicle"]+"/health", nil)
				if requestErr != nil {
					return errors.New("cleanup health request invalid")
				}
				response, responseErr := (&http.Client{Timeout: time.Second}).Do(request)
				if responseErr != nil {
					return errors.New("cleanup stopped a sink before host recovery")
				}
				_ = response.Body.Close()
				if response.StatusCode != 200 {
					return errors.New("cleanup sink unavailable before host recovery")
				}
			}
			t.Log("qualification_cleanup injected_failure_preserved=true host_closed_once=true sinks_alive=true original_native_receipt=1")
			if os.Getenv("DISPATCH_AUDIT_CLEANUP_ASSERTION_FAILURE") == "1" {
				return errors.New("injected cleanup assertion failure")
			}
			return nil
		})
		t.Fatal("injected failure immediately after audit sink restart")
	}
	for _, injectFailure := range []bool{false, true} {
		name := "recovered"
		if injectFailure {
			name = "cleanup_assertion_refused"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeAuditCleanupAfterRestartFailure$", "-test.v", "-test.timeout=50s")
			failureMode := "0"
			if injectFailure {
				failureMode = "1"
			}
			child.Env = append(os.Environ(), "DISPATCH_AUDIT_CLEANUP_CHILD=1", "DISPATCH_AUDIT_CLEANUP_ASSERTION_FAILURE="+failureMode)
			output, err := child.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal("cleanup regression child timed out")
			}
			classification := qualifyAuditCleanupExit(output, err)
			if injectFailure {
				if !errors.Is(classification, errAuditCleanupFailed) || bytes.Count(output, []byte("qualification_cleanup failure=")) != 1 || !bytes.Contains(output, []byte("qualification_cleanup failure=host_assertion")) || !bytes.Contains(output, []byte("qualification_cleanup result=failed failures=1\n")) {
					t.Fatal("parent did not refuse the recovered child with an additional cleanup assertion failure")
				}
				t.Log("qualification_cleanup negative_control=refused")
				return
			}
			if classification != nil {
				t.Fatal(classification)
			}
			t.Log("qualification_cleanup original_failure_preserved=true recovery=complete")
		})
	}
}

var errAuditCleanupFailed = errors.New("cleanup regression reported additional cleanup failure")

func qualifyAuditCleanupExit(output []byte, err error) error {
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return errors.New("cleanup regression child did not return expected failure")
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Exited() || status.ExitStatus() != 1 || bytes.Contains(output, []byte("panic:")) || bytes.Contains(output, []byte("fatal error:")) || bytes.Contains(output, []byte("runtime:")) || len(output) > 128*1024 {
		return errors.New("cleanup regression crashed or exceeded output bound")
	}
	text := string(output)
	stages := []string{"injected failure immediately after audit sink restart", "qualification_cleanup reader_stop=complete", "qualification_cleanup host_close=complete", "qualification_cleanup injected_failure_preserved=true host_closed_once=true sinks_alive=true original_native_receipt=1", "qualification_cleanup diagnostics=retained"}
	position := 0
	for _, stage := range stages {
		offset := strings.Index(text[position:], stage)
		if offset < 0 {
			return errors.New("failure cleanup did not retain expected ordered evidence")
		}
		position += offset + len(stage)
	}
	// Exactly one final, fixed result must follow sink cleanup. Test exit 1 alone
	// cannot distinguish the intentional primary failure from a cleanup error.
	const prefix = "qualification_cleanup result="
	if strings.Count(text, prefix) != 1 {
		return errors.New("cleanup result missing or duplicated")
	}
	resultAt := strings.Index(text, prefix)
	if resultAt < position {
		return errors.New("cleanup result preceded recovery evidence")
	}
	beforeResult := text[position:resultAt]
	if !strings.Contains(beforeResult, "qualification_cleanup sink_stop=complete") && !strings.Contains(beforeResult, "qualification_cleanup sink_stop=incomplete") {
		return errors.New("cleanup result preceded sink cleanup")
	}
	result, _, _ := strings.Cut(text[resultAt:], "\n")
	if strings.Contains(text[resultAt+len(result):], "qualification_cleanup ") {
		return errors.New("cleanup result was not final")
	}
	if result == prefix+"complete failures=0" {
		if strings.Contains(text, "qualification_cleanup failure=") || strings.Contains(beforeResult, "sink_stop=incomplete") {
			return errors.New("cleanup result contradicted failure evidence")
		}
		return nil
	}
	const failed = prefix + "failed failures="
	if strings.HasPrefix(result, failed) {
		count, parseErr := strconv.Atoi(strings.TrimPrefix(result, failed))
		if parseErr == nil && count > 0 && count <= 100 {
			return errAuditCleanupFailed
		}
	}
	return errors.New("cleanup result invalid")
}
