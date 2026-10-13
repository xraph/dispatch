package operatorhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func stopAuditOperator(t *testing.T, host *nativeLifecycleHost, incomplete bool) {
	t.Helper()
	if err := host.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-host.done:
		host.exit = err
		host.stopped = true
		if qualifyErr := qualifyAuditOperatorExit(err, host.logs.String(), incomplete); qualifyErr != nil {
			t.Fatal(qualifyErr)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("operator did not complete bounded shutdown")
	}
	records := safeShutdownRecords(host.logs.String())
	for _, record := range records {
		t.Log(record)
	}
	for _, credential := range host.client.host.Credentials {
		if bytes.Contains(host.logs.Bytes(), []byte(credential.Token)) {
			t.Fatal("operator log disclosed session token")
		}
	}
}

// Only typed, known fields survive child-log retention.
func safeShutdownRecords(output string) []string {
	records := []string{}
	for _, line := range strings.Split(output, "\n") {
		var record struct {
			Stage             string `json:"stage"`
			Category          string `json:"category"`
			DurationMS        int64  `json:"duration_ms"`
			Incomplete        bool   `json:"incomplete"`
			Deadline          bool   `json:"deadline"`
			Canceled          bool   `json:"canceled"`
			CountsKnown       bool   `json:"counts_known"`
			CountsBeforeDrain bool   `json:"counts_before_drain"`
			Pending           int64  `json:"pending"`
			InFlight          int    `json:"in_flight"`
		}
		if len(line) > 4096 || json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		switch record.Stage {
		case "route_load", "route_validate", "publisher_start", "http_shutdown", "worker_stop", "publisher_drain", "host_close":
		default:
			continue
		}
		switch record.Category {
		case "complete", "invalid_profile", "missing_route", "ownership_conflict", "unavailable", "incomplete", "deadline", "canceled":
		default:
			continue
		}
		record.DurationMS = min(max(record.DurationMS, 0), 60000)
		record.Pending = min(max(record.Pending, 0), 1000000)
		record.InFlight = min(max(record.InFlight, 0), 100)
		raw, _ := json.Marshal(record)
		records = append(records, string(raw))
		if len(records) > 32 {
			records = records[1:]
		}
	}
	return records
}

func TestNativeAuditProcessReopen(t *testing.T) {
	profiles, db := lifecycleChronicleProfiles(t)
	sinks := profiles.start(t)
	t.Setenv("DISPATCH_OPERATOR_DSN", profiles.configs["production"].DSNs["dispatch"])
	first := startNativeLifecycleWithSupervisor(t, profiles.supervisor, "audit-process-reopen", "--chronicle-routes-file", profiles.bundle)
	sinks["audit"].kill(t)
	denyAuditRead(t, first.client)
	pending := readAuditSource(t, db["dispatch"], true)
	auditEventually(t, "process did not attempt audit during outage", func() bool {
		return auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND attempts>0 AND delivered_at IS NULL`, pending.ID) == 1
	})
	stopAuditOperator(t, first, true)
	if auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NULL AND receipt IS NULL`, pending.ID) != 1 {
		t.Fatal("incomplete process lost pending audit")
	}
	sinks["audit"] = profiles.startSink(t, "audit")
	replacement := startNativeLifecycleWithSupervisor(t, profiles.supervisor, "audit-process-reopen", "--chronicle-routes-file", profiles.bundle)
	auditEventually(t, "reopened process did not recover original audit", func() bool {
		return auditCount(t, db["dispatch"], `SELECT count(*) FROM dispatch_durable_outbox WHERE id=$1 AND delivered_at IS NOT NULL`, pending.ID) == 1
	})
	verifyAuditReceipt(t, profiles.configs["audit"], db, pending)
	stopAuditOperator(t, replacement, false)
	for _, profile := range profiles.configs {
		if bytes.Contains(first.logs.Bytes(), []byte(profile.Credentials["chronicle"].Secret)) || bytes.Contains(replacement.logs.Bytes(), []byte(profile.Credentials["chronicle"].Secret)) {
			t.Fatal("operator exposed route credential")
		}
	}
	for _, namespace := range []string{"production", "audit", "foreign"} {
		sinks[namespace].kill(t)
	}
	if _, err := os.Stat(replacement.statePath); !os.IsNotExist(err) {
		t.Fatal("operator private state survived shutdown")
	}
	t.Log("expected incomplete process exit retained original source; process reopen recovered exactly one native receipt; clean SIGTERM completed before sinks stopped")
}

// qualifyAuditOperatorExit distinguishes a normal bounded drain failure from a crash.
func qualifyAuditOperatorExit(exitErr error, output string, incomplete bool) error {
	if strings.Contains(output, "panic:") || strings.Contains(output, "fatal error:") || strings.Contains(output, "runtime: ") {
		return errors.New("operator crashed during shutdown")
	}
	if incomplete {
		var processErr *exec.ExitError
		if !errors.As(exitErr, &processErr) {
			return errors.New("operator incomplete exit is not a normal process failure")
		}
		status, ok := processErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Exited() || status.ExitStatus() != 1 || processErr.ExitCode() != 1 {
			return errors.New("operator incomplete exit was not normal exit 1")
		}
	} else if exitErr != nil {
		return errors.New("operator clean shutdown failed")
	}
	found := false
	for _, line := range safeShutdownRecords(output) {
		var record struct {
			Stage      string
			Category   string
			Incomplete bool
			Deadline   bool
			Canceled   bool
		}
		if json.Unmarshal([]byte(line), &record) != nil || record.Stage != "publisher_drain" {
			continue
		}
		if incomplete {
			found = record.Category == "incomplete" && record.Incomplete && record.Deadline && !record.Canceled
		} else {
			found = record.Category == "complete" && !record.Incomplete && !record.Deadline && !record.Canceled
		}
	}
	if !found {
		return errors.New("operator exit lacks matching drain category and flags")
	}
	return nil
}
