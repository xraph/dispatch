package operatorhost

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// auditSupervisor owns current and replacement children through failure cleanup.
// Database and private-file cleanups are registered before this supervisor.
type auditSupervisor struct {
	t               *testing.T
	directory       string
	readers         []*httptest.Server
	hosts           []*Host
	operators       []*nativeLifecycleHost
	sinks           []*nativeChronicle
	afterHosts      []func() error
	hostDiagnostics []func() []string
	observe         func(context.Context) error
}

func newAuditSupervisor(t *testing.T, db map[string]*pgx.Conn) *auditSupervisor {
	t.Helper()
	s := &auditSupervisor{t: t, directory: t.TempDir()}
	s.observe = func(ctx context.Context) error {
		rows, err := db["dispatch"].Query(ctx, `SELECT namespace,delivered_at IS NOT NULL,error_category,count(*) FROM dispatch_durable_outbox GROUP BY namespace,delivered_at IS NOT NULL,error_category ORDER BY namespace,delivered_at IS NOT NULL,error_category LIMIT 32`)
		if err != nil {
			return errors.New("outbox observation unavailable")
		}
		defer rows.Close()
		for rows.Next() {
			var namespace, category string
			var delivered bool
			var count int64
			if err = rows.Scan(&namespace, &delivered, &category, &count); err != nil {
				return errors.New("outbox observation unavailable")
			}
			switch namespace {
			case "production", "audit", "foreign":
			default:
				namespace = "unknown"
			}
			switch category {
			case "", "unavailable", "conflict":
			default:
				category = "unknown"
			}
			t.Logf("qualification_cleanup namespace=%s delivered=%t category=%s count=%d", namespace, delivered, category, min(max(count, 0), 1000000))
		}
		if rows.Err() != nil {
			return errors.New("outbox observation unavailable")
		}
		return nil
	}
	t.Cleanup(s.cleanup)
	return s
}

func (s *auditSupervisor) cleanup() {
	// The primary test may already have failed. Track cleanup assertions separately
	// so the subprocess parent can distinguish recovery from another failure.
	failures := 0
	fail := func(category string) {
		failures = min(failures+1, 100)
		s.t.Error("qualification_cleanup failure=" + category)
	}
	for _, reader := range s.readers {
		reader.Close()
	}
	s.t.Log("qualification_cleanup reader_stop=complete")
	for _, operator := range s.operators {
		if !operator.stopped {
			signalErr := operator.command.Process.Signal(syscall.SIGTERM)
			if errors.Is(signalErr, os.ErrProcessDone) {
				signalErr = nil
			}
			select {
			case operator.exit = <-operator.done:
				operator.stopped = true
			case <-time.After(12 * time.Second):
				fail("operator_join_timeout")
				_ = operator.command.Process.Kill()
				select {
				case operator.exit = <-operator.done:
					operator.stopped = true
				case <-time.After(5 * time.Second):
					fail("operator_join_incomplete")
				}
			}
			if signalErr != nil || operator.exit != nil {
				fail("operator_stop")
			}
		}
		if operator.stopped {
			if operator.client != nil {
				for _, credential := range operator.client.host.Credentials {
					if credential.Token != "" && bytes.Contains(operator.logs.Bytes(), []byte(credential.Token)) {
						fail("operator_credential")
					}
				}
			}
			for _, record := range safeShutdownRecords(operator.logs.String()) {
				s.t.Log(record)
			}
		}
	}
	for _, host := range s.hosts {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := host.Close(ctx)
		cancel()
		if err != nil {
			fail("host_close")
		} else {
			s.t.Log("qualification_cleanup host_close=complete")
		}
	}
	for _, records := range s.hostDiagnostics {
		for _, record := range records() {
			s.t.Log(record)
		}
	}
	for _, observe := range s.afterHosts {
		if observe() != nil {
			fail("host_assertion")
		}
	}
	observation, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if s.observe != nil {
		if s.observe(observation) != nil {
			fail("outbox_observation")
		}
	}
	cancel()
	s.t.Log("qualification_cleanup diagnostics=retained")
	var sinkErr error
	for i := len(s.sinks) - 1; i >= 0; i-- {
		if err := s.sinks[i].cleanup(s.t); err != nil {
			fail("sink_cleanup")
			sinkErr = errors.Join(sinkErr, err)
		}
	}
	if sinkErr == nil {
		s.t.Log("qualification_cleanup sink_stop=complete")
	} else {
		s.t.Log("qualification_cleanup sink_stop=incomplete")
	}
	if failures == 0 {
		s.t.Log("qualification_cleanup result=complete failures=0")
	} else {
		s.t.Logf("qualification_cleanup result=failed failures=%d", failures)
	}
}
