package sinkhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/forge/extensions/auth"
	"github.com/xraph/warden"

	"github.com/xraph/dispatch/api"
	drt "github.com/xraph/dispatch/durable/runtime"
	"github.com/xraph/dispatch/operator"
	"github.com/xraph/dispatch/qualification/internal/authority"
	"github.com/xraph/dispatch/security"
	dpg "github.com/xraph/dispatch/store/postgres"
)

func callbackFile(directory, workflow string) string {
	digest := sha256.Sum256([]byte(workflow))
	return filepath.Join(directory, hex.EncodeToString(digest[:])+".json")
}
func operatorRoutes(ctx context.Context, router forge.Router, registry auth.Registry, w *warden.Engine, store *dpg.Store, boundary security.Boundary, c Config) (func() error, error) {
	noop := func() error { return nil }
	if c.CallbackDirectory == "" {
		return noop, nil
	}
	worker, err := drt.NewWorker(store, drt.Options{Namespace: c.Binding.Namespace, BuildID: "callback-v1", Queue: "callbacks", Owner: "callback-worker", PollInterval: 25 * time.Millisecond, Workflows: map[string]drt.WorkflowFunc{"callback": func(flow *drt.Workflow, _ []byte) ([]byte, error) {
		return flow.ActivityWithOptions("callback", "callback", "", nil, drt.ActivityOptions{StartToCloseTimeout: time.Minute, HeartbeatTimeout: time.Minute}).Get()
	}}, Activities: map[string]drt.ActivityFunc{"callback": func(ctx context.Context, info drt.ActivityInfo, _ []byte) ([]byte, error) {
		handle, err := info.DeferCompletion(ctx)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(handle) //nolint:gosec // Genuine callback proof goes only to the task-owned private fixture directory.
		if err != nil {
			return nil, err
		}
		path := callbackFile(c.CallbackDirectory, handle.Key.WorkflowID)
		if writeErr := os.WriteFile(path+".tmp", encoded, 0o600); writeErr != nil {
			return nil, writeErr
		}
		return nil, os.Rename(path+".tmp", path)
	}}})
	if err != nil {
		return noop, err
	}
	service, err := operator.New(operator.Options{Store: store, Reads: store, Catalog: store, InstallationID: c.Binding.InstallationID, Audit: boundary, Authorizer: &operator.WardenAuthorizer{Engine: func() (*warden.Engine, error) { return w, nil }}, CursorKeys: operator.CursorKeys{Active: "fixture", Keys: map[string][]byte{"fixture": c.TokenKey}}, Runtime: func(namespace, build string) (*drt.Worker, error) {
		if !worker.ServesBuild(namespace, build) {
			return nil, errors.New("fixture runtime unavailable")
		}
		return worker, nil
	}})
	if err != nil {
		return noop, err
	}
	authenticator := security.NewForgeAuthenticator(func() (auth.Registry, error) { return registry, nil }, authority.ProviderName)
	callbacks := api.New(nil, nil, api.WithSecurity(authenticator, boundary), api.WithDurableCallbacks(service, authenticator)).Handler()
	for _, path := range []string{"/v1/durable/activities/complete", "/v1/durable/activities/heartbeat"} {
		if routeErr := router.POST(path, func(f forge.Context) error { callbacks.ServeHTTP(f.Response(), f.Request()); return nil }); routeErr != nil {
			return noop, routeErr
		}
	}
	if err := router.POST("/durable/start", func(f forge.Context) error {
		p, authErr := authenticator.Authenticate(f.Context(), f.Request())
		if authErr != nil {
			return f.String(401, "authentication required")
		}
		var input operator.StartInput
		if decode(f.Request(), &input) != nil {
			return f.String(400, "invalid command")
		}
		receipt, startErr := service.Start(f.Context(), p, input)
		if startErr != nil {
			if errors.Is(startErr, security.ErrForbidden) {
				return f.String(403, "access denied")
			}
			return f.String(503, "command unavailable")
		}
		return f.JSON(202, receipt)
	}); err != nil {
		return noop, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	return func() error { cancel(); return <-done }, nil
}
