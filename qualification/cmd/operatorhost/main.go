package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/qualification/internal/operatorhost"
	"github.com/xraph/dispatch/qualification/internal/sinkhost"
	"github.com/xraph/dispatch/store/memory"
	pgstore "github.com/xraph/dispatch/store/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "operator fixture failed")
		os.Exit(1)
	}
}
func run() (returnErr error) {
	listen := flag.String("listen", "127.0.0.1:0", "numeric loopback listen address")
	runWorkers := flag.Bool("run-workers", true, "run fixture workers; disable only to inspect accepted commands before execution")
	statePath := flag.String("state-file", "", "new private state file containing URL and ephemeral credentials")
	instance := flag.String("lifecycle-instance", "", "trusted physical instance identity for lifecycle qualification")
	activation := flag.String("activation-file", "", "private local file whose creation releases registered workers to start")
	pauseBefore := flag.String("drain-pause-before-file", "", "private marker that pauses the first drain before process invocation")
	pauseAfter := flag.String("drain-pause-after-file", "", "private marker that pauses the first drain after process invocation")
	chroniclePath := flag.String("chronicle-config", "", "private native Chronicle config; starts a lifecycle host without sample executions")
	flag.Parse()
	if *chroniclePath != "" && *instance == "" {
		return errors.New("native Chronicle qualification requires lifecycle mode")
	}
	if (*pauseBefore != "" && *pauseAfter != "") || ((*pauseBefore != "" || *pauseAfter != "") && *instance == "") {
		return errors.New("one lifecycle drain barrier required")
	}
	address, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return err
	}
	ip := net.ParseIP(address)
	if ip == nil || !ip.IsLoopback() || *statePath == "" {
		return errors.New("numeric loopback and private state file required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var store operatorhost.Store = memory.New()
	if dsn := os.Getenv("DISPATCH_OPERATOR_DSN"); dsn != "" {
		driver := pgdriver.New()
		if driverErr := driver.Open(ctx, dsn); driverErr != nil {
			return driverErr
		}
		db, openErr := grove.Open(driver)
		if openErr != nil {
			return openErr
		}
		store = pgstore.New(db)
	}
	var host *operatorhost.Host
	if *instance == "" {
		host, err = operatorhost.New(ctx, store)
	} else {
		host, err = operatorhost.NewWithLifecycle(ctx, store, operatorhost.LifecycleOptions{InstanceID: *instance, SkipSampleExecutions: *chroniclePath != "", DrainObserver: fileDrainObserver{before: *pauseBefore, after: *pauseAfter}})
	}
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = host.Close(closeCtx)
	}()
	if *chroniclePath != "" {
		config, loadErr := sinkhost.Load(*chroniclePath)
		if loadErr != nil {
			return loadErr
		}
		stopPublisher, startErr := host.StartChroniclePublisher(ctx, config.Binding, "http://"+config.Addresses["chronicle"]+"/accept", config.Credentials["chronicle"].Secret)
		if startErr != nil {
			return startErr
		}
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			returnErr = errors.Join(returnErr, stopPublisher(shutdown))
		}()
	}
	if *runWorkers {
		stopWorkers := startWorkers(ctx, host, *activation)
		defer func() { returnErr = errors.Join(returnErr, stopWorkers()) }()
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	file, err := os.OpenFile(*statePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer os.Remove(*statePath)
	url := "http://" + listener.Addr().String()
	err = json.NewEncoder(file).Encode(struct {
		URL         string                             `json:"url"`
		Credentials map[string]operatorhost.Credential `json:"credentials"`
		Runtimes    []durable.QueryRuntimeIdentity     `json:"runtimes,omitempty"`
	}{url, host.Credentials, host.RuntimeIdentities()})
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	server := &http.Server{Handler: host.Handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Println("Operator fixture ready at " + url)
	select {
	case err = <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}

// startWorkers leaves the control endpoint available while enrollment is pending.
func startWorkers(ctx context.Context, host *operatorhost.Host, activation string) func() error {
	ctx, cancel := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() {
		if activation != "" {
			tick := time.NewTicker(25 * time.Millisecond)
			defer tick.Stop()
			for {
				if info, err := os.Stat(activation); err == nil && info.Mode().IsRegular() {
					break
				}
				select {
				case <-ctx.Done():
					stopped <- nil
					return
				case <-tick.C:
				}
			}
		}
		stop := host.StartWorkers(ctx)
		<-ctx.Done()
		stopped <- stop()
	}()
	return func() error { cancel(); return <-stopped }
}
