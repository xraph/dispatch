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

	"github.com/xraph/dispatch/qualification/internal/operatorhost"
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
	flag.Parse()
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
	host, err := operatorhost.New(ctx, store)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = host.Close(closeCtx)
	}()
	if *runWorkers {
		stopWorkers := host.StartWorkers(ctx)
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
	}{url, host.Credentials})
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
