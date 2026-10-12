package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/xraph/dispatch/qualification/internal/sinkhost"
)

func main() { os.Exit(run()) }
func run() int {
	role := flag.String("role", "", "dispatch, chronicle, relay or receiver")
	config := flag.String("config", "", "private local configuration file")
	flag.Parse()
	c, err := sinkhost.Load(*config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "host configuration failed")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := sinkhost.Serve(ctx, *role, c); err != nil {
		fmt.Fprintln(os.Stderr, "host stopped:", sinkhost.SafeError(err, c))
		return 1
	}
	return 0
}
