//go:build linux

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xraph/dispatch/qualification/writerfloor/launcher"
)

func main() { os.Exit(run()) }

func run() int {
	configuration := flag.String("config", "", "root-owned canonical configuration file")
	command := flag.String("command", "inspect", "prepare, enroll, start, inspect or stop")
	launchID := flag.String("launch", "", "immutable launch identifier")
	certificate := flag.String("certificate", "", "existing trusted certificate identifier")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	host, closeHost, e := launcher.OpenHost(ctx, *configuration)
	if e != nil {
		fmt.Fprintln(os.Stderr, "launcher setup refused")
		return 1
	}
	status := 0
	switch *command {
	case "prepare":
		e = host.Prepare(ctx, *launchID, *certificate)
	case "enroll":
		var enrollment launcher.Enrollment
		enrollment, e = host.Enroll(ctx, *launchID)
		if e == nil {
			e = json.NewEncoder(os.Stdout).Encode(enrollment)
		}
	case "start":
		e = host.Start(ctx, *launchID)
	case "inspect":
		var record launcher.Record
		record, e = host.Inspect(*launchID)
		if e == nil {
			e = json.NewEncoder(os.Stdout).Encode(record)
		}
	case "stop":
		bounded, stop := context.WithTimeout(ctx, 25*time.Second)
		e = host.Stop(bounded, *launchID)
		stop()
	default:
		e = launcher.ErrRefused
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, "launcher refused or retained uncertainty; inspect the exact launch record")
		status = 1
	}
	if closeHost() != nil {
		fmt.Fprintln(os.Stderr, "launcher cleanup incomplete")
		status = 1
	}
	return status
}
