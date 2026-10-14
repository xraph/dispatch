//go:build linux

// Command docker-custody-qualify runs explicitly test-only custody qualification.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xraph/dispatch/qualification/writerfloor/internal/dockercustody"
)

func main() { os.Exit(run()) }

func run() int {
	socket := flag.String("socket", "/run/user/1000/docker.sock", "exact rootless provider socket")
	engine := flag.String("engine", "4452e8d6-a916-4fdc-b62a-49f9b3bec30a", "expected provider engine ID")
	image := flag.String("image", "", "preloaded Alpine reference with an exact real RepoDigest")
	directory := flag.String("directory", "", "existing private evidence parent directory")
	version := flag.String("ctrlplane-version", "v1.7.1-0.20261013235740-db121ca19b61", "exact published Ctrlplane module version")
	child := flag.String("child", "", "private child configuration path")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if *child != "" {
		if err := dockercustody.Child(ctx, *child); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	report, err := dockercustody.Run(ctx, dockercustody.Options{Socket: *socket, EngineID: *engine, Image: *image, Directory: *directory, CtrlplaneVersion: *version})
	if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		return 1
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
