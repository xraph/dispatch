//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "writerfloor fixed launcher requires the isolated Linux host")
	os.Exit(1)
}
