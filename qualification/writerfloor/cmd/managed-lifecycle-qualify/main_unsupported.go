//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "managed service requires the fixed Linux qualification host")
	os.Exit(2)
}
