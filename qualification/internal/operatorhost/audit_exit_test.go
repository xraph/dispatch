package operatorhost

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestAuditOperatorExitClassification(t *testing.T) {
	if mode := os.Getenv("DISPATCH_AUDIT_EXIT_CHILD"); mode != "" {
		fmt.Println(`{"stage":"publisher_drain","category":"incomplete","incomplete":true,"deadline":true,"canceled":false}`)
		switch mode {
		case "exit1":
			os.Exit(1)
		case "exit2":
			os.Exit(2)
		case "panic":
			panic("injected shutdown panic")
		case "signal":
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		case "fatal":
			fmt.Println("fatal error: injected runtime failure")
			os.Exit(1)
		case "wrong-flags":
			fmt.Println(`{"stage":"publisher_drain","category":"incomplete","incomplete":true,"deadline":false,"canceled":false}`)
			os.Exit(1)
		}
		os.Exit(3)
	}
	for _, mode := range []string{"exit1", "exit2", "panic", "signal", "fatal", "wrong-flags"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuditOperatorExitClassification$")
			child.Env = append(os.Environ(), "DISPATCH_AUDIT_EXIT_CHILD="+mode)
			output, err := child.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal("exit classification child timed out")
			}
			accepted := qualifyAuditOperatorExit(err, string(output), true) == nil
			if accepted != (mode == "exit1") {
				t.Fatalf("incomplete process classification mode=%s accepted=%v", mode, accepted)
			}
		})
	}
}
