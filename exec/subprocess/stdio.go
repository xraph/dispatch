package subprocess

import (
	"bufio"
	"io"
	"strings"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/exec"
)

// shimDiagnosticPrefix marks a line on the child's stderr as coming from
// the shim itself rather than from the handler or whatever it shelled
// out to — applyRlimits (exec/shim/rlimit_unix.go) is the current
// producer. streamOutput logs these at Warn rather than Info so they do
// not blend into ordinary handler chatter, which is otherwise all Info:
// a shim diagnostic is Dispatch telling the operator something about the
// isolation itself, not output the handler chose to produce.
const shimDiagnosticPrefix = "dispatch/exec/shim: "

// streamOutput copies r line by line into logger, tagging each line with
// the job's id and name and which stream it came from. It runs until r
// returns EOF or another read error, which happens once the child's copy
// of the underlying pipe is closed — at process exit, or once the parent
// kills it and the process is reaped.
//
// Reading is line-oriented, through a bufio.Reader rather than a
// bufio.Scanner, so a handler or a native library writing one very long
// line of unstructured output cannot exceed Scanner's default token limit
// and silently drop the rest of the stream; ReadString has no such cap.
//
// This is still gated on logger: WithLogger's own default is a no-op
// logger, so a shim diagnostic is invisible here regardless of level
// unless the caller configured one — see WithStrictRlimits for the
// rlimit case specifically, which gets a guarantee that does not depend
// on a logger being configured at all.
func streamOutput(r io.Reader, logger log.Logger, req *exec.Request, stream string) {
	reader := bufio.NewReader(r)

	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			trimmed := strings.TrimSuffix(line, "\n")
			fields := []log.Field{
				log.String("job_id", req.JobID.String()),
				log.String("job_name", req.Name),
				log.String("stream", stream),
			}

			if strings.HasPrefix(trimmed, shimDiagnosticPrefix) {
				logger.Warn(trimmed, fields...)
			} else {
				logger.Info(trimmed, fields...)
			}
		}
		if err != nil {
			return
		}
	}
}
