package subprocess

import (
	"bufio"
	"io"
	"strings"

	log "github.com/xraph/go-utils/log"

	"github.com/xraph/dispatch/exec"
)

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
func streamOutput(r io.Reader, logger log.Logger, req *exec.Request, stream string) {
	reader := bufio.NewReader(r)

	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			logger.Info(strings.TrimSuffix(line, "\n"),
				log.String("job_id", req.JobID.String()),
				log.String("job_name", req.Name),
				log.String("stream", stream),
			)
		}
		if err != nil {
			return
		}
	}
}
