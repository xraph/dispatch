#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")" && pwd)
process_dir=$(mktemp -d)
process_container="dispatch-qualification-$$"
cleanup() {
  docker rm -f -v "$process_container" >/dev/null 2>&1 || true
  rm -rf "$process_dir"
}
trap cleanup EXIT
export GOWORK=off GOTOOLCHAIN=go1.26.10
cd "$root"

# Metadata survives private binary and credential cleanup, including failed runs.
process_evidence=$(mktemp -d "${DISPATCH_QUALIFICATION_EVIDENCE_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}}/dispatch-process-evidence.XXXXXX")
printf 'Qualification evidence: %s\n' "$process_evidence"
process_expected=()
if [ -n "${DISPATCH_QUALIFICATION_SOURCE_MANIFEST:-}" ]; then
  process_expected=(--expected "$DISPATCH_QUALIFICATION_SOURCE_MANIFEST")
fi
python3 "$root/source-provenance.py" check --repo "$root/.." \
  --output "$process_evidence/source-before.json" "${process_expected[@]}"
go version
process_replacements=$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)
if [ -n "$process_replacements" ]; then echo "Process qualification refuses module replacements" >&2; exit 1; fi
go build -o "$process_dir/sinkhost" ./cmd/sinkhost
go version -m "$process_dir/sinkhost" | tee "$process_evidence/sinkhost.buildinfo.txt"
python3 "$root/source-provenance.py" artifact --repo "$root/.." \
  --source "$process_evidence/source-before.json" --binary "$process_dir/sinkhost" \
  --metadata "$process_evidence/sinkhost.buildinfo.txt" --output "$process_evidence/sinkhost.json"
python3 "$root/source-provenance.py" check --repo "$root/.." \
  --expected "$process_evidence/source-before.json" --output "$process_evidence/source-after.json"
go list -m -json all > "$process_evidence/modules.json"
govulncheck ./cmd/sinkhost
govulncheck -mode=binary "$process_dir/sinkhost"
docker run -d --name "$process_container" --memory=512m --memory-swap=512m --cpus=1 --pids-limit=128 \
  -p 127.0.0.1::5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17-alpine \
  -c max_connections=40 -c shared_buffers=64MB >/dev/null
for attempt in $(seq 1 30); do
  if docker exec "$process_container" pg_isready -U postgres >/dev/null 2>&1; then break; fi
  if [ "$attempt" = 30 ]; then echo "PostgreSQL readiness failed" >&2; exit 1; fi
  sleep 1
done
process_address=$(docker port "$process_container" 5432/tcp)
export DISPATCH_SINK_TEST_DSN="postgres://postgres@$process_address/postgres?sslmode=disable"
export DISPATCH_SINK_HOST_BINARY="$process_dir/sinkhost"
export DISPATCH_PROCESS_REQUIRED=1
# Configs and credentials remain in Go's private temporary directories. Only
# sanitized process logs and committed receipt evidence can use this directory.
export DISPATCH_SINK_EVIDENCE_DIR="${DISPATCH_SINK_EVIDENCE_DIR:-$process_dir/evidence}"
go test -race -count=1 -timeout=6m -v ./internal/sinkhost -run '^TestProcesses$'
docker exec "$process_container" sh -c 'cat /sys/fs/cgroup/memory.peak'
