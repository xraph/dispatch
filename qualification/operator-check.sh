#!/usr/bin/env bash
set -euo pipefail
operator_root=$(cd "$(dirname "$0")" && pwd)
operator_dir=$(mktemp -d)
operator_container="dispatch-operator-qualification-$$"
cleanup() {
  docker rm -f -v "$operator_container" >/dev/null 2>&1 || true
  rm -rf "$operator_dir"
}
trap cleanup EXIT
export GOWORK=off GOTOOLCHAIN=go1.26.10
cd "$operator_root"

# Metadata survives private binary and credential cleanup, including failed runs.
operator_evidence=$(mktemp -d "${DISPATCH_QUALIFICATION_EVIDENCE_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}}/dispatch-operator-evidence.XXXXXX")
printf 'Qualification evidence: %s\n' "$operator_evidence"
operator_expected=()
if [ -n "${DISPATCH_QUALIFICATION_SOURCE_MANIFEST:-}" ]; then
  operator_expected=(--expected "$DISPATCH_QUALIFICATION_SOURCE_MANIFEST")
fi
python3 "$operator_root/source-provenance.py" check --repo "$operator_root/.." \
  --output "$operator_evidence/source-before.json" "${operator_expected[@]}"
go version
operator_replacements=$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)
if [ -n "$operator_replacements" ]; then echo "Operator qualification refuses replacements" >&2; exit 1; fi
go build -o "$operator_dir/operatorhost" ./cmd/operatorhost
go version -m "$operator_dir/operatorhost" | tee "$operator_evidence/operatorhost.buildinfo.txt"
python3 "$operator_root/source-provenance.py" artifact --repo "$operator_root/.." \
  --source "$operator_evidence/source-before.json" --binary "$operator_dir/operatorhost" \
  --metadata "$operator_evidence/operatorhost.buildinfo.txt" --output "$operator_evidence/operatorhost.json"
go build -o "$operator_dir/sinkhost" ./cmd/sinkhost
go version -m "$operator_dir/sinkhost" | tee "$operator_evidence/sinkhost.buildinfo.txt"
python3 "$operator_root/source-provenance.py" artifact --repo "$operator_root/.." \
  --source "$operator_evidence/source-before.json" --binary "$operator_dir/sinkhost" \
  --metadata "$operator_evidence/sinkhost.buildinfo.txt" --output "$operator_evidence/sinkhost.json"
python3 "$operator_root/source-provenance.py" check --repo "$operator_root/.." \
  --expected "$operator_evidence/source-before.json" --output "$operator_evidence/source-after.json"
go list -m -json all > "$operator_evidence/modules.json"
govulncheck -test ./...
govulncheck ./cmd/operatorhost
govulncheck -mode=binary "$operator_dir/operatorhost"
govulncheck ./cmd/sinkhost
govulncheck -mode=binary "$operator_dir/sinkhost"
docker run -d --name "$operator_container" --memory=512m --memory-swap=512m --cpus=1 --pids-limit=128 \
  -p 127.0.0.1::5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17-alpine \
  -c max_connections=40 -c shared_buffers=64MB >/dev/null
for attempt in $(seq 1 30); do
  if docker exec "$operator_container" pg_isready -U postgres >/dev/null 2>&1; then break; fi
  if [ "$attempt" = 30 ]; then echo "PostgreSQL readiness failed" >&2; exit 1; fi
  sleep 1
done
operator_address=$(docker port "$operator_container" 5432/tcp)
export DISPATCH_OPERATOR_DSN="postgres://postgres@$operator_address/postgres?sslmode=disable"
export DISPATCH_OPERATOR_BINARY="$operator_dir/operatorhost"
export DISPATCH_SINK_HOST_BINARY="$operator_dir/sinkhost"
export DISPATCH_OPERATOR_REQUIRED=1
export GOMEMLIMIT=128MiB
go test -race -count=1 -timeout=3m -v ./internal/operatorhost
docker exec "$operator_container" sh -c 'cat /sys/fs/cgroup/memory.peak'
