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
export GOWORK=off GOTOOLCHAIN=go1.26.9
cd "$operator_root"
go version
operator_replacements=$(go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)
if [ -n "$operator_replacements" ]; then echo "Operator qualification refuses replacements" >&2; exit 1; fi
go build -o "$operator_dir/operatorhost" ./cmd/operatorhost
go version -m "$operator_dir/operatorhost"
govulncheck -test ./...
govulncheck ./cmd/operatorhost
govulncheck -mode=binary "$operator_dir/operatorhost"
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
export DISPATCH_OPERATOR_REQUIRED=1
export GOMEMLIMIT=128MiB
go test -race -count=1 -timeout=3m -v ./internal/operatorhost
docker exec "$operator_container" sh -c 'cat /sys/fs/cgroup/memory.peak'
