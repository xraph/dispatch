export GOWORK := off
export GOTOOLCHAIN := go1.26.9
export GOMAXPROCS := 2
export GOFLAGS := -p=1

.PHONY: f l test check
f:
	goimports -local github.com/xraph/dispatch -w $$(rg --files . -g '*.go')
l:
	golangci-lint run --allow-serial-runners --max-issues-per-linter=0 --max-same-issues=0 ./...
test:
	WRITERFLOOR_REQUIRED=1 go test -race -count=1 -timeout=2m ./...
check:
	go mod tidy -diff
	go mod verify
	go vet ./...
	$(MAKE) test
	go build ./...
