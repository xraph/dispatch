.PHONY: f l test
f:
	gofmt -w main.go
	goimports -w -local github.com/xraph/dispatch main.go
l:
	golangci-lint run --allow-serial-runners ./...
test:
	go test ./...
