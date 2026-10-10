.PHONY: f l test
f:
	gofmt -w main.go
l:
	golangci-lint run --allow-serial-runners ./...
test:
	go test ./...
