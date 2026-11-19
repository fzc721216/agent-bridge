.PHONY: build test check

build:
	go build -o bin/agent-bridge ./cmd/agent-bridge

test:
	go test ./...

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test -race ./...
