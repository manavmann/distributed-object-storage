GO ?= go
BIN := bin

.PHONY: build test lint up down logs smoke demo bench

build:
	@mkdir -p $(BIN)
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test -race ./...

lint:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "gofmt: run 'gofmt -w .'"; exit 1; }
	$(GO) vet ./...

up:
	@echo "up: not implemented yet"

down:
	@echo "down: not implemented yet"

logs:
	@echo "logs: not implemented yet"

smoke:
	@echo "smoke: not implemented yet"

demo:
	@echo "demo: not implemented yet"

bench:
	@echo "bench: not implemented yet"
