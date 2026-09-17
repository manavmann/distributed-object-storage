GO ?= go
BIN := bin
COMPOSE := docker compose -f deploy/docker-compose.yml

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
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

smoke:
	bash scripts/smoke.sh

demo:
	@echo "demo: not implemented yet"

bench:
	@echo "bench: not implemented yet"
