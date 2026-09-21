.PHONY: help up down clean logs ps topics scale smoke migrate tidy build test lint fmt

COMPOSE := docker compose -f deploy/docker-compose.yml

# Go is not installed on this machine, so every Go target below runs inside
# a throwaway golang:1.25 container with the repo bind-mounted at /src.
# Swap these for a local `go` invocation once Go is installed here.
#
# Debian-based golang:1.25, not -alpine: `go test -race` requires cgo, and the
# alpine image ships with CGO_ENABLED=0 and no C compiler. Race detection is
# not optional on a project whose correctness argument is about concurrency.
# MSYS_NO_PATHCONV=1 stops Git Bash on Windows rewriting the container-side
# paths (/src) into Windows paths before docker ever sees them. Harmless on
# Linux and macOS, where the variable is simply ignored.
GO_DOCKER := MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR)":/src -w /src golang:1.25

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'

up: ## Build images and start the stack detached
	$(COMPOSE) up --build -d

down: ## Stop the stack, keep volumes
	$(COMPOSE) down

clean: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

logs: ## Follow logs for all services
	$(COMPOSE) logs -f

ps: ## List running services
	$(COMPOSE) ps

# MSYS_NO_PATHCONV=1 for the same reason as GO_DOCKER above: Git Bash would
# otherwise rewrite the in-container /opt/kafka/... path into a Windows one.
# 12 partitions on jobs.ready caps worker parallelism for the autoscaling
# demo in Phase 9; see docs/plan.md.
topics: ## Create Kafka topics (safe to re-run)
	MSYS_NO_PATHCONV=1 $(COMPOSE) exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic jobs.ready --partitions 12 --replication-factor 3
	MSYS_NO_PATHCONV=1 $(COMPOSE) exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic jobs.dlq --partitions 3 --replication-factor 3

N := 1
scale: ## Scale the worker service, e.g. `make scale N=3` (default N=1)
	$(COMPOSE) up -d --scale worker=$(N) --no-recreate

smoke: ## Submit a job through the api and poll until the worker completes it
	curl -fsS http://localhost:8080/healthz > /dev/null
	curl -fsS http://localhost:8080/readyz > /dev/null
	job_id=$$(curl -fsS -X POST http://localhost:8080/jobs \
		-H 'Content-Type: application/json' \
		-d '{"type":"noop","payload":{}}' \
		| python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])'); \
	if [ -z "$$job_id" ]; then echo "smoke FAILED: no job_id in submit response"; exit 1; fi; \
	status=""; \
	for i in $$(seq 1 30); do \
		status=$$(curl -fsS http://localhost:8080/jobs/$$job_id | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'); \
		if [ "$$status" = "succeeded" ]; then echo "smoke OK"; exit 0; fi; \
		sleep 1; \
	done; \
	echo "smoke FAILED: job $$job_id last observed status: $$status"; \
	exit 1

migrate: ## Run the migrate service on its own, applying pending schema changes
	$(COMPOSE) run --rm migrate

tidy: ## Run `go mod tidy` via Docker
	$(GO_DOCKER) go mod tidy

build: ## Run `go build ./...` via Docker
	$(GO_DOCKER) go build ./...

test: ## Run `go test ./... -race` via Docker
	$(GO_DOCKER) go test ./... -race

lint: ## Run `go vet ./...` via Docker
	$(GO_DOCKER) go vet ./...

fmt: ## Run `gofmt -l .` via Docker (lists unformatted files)
	$(GO_DOCKER) gofmt -l .
