.PHONY: build test test-unit test-bdd coverage lint docker-server docker-worker docker-dispatcher run-server run-worker run-dispatcher tidy

build:
	go build ./...

test: test-unit test-bdd

test-unit:
	go test ./internal/... ./cmd/...

test-bdd:
	go test ./tests/bdd/...

coverage:
	go test ./internal/domain/saga/... ./internal/application/usecases/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

lint:
	go vet ./...

tidy:
	go mod tidy

docker-server:
	docker build --build-arg TARGET=server -t pos-saga-orchestrator:server .

docker-worker:
	docker build --build-arg TARGET=worker -t pos-saga-orchestrator:worker .

docker-dispatcher:
	docker build --build-arg TARGET=outbox-dispatcher -t pos-saga-orchestrator:dispatcher .

run-server:
	go run ./cmd/server

run-worker:
	go run ./cmd/worker

run-dispatcher:
	go run ./cmd/outbox-dispatcher
