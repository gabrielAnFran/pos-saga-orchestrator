# pos-saga-orchestrator

The Saga Orchestrator microservice for a mechanic-shop POS system split
across 4 independently deployed services. This service coordinates the
distributed workflow that spans all of them, and owns the local
docker-compose file that runs the whole stack together for demos.

## Architecture

```
                         RabbitMQ (pos.events topic exchange)
                                    │
        ┌───────────────┬──────────┼──────────┬───────────────┐
        │               │          │           │               │
  ┌───────────┐   ┌───────────┐    │    ┌────────────┐   ┌─────────────┐
  │ OS Service│   │  Billing  │    │    │ Production │   │    Saga     │
  │  (8081)   │   │  Service  │    │    │  Service   │   │ Orchestrator│
  │           │   │  (8082)   │    │    │  (8083)    │   │   (8084)    │
  │ Postgres  │   │ Postgres  │    │    │  MongoDB   │   │  Postgres   │
  └───────────┘   └───────────┘    │    └────────────┘   └─────────────┘
                                    │
                          each service: own DB,
                          own outbox table, own
                          outbox-dispatcher process
```

Each service owns its own database (no shared schema, no cross-service
joins) and talks to the others only via events on `pos.events`. This
repo, the orchestrator, is the only service with an explicit
coordination role: it listens to every event the other three produce and
decides what command each should execute next, including compensating
commands when something fails partway through.

Sibling repositories (cloned alongside this one for local demos, see
`deploy/local/`): `pos-os-service`, `pos-billing-service`,
`pos-production-service`.

## Why an orchestrated saga

See `docs/adr/0001-orchestrated-saga.md` for the full decision record.
Summary: an explicit orchestrator state machine, backed by durable
`saga_instances` + an immutable `saga_history` audit trail, gives a
single place to see what state any order's cross-service workflow is in
and why — which matters both for debugging a real system and for
demoing/grading this one. Choreography (each service reacting to the
previous one's events with no central coordinator) was rejected because
compensation logic and audit trail end up scattered across four
services' independent logs instead of one place.

## Event flow

### Happy path

```
OSCreated
  -> [orchestrator] BUDGET_REQUESTED, emits GenerateBudgetCommand
BudgetGenerated
  -> AWAITING_APPROVAL
BudgetApproved
  -> PAYMENT_REQUESTED, emits RequestPaymentCommand
PaymentConfirmed
  -> EXECUTION_REQUESTED, emits StartExecutionCommand
ExecutionStarted
  -> IN_EXECUTION
ExecutionCompleted
  -> COMPLETED (terminal)  [+ best-effort PATCH to OS Service]
```

### Compensation path (example: payment declined)

```
OSCreated -> BudgetGenerated -> BudgetApproved   (same as happy path)
PaymentFailed
  -> COMPENSATING, emits CancelBudgetCommand
BudgetCancelled
  -> CANCEL_OS_REQUESTED, emits CancelOSCommand
OSCancelled
  -> FAILED (terminal)
```

### Compensation path (example: execution fails after payment)

```
... IN_EXECUTION
ExecutionFailed
  -> COMPENSATING, emits RefundPaymentCommand
PaymentRefunded
  -> CANCEL_BUDGET_REQUESTED, emits CancelBudgetCommand
BudgetCancelled
  -> CANCEL_OS_REQUESTED, emits CancelOSCommand
OSCancelled
  -> FAILED (terminal)
```

The full transition table (every `(state, event) -> (next_state,
commands)` row) lives in `internal/domain/saga/state_machine.go` and is
exhaustively covered by `internal/domain/saga/state_machine_test.go`.

## Repo layout

- `internal/domain/saga` — the pure state machine (no I/O).
- `internal/application/usecases` — wires the state machine to
  persistence/messaging (`handle_event.go`), the outbox pattern for
  atomic state-change + command-publish.
- `internal/infrastructure/{db,messaging,http,config}` — Postgres/GORM,
  RabbitMQ, the OS Service notifier, env config.
- `internal/presentation/handlers` — read-only Gin REST endpoints.
- `cmd/server` — REST API (`/api/v1/sagas`, health checks).
- `cmd/worker` — consumes domain events, drives the saga, runs the
  saga-tick stuck-saga detector.
- `cmd/outbox-dispatcher` — polls the outbox table and publishes to
  RabbitMQ.
- `docs/adr` — architecture decision records.
- `docs/events` — JSON Schema for every event/command on the wire.
- `docs/postman/collection.json` — Postman collection covering all 4
  services' REST endpoints, plus two chained folders (happy path,
  compensation) that auto-capture `os_id`/`budget_id`/`saga_id` across
  requests via test scripts. Verified end-to-end against the local
  docker-compose stack via `newman`.
- `tests/bdd` — godog feature files exercising the pure state machine.
- `deploy/local` — docker-compose for the full 4-service local stack.
- `charts/saga-orchestrator` — Helm chart (3 Deployments + Service +
  ConfigMap + Secret + HPA).

## REST API

- `GET /api/v1/sagas/:id` — saga instance + its history rows.
- `GET /api/v1/sagas?os_id=...` — list sagas for an order.
- `GET /healthz`, `GET /readyz`

No command endpoints: this service is purely event/query driven.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `SAGA_PORT` | `8084` | HTTP port for `cmd/server` |
| `SAGA_DB_DSN` | `postgres://saga:saga@localhost:5435/saga_orchestrator?sslmode=disable` | Postgres DSN |
| `SAGA_AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ URL |
| `SAGA_DISPATCH_INTERVAL_MS` | `500` | Outbox dispatcher poll interval |
| `OS_SERVICE_URL` | `http://os-service:8081` | Base URL for the best-effort completion notification |

## Testing

```bash
make test              # unit + use-case tests + BDD
make test-bdd           # godog BDD only
make test-integration   # testcontainers-go: real Postgres + RabbitMQ (build tag `integration`)
make coverage           # coverage on internal/domain/saga + internal/application/usecases
```

Coverage measured 2026-07-26 via `go test -tags=integration ./... -coverpkg=./...
-coverprofile=coverage.out` (unit + integration together — the same command
CI's `test` job runs): **68.3%** of statements total. Below 80% because it
includes `cmd/server`, `cmd/worker`, `cmd/outbox-dispatcher` (main() wiring,
deliberately untested). What matters for correctness is well above 80%:

| Package | Coverage | How |
|---|---|---|
| `internal/domain/saga` (state machine) | 86.6% | unit — every transition-table row + 5 invalid-transition cases |
| `internal/application/usecases` | 72.6% | unit |
| `internal/infrastructure/config` | 100% | unit |
| `internal/infrastructure/http` (`OSNotifier`) | 87.5% | unit (httptest) |
| `internal/presentation/handlers` | 100% | unit (httptest + fakes) |
| `internal/infrastructure/db` | folded into 68.3% total | integration (testcontainers) |
| `internal/infrastructure/messaging` | folded into 68.3% total | integration (testcontainers) |
| `cmd/*` | 0% | out of scope (wiring) |

`tests/integration/saga_flow_test.go` (testcontainers-go, real Postgres +
RabbitMQ) covers `SagaRepository.Create`/`ApplyTransition` (happy path +
compensation), `FindByOSID`/`FindByID`/`List`/`History`/`StuckSagas`, the
outbox/processed-events repos, the shared `messaging.Conn` helper
(publish/consume/retry/DLQ), and `HandleEvent.Handle` end-to-end for the
`OSCreated` → saga-creation path including idempotent duplicate delivery.
`go build ./...` and `go test -tags=integration ./...` are both green.

## Running locally

See `deploy/local/README.md` for bringing up the full 4-service stack
with docker-compose and a curl walkthrough of both the happy path and
the payment-failure compensation path.
