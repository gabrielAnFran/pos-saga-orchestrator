# Running the full POS stack locally

This compose file brings up all 4 microservices, their own databases, and
RabbitMQ, for a local end-to-end demo of the saga.

## Prerequisites

Clone the 4 repos as siblings on disk:

```
pos/
├── pos-os-service
├── pos-billing-service
├── pos-production-service
└── pos-saga-orchestrator   <- this repo, run compose from here
```

## Bring the stack up

```bash
docker-compose -f deploy/local/docker-compose.yml up --build
```

RabbitMQ management UI: http://localhost:15672 (guest/guest).
Service ports: os-service 8081, billing-service 8082,
production-service 8083, saga-orchestrator 8084, mp-mock 9999.

> As documented in the compose file, the sibling repos' Dockerfiles
> hadn't been written yet at the time this file was authored. If a
> sibling repo's actual `Dockerfile`/`TARGET` convention differs from
> what's assumed here, update that service's `build:` block accordingly.

## Happy path walkthrough

```bash
# 1. Create an order
curl -X POST http://localhost:8081/api/v1/orders -H 'Idempotency-Key: <uuid>' -H 'Content-Type: application/json' \
  -d '{"customer_id":"<uuid>","vehicle_id":"<uuid>","description":"Troca de óleo e revisão"}'

# 2. Watch the saga progress
curl http://localhost:8084/api/v1/sagas?os_id=<os_id>
# expect state to move BUDGET_REQUESTED -> AWAITING_APPROVAL once BudgetGenerated fires

# 3. Approve the budget once BudgetGenerated has fired
curl -X POST http://localhost:8082/api/v1/budgets/<budget_id>/approve -d '{"approved_by":"demo"}'

# 4. Simulate the MercadoPago webhook approving payment
curl -X POST http://localhost:9999/simulate-webhook -d '{"preference_id":"<from budget/payment lookup>","status":"approved"}'

# 5. Progress execution
curl -X PATCH http://localhost:8083/api/v1/executions/<os_id> -d '{"status":"REPAIRING"}'
curl -X PATCH http://localhost:8083/api/v1/executions/<os_id> -d '{"status":"COMPLETED"}'

# 6. Confirm the saga reached COMPLETED and the order reached COMPLETED
curl http://localhost:8084/api/v1/sagas?os_id=<os_id>
curl http://localhost:8081/api/v1/orders/<os_id>
```

## Compensation (payment failure) walkthrough

Same as above through step 3, then at step 4 send a rejection instead:

```bash
curl -X POST http://localhost:9999/simulate-webhook -d '{"preference_id":"<...>","status":"rejected"}'
```

Watch the saga walk through:

```
COMPENSATING -> CANCEL_BUDGET_REQUESTED -> CANCEL_OS_REQUESTED -> FAILED
```

via `curl http://localhost:8084/api/v1/sagas?os_id=<os_id>` (the `history`
field of `GET /api/v1/sagas/:id` shows every transition), and confirm the
order ends up `CANCELLED` on OS Service.
