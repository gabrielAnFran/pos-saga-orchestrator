# Rodando a stack completa do PDV localmente

Este arquivo compose sobe os 4 microsservicos, seus proprios bancos de
dados, e o RabbitMQ, para uma demonstracao local ponta a ponta da saga.

## Pre-requisitos

Clone os 4 repositorios como irmaos no disco:

```
pos/
├── pos-os-service
├── pos-billing-service
├── pos-production-service
└── pos-saga-orchestrator   <- este repositorio, rode o compose a partir daqui
```

## Subindo a stack

```bash
docker-compose -f deploy/local/docker-compose.yml up --build
```

UI de gerenciamento do RabbitMQ: http://localhost:15672 (guest/guest).
Portas dos servicos: os-service 8081, billing-service 8082,
production-service 8083, saga-orchestrator 8084, mp-mock 9999.

> Como documentado no arquivo compose, os Dockerfiles dos repositorios
> irmaos ainda nao tinham sido escritos na epoca em que este arquivo foi
> feito. Se a convencao real de `Dockerfile`/`TARGET` de um repositorio
> irmao for diferente do que se assume aqui, atualize o bloco `build:`
> daquele servico de acordo.

## Passo a passo do caminho feliz

```bash
# 1. Cria um pedido
curl -X POST http://localhost:8081/api/v1/orders -H 'Idempotency-Key: <uuid>' -H 'Content-Type: application/json' \
  -d '{"customer_id":"<uuid>","vehicle_id":"<uuid>","description":"Troca de óleo e revisão"}'

# 2. Acompanha o progresso da saga
curl http://localhost:8084/api/v1/sagas?os_id=<os_id>
# espera-se que o estado va de BUDGET_REQUESTED -> AWAITING_APPROVAL assim que BudgetGenerated disparar

# 3. Aprova o orcamento assim que BudgetGenerated tiver disparado
curl -X POST http://localhost:8082/api/v1/budgets/<budget_id>/approve -d '{"approved_by":"demo"}'

# 4. Simula o webhook do MercadoPago aprovando o pagamento
curl -X POST http://localhost:9999/simulate-webhook -d '{"preference_id":"<from budget/payment lookup>","status":"approved"}'

# 5. Progride a execucao
curl -X PATCH http://localhost:8083/api/v1/executions/<os_id> -d '{"status":"REPAIRING"}'
curl -X PATCH http://localhost:8083/api/v1/executions/<os_id> -d '{"status":"COMPLETED"}'

# 6. Confirma que a saga chegou a COMPLETED e que o pedido chegou a COMPLETED
curl http://localhost:8084/api/v1/sagas?os_id=<os_id>
curl http://localhost:8081/api/v1/orders/<os_id>
```

## Passo a passo de compensacao (falha de pagamento)

Igual ao anterior ate o passo 3, mas no passo 4 envie uma rejeicao em vez
de uma aprovacao:

```bash
curl -X POST http://localhost:9999/simulate-webhook -d '{"preference_id":"<...>","status":"rejected"}'
```

Acompanhe a saga passando por:

```
COMPENSATING -> CANCEL_BUDGET_REQUESTED -> CANCEL_OS_REQUESTED -> FAILED
```

via `curl http://localhost:8084/api/v1/sagas?os_id=<os_id>` (o campo
`history` de `GET /api/v1/sagas/:id` mostra cada transicao), e confirme
que o pedido termina como `CANCELLED` no OS Service.
