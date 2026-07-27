# pos-saga-orchestrator

O microsservico Orquestrador de Saga para um sistema de PDV (POS) de oficina
mecanica, dividido em 4 servicos implantados de forma independente. Este
servico coordena o fluxo de trabalho distribuido que atravessa todos eles, e
e o dono do arquivo docker-compose local que sobe a stack inteira junto para
demonstracoes.

## Arquitetura

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

Cada servico e dono do seu proprio banco de dados (sem schema
compartilhado, sem joins entre servicos) e fala com os outros apenas
atraves de eventos no `pos.events`. Este repositorio, o orquestrador, e o
unico servico com um papel explicito de coordenacao: ele escuta todo
evento que os outros tres produzem e decide qual comando cada um deve
executar em seguida, incluindo comandos de compensacao quando algo falha
no meio do caminho.

Repositorios irmaos (clonados ao lado deste para demonstracoes locais,
veja `deploy/local/`): `pos-os-service`, `pos-billing-service`,
`pos-production-service`.

## Por que uma saga orquestrada

Veja `docs/adr/0001-orchestrated-saga.md` para o registro de decisao
completo. Resumo: uma maquina de estados explicita do orquestrador,
apoiada por um `saga_instances` durável + uma trilha de auditoria
imutavel em `saga_history`, da um unico lugar para ver em que estado o
fluxo de trabalho entre servicos de qualquer pedido esta e por que — o
que importa tanto para depurar um sistema real quanto para
demonstrar/avaliar este. A coreografia (cada servico reagindo aos
eventos do anterior sem um coordenador central) foi rejeitada porque a
logica de compensacao e a trilha de auditoria acabam espalhadas pelos
logs independentes de quatro servicos, em vez de ficarem em um so lugar.

## Fluxo de eventos

### Caminho feliz

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

### Caminho de compensacao (exemplo: pagamento recusado)

```
OSCreated -> BudgetGenerated -> BudgetApproved   (same as happy path)
PaymentFailed
  -> COMPENSATING, emits CancelBudgetCommand
BudgetCancelled
  -> CANCEL_OS_REQUESTED, emits CancelOSCommand
OSCancelled
  -> FAILED (terminal)
```

### Caminho de compensacao (exemplo: execucao falha apos o pagamento)

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

A tabela de transicao completa (cada linha `(state, event) -> (next_state,
commands)`) vive em `internal/domain/saga/state_machine.go` e e coberta
exaustivamente por `internal/domain/saga/state_machine_test.go`.

## Estrutura do repositorio

- `internal/domain/saga` — a maquina de estados pura (sem I/O).
- `internal/application/usecases` — conecta a maquina de estados a
  persistencia/mensageria (`handle_event.go`), o padrao outbox para
  mudanca de estado + publicacao de comando atomicas.
- `internal/infrastructure/{db,messaging,http,config}` — Postgres/GORM,
  RabbitMQ, o notificador do OS Service, configuracao de ambiente.
- `internal/presentation/handlers` — endpoints REST Gin somente leitura.
- `cmd/server` — API REST (`/api/v1/sagas`, health checks).
- `cmd/worker` — consome eventos de dominio, conduz a saga, roda o
  detector de sagas travadas (saga-tick).
- `cmd/outbox-dispatcher` — faz polling da tabela outbox e publica no
  RabbitMQ.
- `docs/adr` — registros de decisao de arquitetura.
- `docs/events` — JSON Schema de cada evento/comando trafegado.
- `docs/postman/collection.json` — colecao Postman cobrindo os endpoints
  REST dos 4 servicos, alem de duas pastas encadeadas (caminho feliz,
  compensacao) que capturam automaticamente `os_id`/`budget_id`/`saga_id`
  entre requisicoes via scripts de teste. Verificada ponta a ponta contra
  a stack docker-compose local via `newman`.
- `tests/bdd` — arquivos de feature godog que exercitam a maquina de
  estados pura.
- `deploy/local` — docker-compose para a stack local completa dos 4
  servicos.
- `charts/saga-orchestrator` — Helm chart (3 Deployments + Service +
  ConfigMap + Secret + HPA).

## API REST

- `GET /api/v1/sagas/:id` — instancia da saga + suas linhas de historico.
- `GET /api/v1/sagas?os_id=...` — lista sagas de um pedido.
- `GET /healthz`, `GET /readyz`

Sem endpoints de comando: este servico e guiado apenas por eventos/consultas.

## Variaveis de ambiente

| Variavel | Padrao | Descricao |
|---|---|---|
| `SAGA_PORT` | `8084` | Porta HTTP do `cmd/server` |
| `SAGA_DB_DSN` | `postgres://saga:saga@localhost:5435/saga_orchestrator?sslmode=disable` | DSN do Postgres |
| `SAGA_AMQP_URL` | `amqp://guest:guest@localhost:5672/` | URL do RabbitMQ |
| `SAGA_DISPATCH_INTERVAL_MS` | `500` | Intervalo de polling do outbox dispatcher |
| `OS_SERVICE_URL` | `http://os-service:8081` | URL base para a notificacao best-effort de conclusao |

## Testes

```bash
make test              # unit + use-case tests + BDD
make test-bdd           # godog BDD only
make test-integration   # testcontainers-go: real Postgres + RabbitMQ (build tag `integration`)
make coverage           # coverage on internal/domain/saga + internal/application/usecases
```

Cobertura medida em 2026-07-26 via `go test -tags=integration ./... -coverpkg=./...
-coverprofile=coverage.out` (unitarios + integracao juntos — o mesmo comando
que o job `test` do CI roda): **68,3%** do total de statements. Abaixo de
80% porque inclui `cmd/server`, `cmd/worker`, `cmd/outbox-dispatcher`
(wiring de `main()`, deliberadamente sem teste). O que importa para
corretude esta bem acima de 80%:

| Pacote | Cobertura | Como |
|---|---|---|
| `internal/domain/saga` (state machine) | 86.6% | unit — cada linha da tabela de transicao + 5 casos de transicao invalida |
| `internal/application/usecases` | 72.6% | unit |
| `internal/infrastructure/config` | 100% | unit |
| `internal/infrastructure/http` (`OSNotifier`) | 87.5% | unit (httptest) |
| `internal/presentation/handlers` | 100% | unit (httptest + fakes) |
| `internal/infrastructure/db` | dentro dos 68.3% totais | integracao (testcontainers) |
| `internal/infrastructure/messaging` | dentro dos 68.3% totais | integracao (testcontainers) |
| `cmd/*` | 0% | fora de escopo (wiring) |

`tests/integration/saga_flow_test.go` (testcontainers-go, Postgres +
RabbitMQ reais) cobre `SagaRepository.Create`/`ApplyTransition` (caminho
feliz + compensacao), `FindByOSID`/`FindByID`/`List`/`History`/`StuckSagas`,
os repositorios de outbox/eventos processados, o helper compartilhado
`messaging.Conn` (publish/consume/retry/DLQ), e `HandleEvent.Handle` ponta
a ponta para o caminho `OSCreated` → criacao de saga, incluindo entrega
duplicada idempotente. `go build ./...` e `go test -tags=integration ./...`
estao ambos verdes.

## Rodando localmente

Veja `deploy/local/README.md` para subir a stack completa dos 4 servicos
com docker-compose e um passo a passo em curl tanto do caminho feliz
quanto do caminho de compensacao por falha de pagamento.
