# ADR 0001: Saga Orquestrada para o Fluxo de Ordem de Servico Entre Servicos

## Status

Aceito.

## Contexto

O sistema de PDV da oficina mecanica esta dividido em quatro
microsservicos implantados de forma independente — OS Service (entrada
de pedidos), Billing Service (orcamentos e pagamentos), Production
Service (execucao/trabalho de reparo) e este Saga Orchestrator — cada um
dono do seu proprio banco de dados, comunicando-se exclusivamente atraves
de um broker de mensagens assincrono (RabbitMQ, topic exchange
`pos.events`). Uma unica ordem de servico ("OS") precisa passar pelos
tres servicos de dominio em sequencia (orcamento → pagamento →
execucao), e qualquer falha no meio do caminho (orcamento rejeitado,
pagamento recusado, execucao falhada) precisa disparar um rollback bem
definido nos servicos que ja comprometeram trabalho — cancelando um
orcamento, estornando um pagamento, cancelando o pedido.

Este e um problema classico de Saga distribuida: nao existe uma
transacao distribuida abrangendo as instancias de Postgres/Mongo
pertencentes a servicos diferentes, entao a consistencia precisa ser
alcancada atraves de uma sequencia de transacoes locais mais acoes de
compensacao, coordenadas de alguma forma.

Para um desafio universitario avaliado e demonstravel, a abordagem de
coordenacao tambem precisa ser facil de explicar e verificar ponta a
ponta: um avaliador deve conseguir apontar para um unico lugar e dizer
"isso e a saga", e reconstituir exatamente o que aconteceu para um
determinado pedido.

## Decisao

Usar uma **saga orquestrada**, nao uma coreografada. Este servico Saga
Orchestrator e dono de uma maquina de estados explicita
(`internal/domain/saga/state_machine.go`) que e a unica fonte de verdade
sobre em que estado a saga de cada pedido esta, e e o unico componente
que decide qual comando enviar em seguida. Ele escuta todo evento de
dominio produzido pelos tres servicos irmaos e, em resposta, atualiza o
proprio estado e/ou emite o proximo comando do fluxo, entregue de volta
ao `pos.events` via padrao outbox.

## Alternativas Consideradas

**Coreografia** (cada servico reage aos eventos do servico anterior e
decide o que fazer em seguida, sem um coordenador central) foi
considerada e rejeitada para este desafio porque:

- **Rastreabilidade**: nao existe um unico lugar registrando "em que
  estado esta o pedido X agora" — um avaliador (ou um engenheiro de
  plantao) precisa reconstruir o progresso da saga correlacionando
  eventos espalhados pelos logs/bancos de dados de quatro servicos. As
  tabelas `saga_instances` + `saga_history` do orquestrador sao esse
  unico lugar por construcao.
- **Logica de compensacao espalhada**: cada servico precisaria conhecer
  nao so a sua propria acao de compensacao, mas tambem *quando*
  dispara-la com base em eventos de servicos com os quais nao se
  comunica de outra forma, acoplando os servicos mais fortemente do que
  os contratos de mensagem sugerem.
- **Mais dificil de depurar e demonstrar**: para um trabalho avaliado
  onde o proprio padrao saga e a coisa sendo avaliada, uma maquina de
  estados explicita com um teste unitario exaustivo da tabela de
  transicao (`state_machine_test.go`) e uma evidencia muito mais forte
  de uma implementacao correta do que inferir a corretude a partir do
  comportamento emergente de quatro servicos.

A coreografia continua sendo uma escolha razoavel para sistemas onde a
centralizacao do orquestrador e ela mesma o maior risco; esse trade-off
nao se encaixava nos objetivos deste projeto.

## Consequencias

- **O orquestrador e um gargalo de coordenacao / ponto unico de falha**
  para o progresso da saga: se ele estiver fora do ar, pedidos em
  andamento nao avancam (embora os servicos com os quais ja falaram
  continuem funcionando normalmente, ja que o RabbitMQ enfileira os
  eventos de forma durável ate o worker do orquestrador voltar).
  - Mitigado mantendo o proprio orquestrador proximo de stateless: todo
    estado durável vive em `saga_instances`/`saga_history`/`outbox` no
    Postgres, nao em memoria, entao um worker/servidor que travou ou foi
    reiniciado retoma exatamente de onde parou lendo sua fila e seu
    banco.
  - Os processos `cmd/server`, `cmd/worker` e `cmd/outbox-dispatcher`
    podem cada um ser escalados horizontalmente atras da mesma fila; o
    RabbitMQ entrega cada evento a exatamente uma instancia consumidora.
- **`saga_history` funciona tambem como o log de auditoria** mencionado
  acima: cada transicao (from_state, to_state, event, error) e anexada
  de forma imutavel, que e exatamente o artefato necessario para
  demonstrar/verificar tanto o caminho feliz quanto o caminho de
  compensacao ponta a ponta.
- **Simplificacoes conhecidas** (documentadas em vez de omitidas
  silenciosamente, fora de escopo para este desafio):
  - A transicao `ExecutionCompleted → COMPLETED` faz um `PATCH` HTTP
    best-effort e fire-and-forget para o OS Service depois que o proprio
    estado da saga ja foi comprometido de forma durável. Uma versao
    totalmente rigorosa faria dessa notificacao parte da propria saga
    compensavel (por exemplo, via sua propria entrada de outbox e
    retry/backoff), mas a corretude da saga nao depende dela ter
    sucesso — o status da OS tambem e derivavel de forma independente —
    entao uma chamada sincrona best-effort foi considerada suficiente
    aqui.
  - O loop de saga-tick do worker apenas *detecta e loga* sagas presas
    em um estado nao terminal por mais de dois minutos; ele nao dispara
    compensacao automaticamente. Um sistema em producao gostaria de
    timeouts por estado com acoes de compensacao automaticas; isso e um
    proximo passo natural, nao implementado para manter o escopo do
    desafio limitado.

## Implementacao

Veja `internal/domain/saga/state_machine.go` para a tabela de transicao
e `internal/domain/saga/instance.go` para a funcao `Apply` pura que e a
logica real da saga; `internal/application/usecases/handle_event.go` a
conecta a persistencia (padrao outbox para mudanca de estado +
publicacao de comando atomicas) e mensageria. `docs/events/*.schema.json`
documenta o contrato de comunicacao de cada evento/comando que o
orquestrador consome ou produz.
