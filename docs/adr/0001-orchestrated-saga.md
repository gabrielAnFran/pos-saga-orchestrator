# ADR 0001: Orchestrated Saga for Cross-Service Service-Order Workflow

## Status

Accepted.

## Context

The mechanic-shop POS system is split into four independently deployed
microservices — OS Service (order intake), Billing Service (budgets and
payments), Production Service (execution/repair work), and this Saga
Orchestrator — each owning its own database, communicating exclusively
over an asynchronous message broker (RabbitMQ, topic exchange
`pos.events`). A single service order ("OS") must pass through all three
domain services in sequence (budget → payment → execution), and any
failure partway through (budget rejected, payment declined, execution
failed) must trigger a well-defined rollback across the services that
already committed work — cancelling a budget, refunding a payment,
cancelling the order.

This is a textbook distributed Saga problem: there is no distributed
transaction spanning Postgres/Mongo instances owned by different
services, so consistency has to be achieved through a sequence of local
transactions plus compensating actions, coordinated somehow.

For a graded, demoable university challenge, the coordination approach
also has to be easy to explain and to verify end-to-end: a reviewer
should be able to point at one place and say "this is the saga," and to
retrace exactly what happened for a given order.

## Decision

Use an **orchestrated saga**, not a choreographed one. This
Saga Orchestrator service owns an explicit state machine
(`internal/domain/saga/state_machine.go`) that is the single source of
truth for what state each order's saga is in, and is the only component
that decides what command to send next. It listens to every domain event
produced by the three sibling services and, in response, either updates
its own state and/or emits the next command in the flow, delivered back
onto `pos.events` via the outbox pattern.

## Alternatives Considered

**Choreography** (each service reacts to the previous service's events
and decides what to do next, with no central coordinator) was
considered and rejected for this challenge because:

- **Traceability**: there is no single place recording "what state is
  order X in right now" — a reviewer (or an on-call engineer) has to
  reconstruct saga progress by correlating events scattered across four
  services' logs/databases. The orchestrator's `saga_instances` +
  `saga_history` tables are that single place by construction.
- **Compensation logic is scattered**: each service would need to know
  not just its own compensating action but also *when* to trigger it
  based on events from services it doesn't otherwise talk to, coupling
  services more tightly than the message contracts suggest.
- **Harder to debug and demo**: for a graded assignment where the saga
  pattern itself is the thing being evaluated, an explicit state machine
  with an exhaustive transition-table unit test
  (`state_machine_test.go`) is much stronger evidence of a correct
  implementation than inferring correctness from four services' emergent
  behavior.

Choreography remains a reasonable choice for systems where the
orchestrator's centralization is itself the bigger risk; that tradeoff
didn't fit this project's goals.

## Consequences

- **The orchestrator is a coordination bottleneck / single point of
  failure** for saga progress: if it's down, in-flight orders don't
  advance (though the services they already talked to keep working
  normally, since RabbitMQ durably queues events for when the
  orchestrator's worker comes back).
  - Mitigated by keeping the orchestrator itself close to stateless:
    all durable state lives in `saga_instances`/`saga_history`/`outbox`
    in Postgres, not in memory, so a crashed or restarted worker/server
    resumes exactly where it left off by reading its queue and its DB.
  - The `cmd/server`, `cmd/worker`, and `cmd/outbox-dispatcher`
    processes can each be scaled horizontally behind the same queue;
    RabbitMQ delivers each event to exactly one consumer instance.
- **`saga_history` doubles as the audit log** referenced above: every
  transition (from_state, to_state, event, error) is appended
  immutably, which is exactly the artifact needed to demo/verify both
  the happy path and the compensation path end-to-end.
- **Known simplifications** (documented rather than silently omitted,
  scoped out for this challenge):
  - The `ExecutionCompleted → COMPLETED` transition makes a best-effort,
    fire-and-forget HTTP `PATCH` to OS Service after the saga's own
    state is already durably committed. A fully rigorous version would
    make that notification itself part of the compensable saga (e.g.
    via its own outbox entry and retry/backoff), but the saga's
    correctness does not depend on it succeeding — OS status is also
    independently derivable — so a synchronous best-effort call was
    judged sufficient here.
  - The worker's saga-tick loop only *detects and logs* sagas stuck in
    a non-terminal state for more than two minutes; it does not
    automatically trigger compensation. A production system would want
    per-state timeouts with automatic compensating actions; that's a
    natural follow-up, not implemented to keep the challenge's scope
    bounded.

## Implementation

See `internal/domain/saga/state_machine.go` for the transition table and
`internal/domain/saga/instance.go` for the pure `Apply` function that is
the actual saga logic; `internal/application/usecases/handle_event.go`
wires it to persistence (outbox pattern for atomic state-change +
command-publish) and messaging. `docs/events/*.schema.json` documents the
wire contract for every event/command the orchestrator consumes or
produces.
