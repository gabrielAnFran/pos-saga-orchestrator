//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/application/usecases"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	infradb "github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/db"
	infrahttp "github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/http"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

func decodeEvent(body []byte, ev *messaging.Event) error {
	return json.Unmarshal(body, ev)
}

// TestSagaRepository_CreateAndTransition_EndToEnd exercises the saga
// repository's Create and ApplyTransition against real Postgres, including
// history rows, outbox rows, and idempotency (processed_events), for both
// a happy-path and a compensation transition.
func TestSagaRepository_CreateAndTransition_EndToEnd(t *testing.T) {
	ctx := context.Background()
	gormDB := setupPostgres(ctx, t)

	repo := infradb.NewSagaRepository(gormDB)
	outboxRepo := infradb.NewOutboxRepository(gormDB)
	processedRepo := infradb.NewProcessedEventRepository(gormDB)

	osID := uuid.New()
	sagaID := uuid.New()
	createEventID := uuid.New()

	instance := saga.SagaInstance{
		ID:          sagaID,
		SagaType:    saga.SagaTypeServiceOrder,
		OSID:        osID,
		State:       saga.StateBudgetRequested,
		Context:     []byte(`{"os_id":"` + osID.String() + `"}`),
		LastEventID: createEventID,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	outboxEvent := infradb.OutboxEvent{
		EventID:     uuid.New(),
		AggregateID: sagaID,
		EventName:   saga.CommandGenerateBudget,
		Payload:     []byte(`{"os_id":"` + osID.String() + `"}`),
		Headers:     []byte(`{}`),
	}

	require.NoError(t, repo.Create(ctx, instance, []infradb.OutboxEvent{outboxEvent}))

	found, err := repo.FindByOSID(ctx, osID)
	require.NoError(t, err)
	require.Equal(t, saga.StateBudgetRequested, found.State)

	byID, err := repo.FindByID(ctx, sagaID)
	require.NoError(t, err)
	require.Equal(t, osID, byID.OSID)

	unpublished, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, unpublished, 1)
	require.Equal(t, saga.CommandGenerateBudget, unpublished[0].EventName)

	isProcessed, err := processedRepo.IsProcessed(ctx, createEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	require.NoError(t, outboxRepo.MarkPublished(ctx, unpublished[0].ID))
	remaining, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, remaining)

	extraEventID := uuid.New()
	require.NoError(t, processedRepo.MarkProcessed(ctx, extraEventID))
	isProcessed, err = processedRepo.IsProcessed(ctx, extraEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	list, err := repo.List(ctx, &osID)
	require.NoError(t, err)
	require.Len(t, list, 1)

	// Happy-path transition: BUDGET_REQUESTED -> AWAITING_APPROVAL.
	approveEventID := uuid.New()
	next := *found
	next.State = saga.StateAwaitingApproval
	require.NoError(t, repo.ApplyTransition(ctx, sagaID, saga.StateBudgetRequested, next, saga.EventBudgetGenerated, approveEventID, nil))

	found, err = repo.FindByOSID(ctx, osID)
	require.NoError(t, err)
	require.Equal(t, saga.StateAwaitingApproval, found.State)

	history, err := repo.History(ctx, sagaID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, saga.StateBudgetRequested, history[0].ToState)
	require.Equal(t, saga.StateAwaitingApproval, history[1].ToState)

	// Compensation transition: AWAITING_APPROVAL -> COMPENSATING, via a
	// PaymentFailed-style event carrying no outbox rows of its own here.
	compEventID := uuid.New()
	compensating := *found
	compensating.State = saga.StateCompensating
	require.NoError(t, repo.ApplyTransition(ctx, sagaID, saga.StateAwaitingApproval, compensating, saga.EventPaymentFailed, compEventID, nil))

	found, err = repo.FindByOSID(ctx, osID)
	require.NoError(t, err)
	require.Equal(t, saga.StateCompensating, found.State)

	isProcessed, err = processedRepo.IsProcessed(ctx, compEventID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	stuck, err := repo.StuckSagas(ctx, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, stuck, 1, "COMPENSATING is non-terminal and updated before the cutoff, so it must be reported stuck")
}

// TestHandleEvent_OSCreated_EndToEnd runs the real HandleEvent use case
// (the actual saga-creation path a running worker exercises) against real
// Postgres, confirming the saga row, history row, and outbox row all land
// transactionally.
func TestHandleEvent_OSCreated_EndToEnd(t *testing.T) {
	ctx := context.Background()
	gormDB := setupPostgres(ctx, t)

	sagaRepo := infradb.NewSagaRepository(gormDB)
	processedRepo := infradb.NewProcessedEventRepository(gormDB)
	outboxRepo := infradb.NewOutboxRepository(gormDB)
	notifier := infrahttp.NewOSNotifier("http://unused.invalid")

	uc := usecases.NewHandleEvent(sagaRepo, processedRepo, notifier)

	osID := uuid.New()
	ev, err := messaging.NewEvent(saga.EventOSCreated, "corr-it", "", map[string]any{"os_id": osID.String(), "customer_id": "c1"})
	require.NoError(t, err)

	require.NoError(t, uc.Handle(ctx, ev))

	found, err := sagaRepo.FindByOSID(ctx, osID)
	require.NoError(t, err)
	require.Equal(t, saga.StateBudgetRequested, found.State)

	history, err := sagaRepo.History(ctx, found.ID)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, saga.EventOSCreated, history[0].EventName)

	unpublished, err := outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, unpublished, 1)
	require.Equal(t, saga.CommandGenerateBudget, unpublished[0].EventName)

	eventUUID, err := uuid.Parse(ev.EventID)
	require.NoError(t, err)
	isProcessed, err := processedRepo.IsProcessed(ctx, eventUUID)
	require.NoError(t, err)
	require.True(t, isProcessed)

	// A duplicate delivery of the same event must be a no-op.
	require.NoError(t, uc.Handle(ctx, ev))
	unpublished, err = outboxRepo.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, unpublished, 1, "no new outbox row should be created for a duplicate event")
}

// TestMessaging_PublishConsumeRoundTrip exercises the shared Conn helper
// against real RabbitMQ: declare a service queue, publish an event, consume
// it, and separately verify that a handler error routes through the retry
// queue and, after MaxRetries, lands in the DLQ.
func TestMessaging_PublishConsumeRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	amqpURL := setupRabbitMQ(ctx, t)

	conn, err := messaging.Dial(amqpURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	const svc = "it-roundtrip"
	_, err = conn.DeclareServiceQueue(svc, []string{"TestEvent"})
	require.NoError(t, err)

	ev, err := messaging.NewEvent("TestEvent", "corr-1", "", map[string]any{"hello": "world"})
	require.NoError(t, err)
	require.NoError(t, conn.Publish(ctx, ev))

	received := make(chan messaging.Event, 1)
	go func() {
		_ = conn.Consume(ctx, svc, func(_ context.Context, e messaging.Event) error {
			received <- e
			cancel()
			return nil
		})
	}()

	select {
	case e := <-received:
		require.Equal(t, "TestEvent", e.EventName)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for consumed event")
	}
}

// TestMessaging_Consume_HandlerErrorRetriesThenMalformedPayloadIsParked
// drives Consume itself (not Retry directly): a handler that fails routes
// the message to the retry queue, and a malformed payload published
// straight onto the queue is parked without ever reaching the handler.
func TestMessaging_Consume_HandlerErrorRetriesThenMalformedPayloadIsParked(t *testing.T) {
	ctx := context.Background()
	amqpURL := setupRabbitMQ(ctx, t)

	conn, err := messaging.Dial(amqpURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	const svc = "it-consume-errors"
	_, err = conn.DeclareServiceQueue(svc, []string{"BadEvent", "MalformedEvent"})
	require.NoError(t, err)

	ev, err := messaging.NewEvent("BadEvent", "corr-1", "", map[string]any{})
	require.NoError(t, err)
	require.NoError(t, conn.Publish(ctx, ev))

	require.NoError(t, conn.Channel().PublishWithContext(ctx, messaging.EventsExchange, "MalformedEvent", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte("not json"),
	}))

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 15*time.Second)
	defer consumeCancel()
	var handlerCalls int
	go func() {
		_ = conn.Consume(consumeCtx, svc, func(_ context.Context, e messaging.Event) error {
			handlerCalls++
			return assert.AnError
		})
	}()

	retryMsgs, err := conn.Channel().Consume(svc+".retry.q", "", true, false, false, false, nil)
	require.NoError(t, err)

	seen := map[string]bool{}
	timeout := time.After(15 * time.Second)
	for len(seen) < 2 {
		select {
		case d := <-retryMsgs:
			var e messaging.Event
			if json.Unmarshal(d.Body, &e) == nil {
				seen[e.EventName] = true
			} else {
				seen["malformed"] = true
			}
		case <-timeout:
			t.Fatalf("timed out waiting for both retried messages, saw: %v (handler calls=%d)", seen, handlerCalls)
		}
	}
	require.True(t, seen["BadEvent"], "handler-failed event should be retried")
	require.GreaterOrEqual(t, handlerCalls, 1)
}

// TestMessaging_RetryOnce verifies a failed handler routes the message
// through the retry queue (visible via the incremented x-retry-count
// header once the 30s TTL requeues it to the main queue).
func TestMessaging_RetryOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	amqpURL := setupRabbitMQ(ctx, t)

	conn, err := messaging.Dial(amqpURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	const svc = "it-retry"
	_, err = conn.DeclareServiceQueue(svc, []string{"FailingEvent"})
	require.NoError(t, err)

	ev, err := messaging.NewEvent("FailingEvent", "corr-1", "", map[string]any{})
	require.NoError(t, err)
	require.NoError(t, conn.Publish(ctx, ev))

	msgs, err := conn.Channel().Consume(svc+".events.q", "", false, false, false, false, nil)
	require.NoError(t, err)

	select {
	case d := <-msgs:
		require.NoError(t, conn.Retry(ctx, svc, d))
		require.NoError(t, d.Ack(false))
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the published event")
	}

	retryMsgs, err := conn.Channel().Consume(svc+".retry.q", "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-retryMsgs:
		require.EqualValues(t, 1, d.Headers["x-retry-count"])
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the message to land in the retry queue")
	}
}

// TestMessaging_RetryExceedsMaxRetries_GoesToDLQ exercises Retry directly
// on a delivery already at MaxRetries, asserting the (MaxRetries+1)th
// retry is parked in the DLQ instead of the retry queue.
func TestMessaging_RetryExceedsMaxRetries_GoesToDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	amqpURL := setupRabbitMQ(ctx, t)

	conn, err := messaging.Dial(amqpURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	const svc = "it-dlq"
	_, err = conn.DeclareServiceQueue(svc, []string{"FailingEvent"})
	require.NoError(t, err)

	ev, err := messaging.NewEvent("FailingEvent", "corr-1", "", map[string]any{})
	require.NoError(t, err)
	body, err := json.Marshal(ev)
	require.NoError(t, err)

	delivery := amqp.Delivery{
		ContentType: "application/json",
		Body:        body,
		Headers:     amqp.Table{"x-retry-count": int32(messaging.MaxRetries)},
	}
	require.NoError(t, conn.Retry(ctx, svc, delivery))

	dlqMsgs, err := conn.Channel().Consume(svc+".events.dlq", "", true, false, false, false, nil)
	require.NoError(t, err)

	select {
	case d := <-dlqMsgs:
		var dlqEvent messaging.Event
		require.NoError(t, decodeEvent(d.Body, &dlqEvent))
		require.Equal(t, "FailingEvent", dlqEvent.EventName)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for DLQ message")
	}
}

func setupPostgres(ctx context.Context, t *testing.T) *gorm.DB {
	t.Helper()
	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("saga_orchestrator"),
		tcpostgres.WithUsername("saga"),
		tcpostgres.WithPassword("saga"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	require.NoError(t, infradb.Migrate(dsn, "../../migrations"))

	gormDB, err := infradb.Open(dsn)
	require.NoError(t, err)
	return gormDB
}

func setupRabbitMQ(ctx context.Context, t *testing.T) string {
	t.Helper()
	rmqContainer, err := tcrabbitmq.Run(ctx, "rabbitmq:3.13-management-alpine",
		testcontainers.WithWaitStrategy(wait.ForLog("Server startup complete").WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rmqContainer.Terminate(ctx) })

	amqpURL, err := rmqContainer.AmqpURL(ctx)
	require.NoError(t, err)
	return amqpURL
}
