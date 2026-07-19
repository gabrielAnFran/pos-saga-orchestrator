package usecases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
)

type HandleEvent struct {
	Sagas      SagaRepository
	Processed  ProcessedEventRepository
	OSNotifier OSNotifier
}

func NewHandleEvent(sagas SagaRepository, processed ProcessedEventRepository, notifier OSNotifier) *HandleEvent {
	return &HandleEvent{Sagas: sagas, Processed: processed, OSNotifier: notifier}
}

// Handle processes one incoming domain event. It returns an error only
// for genuine infrastructure failures the caller should retry; duplicate
// events and out-of-order/invalid transitions are logged and swallowed.
func (h *HandleEvent) Handle(ctx context.Context, ev messaging.Event) error {
	eventID, err := uuid.Parse(ev.EventID)
	if err != nil {
		return fmt.Errorf("invalid event_id %q: %w", ev.EventID, err)
	}

	processed, err := h.Processed.IsProcessed(ctx, eventID)
	if err != nil {
		return fmt.Errorf("checking idempotency: %w", err)
	}
	if processed {
		slog.Info("event already processed, skipping", "event_id", ev.EventID, "event_name", ev.EventName)
		return nil
	}

	osID, err := extractOSID(ev.Payload)
	if err != nil {
		return fmt.Errorf("extracting os_id: %w", err)
	}

	if ev.EventName == saga.EventOSCreated {
		return h.handleOSCreated(ctx, osID, ev, eventID)
	}

	current, err := h.Sagas.FindByOSID(ctx, osID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			slog.Warn("no saga found for os_id, ignoring event", "os_id", osID, "event_name", ev.EventName)
			return h.Processed.MarkProcessed(ctx, eventID)
		}
		return fmt.Errorf("finding saga by os_id: %w", err)
	}

	return h.applyAndPersist(ctx, *current, ev, eventID)
}

func (h *HandleEvent) handleOSCreated(ctx context.Context, osID uuid.UUID, ev messaging.Event, eventID uuid.UUID) error {
	if existing, err := h.Sagas.FindByOSID(ctx, osID); err == nil && existing != nil {
		slog.Warn("duplicate OSCreated for existing saga, ignoring", "os_id", osID)
		return h.Processed.MarkProcessed(ctx, eventID)
	} else if err != nil && !errors.Is(err, db.ErrNotFound) {
		return fmt.Errorf("checking for existing saga: %w", err)
	}

	next, commands, err := saga.Apply(saga.SagaInstance{}, ev.EventName, ev.Payload)
	if err != nil {
		if errors.Is(err, saga.ErrInvalidTransition) {
			slog.Warn("invalid transition on OSCreated, ignoring", "os_id", osID)
			return h.Processed.MarkProcessed(ctx, eventID)
		}
		return fmt.Errorf("applying transition: %w", err)
	}

	next.ID = uuid.New()
	next.LastEventID = eventID

	outboxRows, err := buildOutboxRows(commands, ev.CorrelationID, next.ID, osID)
	if err != nil {
		return fmt.Errorf("building outbox rows: %w", err)
	}

	if err := h.Sagas.Create(ctx, next, outboxRows); err != nil {
		return fmt.Errorf("persisting new saga: %w", err)
	}
	slog.Info("saga created", "saga_id", next.ID, "os_id", osID, "state", next.State)
	h.syncOrderStatus(ctx, osID, next.State)
	return nil
}

// orderStatusForSagaState maps saga states to the OS Service order status
// they imply, for the best-effort post-commit sync in syncOrderStatus.
// States with no natural order-status counterpart (compensation substates
// already handled by OS Service's own CancelOSCommand consumer) are
// intentionally omitted.
var orderStatusForSagaState = map[string]string{
	saga.StateBudgetRequested:    "BUDGETING",
	saga.StateAwaitingApproval:   "AWAITING_APPROVAL",
	saga.StatePaymentRequested:   "PAYING",
	saga.StateExecutionRequested: "PAID",
	saga.StateInExecution:        "IN_EXECUTION",
	saga.StateCompleted:          "COMPLETED",
}

// syncOrderStatus best-effort walks the order forward to mirror the
// saga's new state. Saga state in saga_instances/saga_history remains
// the durable source of truth regardless of this call's outcome.
func (h *HandleEvent) syncOrderStatus(ctx context.Context, osID uuid.UUID, sagaState string) {
	target, ok := orderStatusForSagaState[sagaState]
	if !ok || h.OSNotifier == nil {
		return
	}
	if err := h.OSNotifier.SyncStatus(ctx, osID, target); err != nil {
		slog.Warn("failed to sync order status", "os_id", osID, "target", target, "error", err)
	}
}

func (h *HandleEvent) applyAndPersist(ctx context.Context, current saga.SagaInstance, ev messaging.Event, eventID uuid.UUID) error {
	next, commands, err := saga.Apply(current, ev.EventName, ev.Payload)
	if err != nil {
		if errors.Is(err, saga.ErrInvalidTransition) {
			slog.Warn("invalid/duplicate transition, ignoring", "saga_id", current.ID, "state", current.State, "event_name", ev.EventName)
			return h.Processed.MarkProcessed(ctx, eventID)
		}
		return fmt.Errorf("applying transition: %w", err)
	}

	outboxRows, err := buildOutboxRows(commands, ev.CorrelationID, current.ID, current.OSID)
	if err != nil {
		return fmt.Errorf("building outbox rows: %w", err)
	}

	if err := h.Sagas.ApplyTransition(ctx, current.ID, current.State, next, ev.EventName, eventID, outboxRows); err != nil {
		return fmt.Errorf("persisting transition: %w", err)
	}
	slog.Info("saga transitioned", "saga_id", current.ID, "from", current.State, "to", next.State, "event_name", ev.EventName)

	h.syncOrderStatus(ctx, current.OSID, next.State)

	return nil
}

func buildOutboxRows(commands []saga.CommandToEmit, correlationID string, sagaID, osID uuid.UUID) ([]db.OutboxEvent, error) {
	rows := make([]db.OutboxEvent, 0, len(commands))
	for _, cmd := range commands {
		event, err := messaging.NewEvent(cmd.Name, correlationID, sagaID.String(), cmd.Payload)
		if err != nil {
			return nil, err
		}
		payloadBytes, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		headersBytes, err := json.Marshal(map[string]any{"x-correlation-id": correlationID})
		if err != nil {
			return nil, err
		}
		eventID, err := uuid.Parse(event.EventID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, db.OutboxEvent{
			EventID:     eventID,
			AggregateID: osID,
			EventName:   cmd.Name,
			Payload:     payloadBytes,
			Headers:     headersBytes,
		})
	}
	return rows, nil
}

func extractOSID(payload json.RawMessage) (uuid.UUID, error) {
	var m struct {
		OSID string `json:"os_id"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return uuid.Nil, err
	}
	if m.OSID == "" {
		return uuid.Nil, fmt.Errorf("payload missing os_id")
	}
	return uuid.Parse(m.OSID)
}
