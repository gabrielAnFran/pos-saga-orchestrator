package usecases

import (
	"context"
	"testing"

	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/db"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSagaRepo is a hand-rolled in-memory double: no real DB, keyed by
// os_id like the real repository's unique lookup path.
type fakeSagaRepo struct {
	byOSID map[uuid.UUID]*saga.SagaInstance
	byID   map[uuid.UUID]*saga.SagaInstance
	outbox []db.OutboxEvent
}

func newFakeSagaRepo() *fakeSagaRepo {
	return &fakeSagaRepo{
		byOSID: map[uuid.UUID]*saga.SagaInstance{},
		byID:   map[uuid.UUID]*saga.SagaInstance{},
	}
}

func (f *fakeSagaRepo) Create(ctx context.Context, s saga.SagaInstance, outboxRows []db.OutboxEvent) error {
	cp := s
	f.byOSID[s.OSID] = &cp
	f.byID[s.ID] = &cp
	f.outbox = append(f.outbox, outboxRows...)
	return nil
}

func (f *fakeSagaRepo) ApplyTransition(ctx context.Context, sagaID uuid.UUID, fromState string, next saga.SagaInstance, eventName string, eventID uuid.UUID, outboxRows []db.OutboxEvent) error {
	cp := next
	f.byOSID[next.OSID] = &cp
	f.byID[sagaID] = &cp
	f.outbox = append(f.outbox, outboxRows...)
	return nil
}

func (f *fakeSagaRepo) FindByOSID(ctx context.Context, osID uuid.UUID) (*saga.SagaInstance, error) {
	s, ok := f.byOSID[osID]
	if !ok {
		return nil, db.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (f *fakeSagaRepo) FindByID(ctx context.Context, id uuid.UUID) (*saga.SagaInstance, error) {
	s, ok := f.byID[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (f *fakeSagaRepo) List(ctx context.Context, osIDFilter *uuid.UUID) ([]saga.SagaInstance, error) {
	var out []saga.SagaInstance
	for _, s := range f.byID {
		out = append(out, *s)
	}
	return out, nil
}

type fakeProcessedRepo struct {
	processed map[uuid.UUID]bool
}

func newFakeProcessedRepo() *fakeProcessedRepo {
	return &fakeProcessedRepo{processed: map[uuid.UUID]bool{}}
}

func (f *fakeProcessedRepo) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	return f.processed[eventID], nil
}

func (f *fakeProcessedRepo) MarkProcessed(ctx context.Context, eventID uuid.UUID) error {
	f.processed[eventID] = true
	return nil
}

type fakeNotifier struct {
	calledFor    []uuid.UUID
	syncedStates []string
}

func (f *fakeNotifier) NotifyCompleted(ctx context.Context, osID uuid.UUID) error {
	f.calledFor = append(f.calledFor, osID)
	return nil
}

func (f *fakeNotifier) SyncStatus(ctx context.Context, osID uuid.UUID, target string) error {
	f.calledFor = append(f.calledFor, osID)
	f.syncedStates = append(f.syncedStates, target)
	return nil
}

func newHandler() (*HandleEvent, *fakeSagaRepo, *fakeProcessedRepo, *fakeNotifier) {
	sagas := newFakeSagaRepo()
	processed := newFakeProcessedRepo()
	notifier := &fakeNotifier{}
	return NewHandleEvent(sagas, processed, notifier), sagas, processed, notifier
}

func mustEvent(t *testing.T, name, correlationID string, payload map[string]any) messaging.Event {
	t.Helper()
	ev, err := messaging.NewEvent(name, correlationID, "", payload)
	require.NoError(t, err)
	return ev
}

func TestHandle_OSCreated_CreatesNewSaga(t *testing.T) {
	h, sagas, _, _ := newHandler()
	osID := uuid.New()
	ev := mustEvent(t, saga.EventOSCreated, "corr-1", map[string]any{"os_id": osID.String(), "customer_id": "c1"})

	err := h.Handle(context.Background(), ev)
	require.NoError(t, err)

	s, err := sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateBudgetRequested, s.State)
	require.Len(t, sagas.outbox, 1)
	assert.Equal(t, saga.CommandGenerateBudget, sagas.outbox[0].EventName)
}

func TestHandle_HappyPath_ReachesCompleted(t *testing.T) {
	h, sagas, _, notifier := newHandler()
	osID := uuid.New()
	budgetID := uuid.New().String()
	paymentID := uuid.New().String()

	steps := []messaging.Event{
		mustEvent(t, saga.EventOSCreated, "c", map[string]any{"os_id": osID.String()}),
		mustEvent(t, saga.EventBudgetGenerated, "c", map[string]any{"os_id": osID.String(), "budget_id": budgetID, "amount_cents": 1000}),
		mustEvent(t, saga.EventBudgetApproved, "c", map[string]any{"os_id": osID.String(), "budget_id": budgetID}),
		mustEvent(t, saga.EventPaymentConfirmed, "c", map[string]any{"os_id": osID.String(), "payment_id": paymentID}),
		mustEvent(t, saga.EventExecutionStarted, "c", map[string]any{"os_id": osID.String()}),
		mustEvent(t, saga.EventExecutionCompleted, "c", map[string]any{"os_id": osID.String()}),
	}

	for _, ev := range steps {
		require.NoError(t, h.Handle(context.Background(), ev))
	}

	s, err := sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCompleted, s.State)
	// Notifier is called once per saga transition that has an order-status
	// mapping (BUDGET_REQUESTED, AWAITING_APPROVAL, PAYMENT_REQUESTED,
	// EXECUTION_REQUESTED, IN_EXECUTION, COMPLETED), always for this osID,
	// ending with the order synced to COMPLETED.
	require.NotEmpty(t, notifier.calledFor)
	for _, id := range notifier.calledFor {
		assert.Equal(t, osID, id)
	}
	assert.Equal(t, "COMPLETED", notifier.syncedStates[len(notifier.syncedStates)-1])

	// Every non-terminal step in the happy path except BudgetGenerated,
	// ExecutionStarted and ExecutionCompleted emits exactly one command.
	assert.Len(t, sagas.outbox, 3)
}

func TestHandle_CompensationChain_PaymentFailed(t *testing.T) {
	h, sagas, _, _ := newHandler()
	osID := uuid.New()
	budgetID := uuid.New().String()

	steps := []messaging.Event{
		mustEvent(t, saga.EventOSCreated, "c", map[string]any{"os_id": osID.String()}),
		mustEvent(t, saga.EventBudgetGenerated, "c", map[string]any{"os_id": osID.String(), "budget_id": budgetID}),
		mustEvent(t, saga.EventBudgetApproved, "c", map[string]any{"os_id": osID.String(), "budget_id": budgetID}),
		mustEvent(t, saga.EventPaymentFailed, "c", map[string]any{"os_id": osID.String(), "reason": "declined"}),
	}
	for _, ev := range steps {
		require.NoError(t, h.Handle(context.Background(), ev))
	}
	s, err := sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCompensating, s.State)

	// BudgetCancelled arrives directly (no PaymentRefunded, since no
	// payment was ever confirmed) and should skip straight to
	// CANCEL_OS_REQUESTED then FAILED.
	require.NoError(t, h.Handle(context.Background(), mustEvent(t, saga.EventBudgetCancelled, "c", map[string]any{"os_id": osID.String()})))
	s, err = sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCancelOSRequested, s.State)

	require.NoError(t, h.Handle(context.Background(), mustEvent(t, saga.EventOSCancelled, "c", map[string]any{"os_id": osID.String()})))
	s, err = sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateFailed, s.State)
}

func TestHandle_CompensationChain_ExecutionFailed_ViaRefund(t *testing.T) {
	h, sagas, _, _ := newHandler()
	osID := uuid.New()
	budgetID := uuid.New().String()
	paymentID := uuid.New().String()

	steps := []messaging.Event{
		mustEvent(t, saga.EventOSCreated, "c", map[string]any{"os_id": osID.String()}),
		mustEvent(t, saga.EventBudgetGenerated, "c", map[string]any{"os_id": osID.String(), "budget_id": budgetID}),
		mustEvent(t, saga.EventBudgetApproved, "c", map[string]any{"os_id": osID.String(), "budget_id": budgetID}),
		mustEvent(t, saga.EventPaymentConfirmed, "c", map[string]any{"os_id": osID.String(), "payment_id": paymentID}),
		mustEvent(t, saga.EventExecutionStarted, "c", map[string]any{"os_id": osID.String()}),
		mustEvent(t, saga.EventExecutionFailed, "c", map[string]any{"os_id": osID.String(), "reason": "part missing"}),
	}
	for _, ev := range steps {
		require.NoError(t, h.Handle(context.Background(), ev))
	}
	s, err := sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCompensating, s.State)

	require.NoError(t, h.Handle(context.Background(), mustEvent(t, saga.EventPaymentRefunded, "c", map[string]any{"os_id": osID.String()})))
	s, err = sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCancelBudgetRequested, s.State)

	require.NoError(t, h.Handle(context.Background(), mustEvent(t, saga.EventBudgetCancelled, "c", map[string]any{"os_id": osID.String()})))
	s, err = sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateCancelOSRequested, s.State)

	require.NoError(t, h.Handle(context.Background(), mustEvent(t, saga.EventOSCancelled, "c", map[string]any{"os_id": osID.String()})))
	s, err = sagas.FindByOSID(context.Background(), osID)
	require.NoError(t, err)
	assert.Equal(t, saga.StateFailed, s.State)
}

func TestHandle_DuplicateEvent_IsNoOp(t *testing.T) {
	h, sagas, processed, _ := newHandler()
	osID := uuid.New()
	ev := mustEvent(t, saga.EventOSCreated, "c", map[string]any{"os_id": osID.String()})

	require.NoError(t, h.Handle(context.Background(), ev))
	initialOutboxLen := len(sagas.outbox)

	require.NoError(t, h.Handle(context.Background(), ev))
	assert.Len(t, sagas.outbox, initialOutboxLen, "no new commands should be emitted for a duplicate event")

	eventID, err := uuid.Parse(ev.EventID)
	require.NoError(t, err)
	isProcessed, err := processed.IsProcessed(context.Background(), eventID)
	require.NoError(t, err)
	assert.True(t, isProcessed)
}
