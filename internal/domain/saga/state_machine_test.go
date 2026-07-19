package saga

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// row mirrors one line of the transition table in the spec, so this test
// is the direct evidence the encoded table matches the design exactly.
type row struct {
	name      string
	fromState string
	event     string
	payload   map[string]any
	wantState string
	wantCmds  []string
}

func allRows() []row {
	return []row{
		{
			name:      "OSCreated creates saga",
			fromState: "",
			event:     EventOSCreated,
			payload:   map[string]any{"os_id": uuid.New().String(), "customer_id": uuid.New().String()},
			wantState: StateBudgetRequested,
			wantCmds:  []string{CommandGenerateBudget},
		},
		{
			name:      "BudgetGenerated moves to awaiting approval",
			fromState: StateBudgetRequested,
			event:     EventBudgetGenerated,
			payload:   map[string]any{"budget_id": uuid.New().String(), "amount_cents": 1000},
			wantState: StateAwaitingApproval,
			wantCmds:  nil,
		},
		{
			name:      "BudgetApproved requests payment",
			fromState: StateAwaitingApproval,
			event:     EventBudgetApproved,
			payload:   map[string]any{"budget_id": uuid.New().String()},
			wantState: StatePaymentRequested,
			wantCmds:  []string{CommandRequestPayment},
		},
		{
			name:      "BudgetRejected starts compensation",
			fromState: StateAwaitingApproval,
			event:     EventBudgetRejected,
			payload:   map[string]any{"reason": "too expensive"},
			wantState: StateCompensating,
			wantCmds:  []string{CommandCancelOS},
		},
		{
			name:      "PaymentConfirmed starts execution",
			fromState: StatePaymentRequested,
			event:     EventPaymentConfirmed,
			payload:   map[string]any{"payment_id": uuid.New().String()},
			wantState: StateExecutionRequested,
			wantCmds:  []string{CommandStartExecution},
		},
		{
			name:      "PaymentFailed starts compensation",
			fromState: StatePaymentRequested,
			event:     EventPaymentFailed,
			payload:   map[string]any{"reason": "card declined"},
			wantState: StateCompensating,
			wantCmds:  []string{CommandCancelBudget},
		},
		{
			name:      "ExecutionStarted moves to in-execution",
			fromState: StateExecutionRequested,
			event:     EventExecutionStarted,
			payload:   map[string]any{},
			wantState: StateInExecution,
			wantCmds:  nil,
		},
		{
			name:      "ExecutionCompleted completes the saga",
			fromState: StateInExecution,
			event:     EventExecutionCompleted,
			payload:   map[string]any{},
			wantState: StateCompleted,
			wantCmds:  nil,
		},
		{
			name:      "ExecutionFailed triggers refund",
			fromState: StateInExecution,
			event:     EventExecutionFailed,
			payload:   map[string]any{"reason": "part unavailable"},
			wantState: StateCompensating,
			wantCmds:  []string{CommandRefundPayment},
		},
		{
			name:      "PaymentRefunded cancels the budget",
			fromState: StateCompensating,
			event:     EventPaymentRefunded,
			payload:   map[string]any{},
			wantState: StateCancelBudgetRequested,
			wantCmds:  []string{CommandCancelBudget},
		},
		{
			name:      "BudgetCancelled directly from COMPENSATING cancels the OS",
			fromState: StateCompensating,
			event:     EventBudgetCancelled,
			payload:   map[string]any{},
			wantState: StateCancelOSRequested,
			wantCmds:  []string{CommandCancelOS},
		},
		{
			name:      "BudgetCancelled from CANCEL_BUDGET_REQUESTED cancels the OS",
			fromState: StateCancelBudgetRequested,
			event:     EventBudgetCancelled,
			payload:   map[string]any{},
			wantState: StateCancelOSRequested,
			wantCmds:  []string{CommandCancelOS},
		},
		{
			name:      "OSCancelled terminates the saga as FAILED",
			fromState: StateCancelOSRequested,
			event:     EventOSCancelled,
			payload:   map[string]any{},
			wantState: StateFailed,
			wantCmds:  nil,
		},
	}
}

func TestApply_AllValidTransitions(t *testing.T) {
	for _, r := range allRows() {
		t.Run(r.name, func(t *testing.T) {
			current := SagaInstance{State: r.fromState, OSID: uuid.New()}
			payload, err := json.Marshal(r.payload)
			require.NoError(t, err)

			next, cmds, err := Apply(current, r.event, payload)
			require.NoError(t, err)
			assert.Equal(t, r.wantState, next.State)

			gotNames := make([]string, 0, len(cmds))
			for _, c := range cmds {
				gotNames = append(gotNames, c.Name)
			}
			assert.Equal(t, r.wantCmds, gotNamesOrNil(gotNames))
		})
	}
}

func gotNamesOrNil(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

func TestApply_EveryTableRowCovered(t *testing.T) {
	// Guards against silently adding a table row without a matching test row.
	covered := map[transitionKey]bool{}
	for _, r := range allRows() {
		covered[transitionKey{r.fromState, r.event}] = true
	}
	for k := range transitionTable {
		assert.True(t, covered[k], "table row %+v has no covering test case", k)
	}
	assert.Len(t, covered, len(transitionTable))
}

func TestApply_InvalidTransitions(t *testing.T) {
	cases := []struct {
		name  string
		state string
		event string
	}{
		{"completed saga ignores further events", StateCompleted, EventExecutionFailed},
		{"failed saga ignores further events", StateFailed, EventOSCreated},
		{"unknown event in a known state", StateAwaitingApproval, EventPaymentConfirmed},
		{"event arriving before its prerequisite", StatePaymentRequested, EventExecutionStarted},
		{"duplicate OSCreated on an existing saga", StateBudgetRequested, EventOSCreated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current := SagaInstance{State: tc.state, OSID: uuid.New()}
			_, _, err := Apply(current, tc.event, json.RawMessage(`{}`))
			assert.ErrorIs(t, err, ErrInvalidTransition)
		})
	}
}

func TestApply_ContextAccumulatesAcrossTransitions(t *testing.T) {
	osID := uuid.New()
	budgetID := uuid.New().String()

	created, cmds, err := Apply(SagaInstance{}, EventOSCreated, mustJSON(map[string]any{
		"os_id": osID.String(), "customer_id": "cust-1",
	}))
	require.NoError(t, err)
	require.Len(t, cmds, 1)
	assert.Equal(t, CommandGenerateBudget, cmds[0].Name)
	assert.Equal(t, osID.String(), cmds[0].Payload["os_id"])

	generated, _, err := Apply(created, EventBudgetGenerated, mustJSON(map[string]any{
		"budget_id": budgetID, "amount_cents": 5000,
	}))
	require.NoError(t, err)

	approved, cmds, err := Apply(generated, EventBudgetApproved, mustJSON(map[string]any{
		"budget_id": budgetID,
	}))
	require.NoError(t, err)
	require.Len(t, cmds, 1)
	assert.Equal(t, CommandRequestPayment, cmds[0].Name)
	assert.Equal(t, budgetID, cmds[0].Payload["budget_id"])
	assert.EqualValues(t, 5000, cmds[0].Payload["amount_cents"])
	assert.Equal(t, osID.String(), cmds[0].Payload["os_id"])
	_ = approved
}

func mustJSON(v map[string]any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
