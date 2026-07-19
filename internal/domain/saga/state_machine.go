// Package saga implements the orchestrator's state machine: a pure,
// side-effect-free transition table plus the command payloads each
// transition emits. Persistence and messaging live outside this package.
package saga

import "errors"

// States. PENDING is not reachable via the transition table below (the
// saga is created directly in BUDGET_REQUESTED by OSCreated) but kept as
// a documented "not started" value for zero-value instances.
const (
	StatePending               = "PENDING"
	StateBudgetRequested       = "BUDGET_REQUESTED"
	StateAwaitingApproval      = "AWAITING_APPROVAL"
	StatePaymentRequested      = "PAYMENT_REQUESTED"
	StateExecutionRequested    = "EXECUTION_REQUESTED"
	StateInExecution           = "IN_EXECUTION"
	StateCompleted             = "COMPLETED"
	StateCompensating          = "COMPENSATING"
	StateCancelBudgetRequested = "CANCEL_BUDGET_REQUESTED"
	StateCancelOSRequested     = "CANCEL_OS_REQUESTED"
	StateFailed                = "FAILED"
)

// Events consumed by the orchestrator (produced by sibling services).
const (
	EventOSCreated          = "OSCreated"
	EventBudgetGenerated    = "BudgetGenerated"
	EventBudgetApproved     = "BudgetApproved"
	EventBudgetRejected     = "BudgetRejected"
	EventPaymentConfirmed   = "PaymentConfirmed"
	EventPaymentFailed      = "PaymentFailed"
	EventExecutionStarted   = "ExecutionStarted"
	EventExecutionCompleted = "ExecutionCompleted"
	EventExecutionFailed    = "ExecutionFailed"
	EventPaymentRefunded    = "PaymentRefunded"
	EventBudgetCancelled    = "BudgetCancelled"
	EventOSCancelled        = "OSCancelled"
)

// Commands emitted by the orchestrator (consumed by sibling services).
const (
	CommandGenerateBudget = "GenerateBudgetCommand"
	CommandRequestPayment = "RequestPaymentCommand"
	CommandStartExecution = "StartExecutionCommand"
	CommandRefundPayment  = "RefundPaymentCommand"
	CommandCancelBudget   = "CancelBudgetCommand"
	CommandCancelOS       = "CancelOSCommand"
)

// TerminalStates are states from which no further transition is valid.
var TerminalStates = map[string]bool{
	StateCompleted: true,
	StateFailed:    true,
}

// ErrInvalidTransition signals a (state, event) pair that isn't in the
// transition table. This is expected/normal for duplicate or out-of-order
// events in an eventually-consistent system and must not be treated as an
// infrastructure failure by callers.
var ErrInvalidTransition = errors.New("saga: invalid state transition")

// transitionKey identifies a row of the transition table.
type transitionKey struct {
	state string
	event string
}

// transition describes the outcome of a valid (state, event) pair.
type transition struct {
	next     string
	commands []string
}

// transitionTable is the exhaustive, authoritative encoding of the saga's
// state machine. A (state, event) pair not present here is invalid.
//
// The zero-value/creation row is keyed on state "" (no saga yet) and
// EventOSCreated; it is handled specially by Apply/the use case since
// there is no existing SagaInstance to look up by (state, event) alone.
var transitionTable = map[transitionKey]transition{
	{"", EventOSCreated}: {
		next:     StateBudgetRequested,
		commands: []string{CommandGenerateBudget},
	},
	{StateBudgetRequested, EventBudgetGenerated}: {
		next: StateAwaitingApproval,
	},
	{StateAwaitingApproval, EventBudgetApproved}: {
		next:     StatePaymentRequested,
		commands: []string{CommandRequestPayment},
	},
	{StateAwaitingApproval, EventBudgetRejected}: {
		next:     StateCompensating,
		commands: []string{CommandCancelOS},
	},
	{StatePaymentRequested, EventPaymentConfirmed}: {
		next:     StateExecutionRequested,
		commands: []string{CommandStartExecution},
	},
	{StatePaymentRequested, EventPaymentFailed}: {
		next:     StateCompensating,
		commands: []string{CommandCancelBudget},
	},
	{StateExecutionRequested, EventExecutionStarted}: {
		next: StateInExecution,
	},
	{StateInExecution, EventExecutionCompleted}: {
		next: StateCompleted,
	},
	{StateInExecution, EventExecutionFailed}: {
		next:     StateCompensating,
		commands: []string{CommandRefundPayment},
	},
	{StateCompensating, EventPaymentRefunded}: {
		next:     StateCancelBudgetRequested,
		commands: []string{CommandCancelBudget},
	},
	// Direct path: compensation started from PaymentFailed never had a
	// confirmed payment, so BudgetCancelled can arrive straight from
	// COMPENSATING without a prior PaymentRefunded.
	{StateCompensating, EventBudgetCancelled}: {
		next:     StateCancelOSRequested,
		commands: []string{CommandCancelOS},
	},
	{StateCancelBudgetRequested, EventBudgetCancelled}: {
		next:     StateCancelOSRequested,
		commands: []string{CommandCancelOS},
	},
	{StateCancelOSRequested, EventOSCancelled}: {
		next: StateFailed,
	},
}

// lookup returns the transition for (state, event), or false if invalid.
func lookup(state, event string) (transition, bool) {
	t, ok := transitionTable[transitionKey{state, event}]
	return t, ok
}
