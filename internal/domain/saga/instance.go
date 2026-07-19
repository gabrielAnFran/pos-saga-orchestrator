package saga

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// SagaInstance is the durable record of one saga's progress. Context
// accumulates fields extracted from incoming event payloads (budget_id,
// payment_id, amount_cents, ...) so later transitions can build the
// payloads of the commands they emit.
type SagaInstance struct {
	ID          uuid.UUID
	SagaType    string
	OSID        uuid.UUID
	State       string
	Context     json.RawMessage
	LastEventID uuid.UUID
	RetryCount  int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CommandToEmit is a command produced by a transition, ready to be
// wrapped in a messaging.Event and written to the outbox.
type CommandToEmit struct {
	Name    string
	Payload map[string]any
}

const SagaTypeServiceOrder = "service_order"

// contextMap unmarshals ctx into a plain map, treating nil/empty as {}.
func contextMap(ctx json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(ctx) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(ctx, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// payloadMap unmarshals an event payload into a plain map.
func payloadMap(payload json.RawMessage) (map[string]any, error) {
	m := map[string]any{}
	if len(payload) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// extractFields copies the given keys from src into dst when present.
func extractFields(dst, src map[string]any, keys ...string) {
	for _, k := range keys {
		if v, ok := src[k]; ok {
			dst[k] = v
		}
	}
}

// contextFieldsByEvent lists, per event name, which payload fields get
// merged into the saga's Context for later use by downstream commands.
var contextFieldsByEvent = map[string][]string{
	EventOSCreated:        {"os_id", "customer_id"},
	EventBudgetGenerated:  {"budget_id", "amount_cents"},
	EventBudgetApproved:   {"budget_id"},
	EventPaymentConfirmed: {"payment_id", "mp_payment_id"},
	EventExecutionFailed:  {"reason"},
	EventBudgetRejected:   {"reason"},
	EventPaymentFailed:    {"reason"},
}

// buildCommandPayload constructs the payload for a command given the
// saga's accumulated context, per the field lists in the spec.
func buildCommandPayload(command string, osID uuid.UUID, ctx map[string]any) map[string]any {
	switch command {
	case CommandGenerateBudget:
		p := map[string]any{"os_id": osID.String()}
		if v, ok := ctx["customer_id"]; ok {
			p["customer_id"] = v
		}
		return p
	case CommandRequestPayment:
		p := map[string]any{"os_id": osID.String()}
		if v, ok := ctx["budget_id"]; ok {
			p["budget_id"] = v
		}
		if v, ok := ctx["amount_cents"]; ok {
			p["amount_cents"] = v
		}
		return p
	case CommandStartExecution:
		p := map[string]any{"os_id": osID.String()}
		if v, ok := ctx["budget_id"]; ok {
			p["budget_id"] = v
		}
		return p
	case CommandRefundPayment:
		p := map[string]any{}
		if v, ok := ctx["payment_id"]; ok {
			p["payment_id"] = v
		}
		if v, ok := ctx["mp_payment_id"]; ok {
			p["mp_payment_id"] = v
		}
		p["reason"] = reasonOrDefault(ctx, "execution failed")
		return p
	case CommandCancelBudget:
		p := map[string]any{}
		if v, ok := ctx["budget_id"]; ok {
			p["budget_id"] = v
		}
		p["reason"] = reasonOrDefault(ctx, "saga compensation")
		return p
	case CommandCancelOS:
		p := map[string]any{"os_id": osID.String()}
		p["reason"] = reasonOrDefault(ctx, "saga compensation")
		return p
	default:
		return map[string]any{}
	}
}

func reasonOrDefault(ctx map[string]any, def string) string {
	if v, ok := ctx["reason"]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return def
}

// Apply is the pure core of the orchestrator: given the current saga
// state (zero-value SagaInstance with State "" if the saga doesn't exist
// yet), an incoming event name and payload, it returns the next saga
// state and the commands to emit, or ErrInvalidTransition if (state,
// event) isn't a valid row of the transition table.
func Apply(current SagaInstance, eventName string, payload json.RawMessage) (SagaInstance, []CommandToEmit, error) {
	t, ok := lookup(current.State, eventName)
	if !ok {
		return SagaInstance{}, nil, ErrInvalidTransition
	}

	ctx, err := contextMap(current.Context)
	if err != nil {
		return SagaInstance{}, nil, err
	}
	pm, err := payloadMap(payload)
	if err != nil {
		return SagaInstance{}, nil, err
	}
	if fields, ok := contextFieldsByEvent[eventName]; ok {
		extractFields(ctx, pm, fields...)
	}

	osID := current.OSID
	if osID == uuid.Nil {
		if v, ok := pm["os_id"]; ok {
			if s, ok := v.(string); ok {
				if parsed, err := uuid.Parse(s); err == nil {
					osID = parsed
				}
			}
		}
	}

	newCtxBytes, err := json.Marshal(ctx)
	if err != nil {
		return SagaInstance{}, nil, err
	}

	next := current
	next.OSID = osID
	next.State = t.next
	next.Context = newCtxBytes
	if next.SagaType == "" {
		next.SagaType = SagaTypeServiceOrder
	}

	commands := make([]CommandToEmit, 0, len(t.commands))
	for _, cmdName := range t.commands {
		commands = append(commands, CommandToEmit{
			Name:    cmdName,
			Payload: buildCommandPayload(cmdName, osID, ctx),
		})
	}

	return next, commands, nil
}
