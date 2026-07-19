// Package bdd drives the pure saga.Apply state machine plus a tiny
// in-memory fake repository through godog scenarios. It intentionally
// avoids real Postgres/RabbitMQ to stay fast and hermetic; a
// testcontainers-backed BDD suite is a nice-to-have left for later.
package bdd

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cucumber/godog"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	"github.com/google/uuid"
)

type sagaCtx struct {
	osID     uuid.UUID
	instance saga.SagaInstance
}

func anOSIsCreatedWithID(ctx context.Context, id string) (context.Context, error) {
	osID, err := uuid.Parse(id)
	if err != nil {
		return ctx, err
	}
	sc := &sagaCtx{osID: osID, instance: saga.SagaInstance{}}
	next, _, err := saga.Apply(sc.instance, saga.EventOSCreated, mustJSON(map[string]any{"os_id": osID.String()}))
	if err != nil {
		return ctx, err
	}
	sc.instance = next
	return context.WithValue(ctx, sagaCtxKey{}, sc), nil
}

type sagaCtxKey struct{}

func getSagaCtx(ctx context.Context) *sagaCtx {
	return ctx.Value(sagaCtxKey{}).(*sagaCtx)
}

func theBudgetIsGeneratedAndApproved(ctx context.Context) (context.Context, error) {
	sc := getSagaCtx(ctx)
	if err := applyErr(sc, saga.EventBudgetGenerated, map[string]any{"budget_id": uuid.New().String(), "amount_cents": 1000}); err != nil {
		return ctx, err
	}
	if err := applyErr(sc, saga.EventBudgetApproved, map[string]any{}); err != nil {
		return ctx, err
	}
	return ctx, nil
}

func thePaymentIsConfirmed(ctx context.Context) (context.Context, error) {
	sc := getSagaCtx(ctx)
	return ctx, applyErr(sc, saga.EventPaymentConfirmed, map[string]any{"payment_id": uuid.New().String()})
}

func thePaymentFails(ctx context.Context) (context.Context, error) {
	sc := getSagaCtx(ctx)
	return ctx, applyErr(sc, saga.EventPaymentFailed, map[string]any{"reason": "card declined"})
}

func theExecutionStartsAndCompletes(ctx context.Context) (context.Context, error) {
	sc := getSagaCtx(ctx)
	if err := applyErr(sc, saga.EventExecutionStarted, map[string]any{}); err != nil {
		return ctx, err
	}
	return ctx, applyErr(sc, saga.EventExecutionCompleted, map[string]any{})
}

func theSagaShouldReachState(ctx context.Context, want string) error {
	sc := getSagaCtx(ctx)
	if sc.instance.State != want {
		return fmt.Errorf("expected state %q, got %q", want, sc.instance.State)
	}
	return nil
}

func theSagaShouldReachStateAfterBudgetCancellation(ctx context.Context, want string) (context.Context, error) {
	sc := getSagaCtx(ctx)
	if err := applyErr(sc, saga.EventBudgetCancelled, map[string]any{}); err != nil {
		return ctx, err
	}
	return ctx, theSagaShouldReachState(ctx, want)
}

func theSagaShouldReachStateAfterOSIsCancelled(ctx context.Context, want string) (context.Context, error) {
	sc := getSagaCtx(ctx)
	if err := applyErr(sc, saga.EventOSCancelled, map[string]any{}); err != nil {
		return ctx, err
	}
	return ctx, theSagaShouldReachState(ctx, want)
}

func applyErr(sc *sagaCtx, eventName string, payload map[string]any) error {
	payload["os_id"] = sc.osID.String()
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	next, _, err := saga.Apply(sc.instance, eventName, b)
	if err != nil {
		return fmt.Errorf("apply %s from %s: %w", eventName, sc.instance.State, err)
	}
	sc.instance = next
	return nil
}

func mustJSON(v map[string]any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func InitializeScenario(sc *godog.ScenarioContext) {
	sc.Given(`^an OS is created with id "([^"]*)"$`, anOSIsCreatedWithID)
	sc.When(`^the budget is generated and approved$`, theBudgetIsGeneratedAndApproved)
	sc.When(`^the payment is confirmed$`, thePaymentIsConfirmed)
	sc.When(`^the payment fails$`, thePaymentFails)
	sc.When(`^the execution starts and completes$`, theExecutionStartsAndCompletes)
	sc.Then(`^the saga should reach state "([^"]*)"$`, theSagaShouldReachState)
	sc.Then(`^the saga should reach state "([^"]*)" after budget cancellation$`, theSagaShouldReachStateAfterBudgetCancellation)
	sc.Then(`^the saga should reach state "([^"]*)" after the OS is cancelled$`, theSagaShouldReachStateAfterOSIsCancelled)
}

func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		Name:                "saga-orchestrator",
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}
