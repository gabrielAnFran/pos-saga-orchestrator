package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/application/usecases"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/config"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/db"
	nethttp "github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/http"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/messaging"
)

const serviceName = "saga-orchestrator"

// routingKeys lists every event produced by the sibling services that
// this orchestrator needs to react to.
var routingKeys = []string{
	saga.EventOSCreated,
	saga.EventBudgetGenerated,
	saga.EventBudgetApproved,
	saga.EventBudgetRejected,
	saga.EventPaymentConfirmed,
	saga.EventPaymentFailed,
	saga.EventExecutionStarted,
	saga.EventExecutionCompleted,
	saga.EventExecutionFailed,
	saga.EventPaymentRefunded,
	saga.EventBudgetCancelled,
	saga.EventOSCancelled,
}

// stuckThreshold and tickInterval drive the lightweight saga-tick
// detector described in the ADR as a documented simplification of a
// fuller per-state-timeout compensation design.
const (
	stuckThreshold = 2 * time.Minute
	tickInterval   = 10 * time.Second
)

func main() {
	cfg := config.Load()

	if err := db.Migrate(cfg.DBDSN, "migrations"); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	gormDB, err := db.Open(cfg.DBDSN)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	amqpConn, err := messaging.Dial(cfg.AMQPURL)
	if err != nil {
		slog.Error("failed to connect to amqp", "error", err)
		os.Exit(1)
	}
	defer amqpConn.Close()

	if _, err := amqpConn.DeclareServiceQueue(serviceName, routingKeys); err != nil {
		slog.Error("failed to declare service queue", "error", err)
		os.Exit(1)
	}

	sagaRepo := db.NewSagaRepository(gormDB)
	processedRepo := db.NewProcessedEventRepository(gormDB)
	notifier := nethttp.NewOSNotifier(cfg.OSServiceURL)
	handler := usecases.NewHandleEvent(sagaRepo, processedRepo, notifier)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runSagaTick(ctx, sagaRepo)

	slog.Info("saga-orchestrator worker consuming events")
	if err := amqpConn.Consume(ctx, serviceName, handler.Handle); err != nil && ctx.Err() == nil {
		slog.Error("consumer stopped unexpectedly", "error", err)
		os.Exit(1)
	}
	slog.Info("saga-orchestrator worker shutting down")
}

// runSagaTick periodically detects sagas that haven't progressed in a
// while and logs them. A fuller implementation would auto-trigger
// compensation per-state timeouts; that's out of scope for this
// challenge, see docs/adr/0001-orchestrated-saga.md.
func runSagaTick(ctx context.Context, repo *db.SagaRepository) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-stuckThreshold)
			stuck, err := repo.StuckSagas(ctx, cutoff)
			if err != nil {
				slog.Warn("saga-tick: failed to query stuck sagas", "error", err)
				continue
			}
			for _, s := range stuck {
				slog.Warn("saga-tick: stuck saga detected", "saga_id", s.ID, "os_id", s.OSID, "state", s.State, "updated_at", s.UpdatedAt)
			}
			if len(stuck) > 0 {
				slog.Info("saga-tick: stuck_sagas_total", "count", len(stuck))
			}
		}
	}
}
