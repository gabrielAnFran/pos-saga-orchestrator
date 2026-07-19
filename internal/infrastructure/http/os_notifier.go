// Package http holds outbound HTTP concerns: best-effort, post-commit
// synchronization of the OS Service's order status to mirror saga
// progress, described in the ADR as a documented simplification (not
// part of the saga's own correctness — saga_instances/saga_history
// remain the durable source of truth regardless of whether these calls
// succeed).
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// orderStatusChain mirrors OS Service's entities.Order state machine,
// which only allows single-step forward transitions. To move an order
// from an arbitrary earlier status to a saga-driven target, we must
// walk it through every intermediate status in order.
var orderStatusChain = []string{
	"CREATED", "BUDGETING", "AWAITING_APPROVAL", "APPROVED",
	"PAYING", "PAID", "IN_EXECUTION", "COMPLETED",
}

type OSNotifier struct {
	baseURL string
	client  *http.Client
}

func NewOSNotifier(baseURL string) *OSNotifier {
	return &OSNotifier{baseURL: baseURL, client: &http.Client{Timeout: 5 * time.Second}}
}

// NotifyCompleted is kept for backward compatibility with existing callers;
// it's equivalent to SyncStatus(ctx, osID, "COMPLETED").
func (n *OSNotifier) NotifyCompleted(ctx context.Context, osID uuid.UUID) error {
	return n.SyncStatus(ctx, osID, "COMPLETED")
}

// SyncStatus walks the order forward, one valid transition at a time,
// from its current status up to target. No-op if already at or past
// target. Best-effort: the first failing step aborts and returns an
// error for the caller to log; it does not roll back.
func (n *OSNotifier) SyncStatus(ctx context.Context, osID uuid.UUID, target string) error {
	current, err := n.getStatus(ctx, osID)
	if err != nil {
		return fmt.Errorf("fetching current order status: %w", err)
	}

	curIdx := indexOf(orderStatusChain, current)
	targetIdx := indexOf(orderStatusChain, target)
	if curIdx == -1 || targetIdx == -1 {
		return fmt.Errorf("unknown status in chain: current=%q target=%q", current, target)
	}
	if curIdx >= targetIdx {
		return nil
	}

	for i := curIdx + 1; i <= targetIdx; i++ {
		if err := n.patchStatus(ctx, osID, orderStatusChain[i]); err != nil {
			return fmt.Errorf("advancing order to %s: %w", orderStatusChain[i], err)
		}
	}
	return nil
}

func (n *OSNotifier) getStatus(ctx context.Context, osID uuid.UUID) (string, error) {
	url := fmt.Sprintf("%s/api/v1/orders/%s", n.baseURL, osID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("os-service returned status %d", resp.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.Status, nil
}

func (n *OSNotifier) patchStatus(ctx context.Context, osID uuid.UUID, status string) error {
	body, err := json.Marshal(map[string]string{"status": status})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/orders/%s/status", n.baseURL, osID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("os-service returned status %d", resp.StatusCode)
	}
	return nil
}

func indexOf(chain []string, status string) int {
	for i, s := range chain {
		if s == status {
			return i
		}
	}
	return -1
}
