// Package http holds outbound HTTP concerns: the synchronous "notify
// OS Service that execution completed" callback described in the ADR as
// a documented simplification (not part of the saga's own correctness).
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

type OSNotifier struct {
	baseURL string
	client  *http.Client
}

func NewOSNotifier(baseURL string) *OSNotifier {
	return &OSNotifier{baseURL: baseURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (n *OSNotifier) NotifyCompleted(ctx context.Context, osID uuid.UUID) error {
	body, err := json.Marshal(map[string]string{"status": "COMPLETED"})
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
