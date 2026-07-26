package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordedServer struct {
	server  *httptest.Server
	status  string
	patches []string
	failOn  string // if set, this PATCH target status returns 500
}

func newRecordedServer(initialStatus string) *recordedServer {
	rs := &recordedServer{status: initialStatus}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/orders/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": rs.status})
		case http.MethodPatch:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				panic(err)
			}
			target := body["status"]
			if rs.failOn != "" && target == rs.failOn {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			rs.patches = append(rs.patches, target)
			rs.status = target
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	rs.server = httptest.NewServer(mux)
	return rs
}

func TestSyncStatus_NoOpWhenAlreadyAtTarget(t *testing.T) {
	rs := newRecordedServer("PAYING")
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "PAYING")
	require.NoError(t, err)
	assert.Empty(t, rs.patches)
}

func TestSyncStatus_NoOpWhenPastTarget(t *testing.T) {
	rs := newRecordedServer("COMPLETED")
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "PAYING")
	require.NoError(t, err)
	assert.Empty(t, rs.patches)
}

func TestSyncStatus_SingleStepAdvance(t *testing.T) {
	rs := newRecordedServer("CREATED")
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "BUDGETING")
	require.NoError(t, err)
	assert.Equal(t, []string{"BUDGETING"}, rs.patches)
}

func TestSyncStatus_MultiStepAdvance(t *testing.T) {
	rs := newRecordedServer("CREATED")
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "PAYING")
	require.NoError(t, err)
	assert.Equal(t, []string{"BUDGETING", "AWAITING_APPROVAL", "APPROVED", "PAYING"}, rs.patches)
}

func TestSyncStatus_ErrorPartwayThroughStopsAndPropagates(t *testing.T) {
	rs := newRecordedServer("CREATED")
	rs.failOn = "APPROVED"
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "PAYING")
	require.Error(t, err)
	assert.Equal(t, []string{"BUDGETING", "AWAITING_APPROVAL"}, rs.patches)
}

func TestSyncStatus_UnknownStatusErrors(t *testing.T) {
	rs := newRecordedServer("SOMETHING_WEIRD")
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "PAYING")
	require.Error(t, err)
}

func TestSyncStatus_GetStatusHTTPErrorPropagates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	n := NewOSNotifier(server.URL)
	err := n.SyncStatus(context.Background(), uuid.New(), "PAYING")
	require.Error(t, err)
}

func TestNotifyCompleted_DelegatesToSyncStatus(t *testing.T) {
	rs := newRecordedServer("IN_EXECUTION")
	defer rs.server.Close()

	n := NewOSNotifier(rs.server.URL)
	err := n.NotifyCompleted(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.Equal(t, []string{"COMPLETED"}, rs.patches)
}
