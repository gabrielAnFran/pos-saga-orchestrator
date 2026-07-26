package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/db"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type fakeSagaReader struct {
	byID       map[uuid.UUID]*saga.SagaInstance
	history    map[uuid.UUID][]db.SagaHistoryRow
	all        []saga.SagaInstance
	listErr    error
	findErr    error
	historyErr error
}

func (f *fakeSagaReader) FindByID(ctx context.Context, id uuid.UUID) (*saga.SagaInstance, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	s, ok := f.byID[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (f *fakeSagaReader) List(ctx context.Context, osIDFilter *uuid.UUID) ([]saga.SagaInstance, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if osIDFilter == nil {
		return f.all, nil
	}
	var out []saga.SagaInstance
	for _, s := range f.all {
		if s.OSID == *osIDFilter {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeSagaReader) History(ctx context.Context, sagaID uuid.UUID) ([]db.SagaHistoryRow, error) {
	if f.historyErr != nil {
		return nil, f.historyErr
	}
	return f.history[sagaID], nil
}

func newTestRouter(t *testing.T, reader SagaReader, sqlDB *gorm.DB) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := New(reader, sqlDB)
	h.Register(r)
	return r
}

func mockGormDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	mock.ExpectPing() // gorm.Open pings once to verify the connection
	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	require.NoError(t, err)
	return gormDB, mock
}

func TestHealthz(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReadyz_DBUp(t *testing.T) {
	gormDB, mock := mockGormDB(t)
	mock.ExpectPing()
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReadyz_DBDown(t *testing.T) {
	gormDB, mock := mockGormDB(t)
	mock.ExpectPing().WillReturnError(assert.AnError)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestGetSaga_Found(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	id := uuid.New()
	osID := uuid.New()
	reader := &fakeSagaReader{
		byID: map[uuid.UUID]*saga.SagaInstance{
			id: {ID: id, OSID: osID, State: saga.StateCompleted},
		},
		history: map[uuid.UUID][]db.SagaHistoryRow{
			id: {{SagaID: id, ToState: saga.StateCompleted}},
		},
	}
	r := newTestRouter(t, reader, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+id.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotNil(t, body["saga"])
	assert.Len(t, body["history"], 1)
}

func TestGetSaga_NotFound(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{byID: map[uuid.UUID]*saga.SagaInstance{}}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetSaga_InvalidID(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListSagas_All(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	reader := &fakeSagaReader{all: []saga.SagaInstance{{ID: uuid.New()}, {ID: uuid.New()}}}
	r := newTestRouter(t, reader, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Len(t, body["sagas"], 2)
}

func TestListSagas_FilteredByOSID(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	osID := uuid.New()
	reader := &fakeSagaReader{all: []saga.SagaInstance{
		{ID: uuid.New(), OSID: osID},
		{ID: uuid.New(), OSID: uuid.New()},
	}}
	r := newTestRouter(t, reader, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas?os_id="+osID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Len(t, body["sagas"], 1)
}

func TestGetSaga_FindByIDInternalError(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{findErr: assert.AnError}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetSaga_HistoryInternalError(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	id := uuid.New()
	reader := &fakeSagaReader{
		byID:       map[uuid.UUID]*saga.SagaInstance{id: {ID: id}},
		historyErr: assert.AnError,
	}
	r := newTestRouter(t, reader, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas/"+id.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestListSagas_InternalError(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{listErr: assert.AnError}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestListSagas_InvalidOSID(t *testing.T) {
	gormDB, _ := mockGormDB(t)
	r := newTestRouter(t, &fakeSagaReader{}, gormDB)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sagas?os_id=not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
