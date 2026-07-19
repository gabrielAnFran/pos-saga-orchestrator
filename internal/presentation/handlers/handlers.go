package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/domain/saga"
	"github.com/gabrielAnFran/pos-saga-orchestrator/internal/infrastructure/db"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type SagaReader interface {
	FindByID(ctx context.Context, id uuid.UUID) (*saga.SagaInstance, error)
	List(ctx context.Context, osIDFilter *uuid.UUID) ([]saga.SagaInstance, error)
	History(ctx context.Context, sagaID uuid.UUID) ([]db.SagaHistoryRow, error)
}

type Handlers struct {
	Sagas SagaReader
	SQLDB *gorm.DB
}

func New(sagas SagaReader, sqlDB *gorm.DB) *Handlers {
	return &Handlers{Sagas: sagas, SQLDB: sqlDB}
}

func (h *Handlers) Register(r *gin.Engine) {
	r.GET("/healthz", h.Healthz)
	r.GET("/readyz", h.Readyz)

	v1 := r.Group("/api/v1")
	v1.GET("/sagas/:id", h.GetSaga)
	v1.GET("/sagas", h.ListSagas)
}

func (h *Handlers) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handlers) Readyz(c *gin.Context) {
	sqlDB, err := h.SQLDB.DB()
	if err != nil || sqlDB.PingContext(c.Request.Context()) != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (h *Handlers) GetSaga(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saga id"})
		return
	}

	s, err := h.Sagas.FindByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "saga not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	history, err := h.Sagas.History(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"saga": s, "history": history})
}

func (h *Handlers) ListSagas(c *gin.Context) {
	var filter *uuid.UUID
	if raw := c.Query("os_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid os_id"})
			return
		}
		filter = &id
	}

	sagas, err := h.Sagas.List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"sagas": sagas})
}
