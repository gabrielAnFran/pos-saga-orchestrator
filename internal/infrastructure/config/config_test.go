package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad_Defaults(t *testing.T) {
	for _, k := range []string{"SAGA_PORT", "SAGA_DB_DSN", "SAGA_AMQP_URL", "SAGA_DISPATCH_INTERVAL_MS", "OS_SERVICE_URL"} {
		t.Setenv(k, "")
	}

	cfg := Load()

	assert.Equal(t, "8084", cfg.Port)
	assert.Equal(t, "postgres://saga:saga@localhost:5435/saga_orchestrator?sslmode=disable", cfg.DBDSN)
	assert.Equal(t, "amqp://guest:guest@localhost:5672/", cfg.AMQPURL)
	assert.Equal(t, 500, cfg.DispatchIntervalMS)
	assert.Equal(t, "http://os-service:8081", cfg.OSServiceURL)
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("SAGA_PORT", "9090")
	t.Setenv("SAGA_DB_DSN", "postgres://custom")
	t.Setenv("SAGA_AMQP_URL", "amqp://custom")
	t.Setenv("SAGA_DISPATCH_INTERVAL_MS", "1234")
	t.Setenv("OS_SERVICE_URL", "http://custom:8081")

	cfg := Load()

	assert.Equal(t, "9090", cfg.Port)
	assert.Equal(t, "postgres://custom", cfg.DBDSN)
	assert.Equal(t, "amqp://custom", cfg.AMQPURL)
	assert.Equal(t, 1234, cfg.DispatchIntervalMS)
	assert.Equal(t, "http://custom:8081", cfg.OSServiceURL)
}

func TestLoad_InvalidIntFallsBackToDefault(t *testing.T) {
	t.Setenv("SAGA_DISPATCH_INTERVAL_MS", "not-a-number")

	cfg := Load()

	assert.Equal(t, 500, cfg.DispatchIntervalMS)
}
