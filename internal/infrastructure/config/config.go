package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port               string
	DBDSN              string
	AMQPURL            string
	DispatchIntervalMS int
	OSServiceURL       string
}

func Load() Config {
	return Config{
		Port:               getEnv("SAGA_PORT", "8084"),
		DBDSN:              getEnv("SAGA_DB_DSN", "postgres://saga:saga@localhost:5435/saga_orchestrator?sslmode=disable"),
		AMQPURL:            getEnv("SAGA_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		DispatchIntervalMS: getEnvInt("SAGA_DISPATCH_INTERVAL_MS", 500),
		OSServiceURL:       getEnv("OS_SERVICE_URL", "http://os-service:8081"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
