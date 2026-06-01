package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr          string
	MetricsAddr       string
	DatabaseURL       string
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	RedisStream       string
	RedisGroup        string
	OTLPEndpoint      string
	WorkerConcurrency int
	ShutdownTimeout   time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		MetricsAddr:       env("METRICS_ADDR", ":9090"),
		DatabaseURL:       env("DATABASE_URL", "postgres://aetherflow:aetherflow@localhost:5432/aetherflow?sslmode=disable"),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     env("REDIS_PASSWORD", ""),
		RedisStream:       env("REDIS_STREAM", "aetherflow:tasks"),
		RedisGroup:        env("REDIS_GROUP", "aetherflow-workers"),
		OTLPEndpoint:      env("OTLP_ENDPOINT", ""),
		WorkerConcurrency: envInt("WORKER_CONCURRENCY", 4),
		RedisDB:           envInt("REDIS_DB", 0),
		ShutdownTimeout:   envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
