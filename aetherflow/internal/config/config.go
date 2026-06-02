package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
	APIKeys           string
	WorkerConcurrency int
	MaxRequestBytes   int64
	AllowPrivateHTTP  bool
	WorkerLease       time.Duration
	RedisPendingIdle  time.Duration
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
		APIKeys:           env("API_KEYS", ""),
		WorkerConcurrency: envInt("WORKER_CONCURRENCY", 4),
		MaxRequestBytes:   envInt64("MAX_REQUEST_BYTES", 1<<20),
		AllowPrivateHTTP:  envBool("ALLOW_PRIVATE_HTTP", false),
		WorkerLease:       envDuration("WORKER_LEASE", 2*time.Minute),
		RedisPendingIdle:  envDuration("REDIS_PENDING_IDLE", 30*time.Second),
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

func envInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
