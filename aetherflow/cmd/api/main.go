package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/aetherflow/aetherflow/internal/api"
	"github.com/aetherflow/aetherflow/internal/config"
	"github.com/aetherflow/aetherflow/internal/dsl"
	"github.com/aetherflow/aetherflow/internal/engine"
	"github.com/aetherflow/aetherflow/internal/store"
	"github.com/aetherflow/aetherflow/internal/telemetry"
	"github.com/aetherflow/aetherflow/internal/timer"
	"github.com/aetherflow/aetherflow/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("aetherflow stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	traceShutdown, err := telemetry.InitTracing(rootCtx, "aetherflow", cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceShutdown(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "error", err)
		}
	}()

	pool, err := pgxpool.New(rootCtx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()
	repo := store.NewPostgres(pool)
	if err := repo.Ping(rootCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer redisClient.Close()
	pingCtx, cancel := context.WithTimeout(rootCtx, 3*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		cancel()
		return fmt.Errorf("ping redis: %w", err)
	}
	cancel()

	metrics := telemetry.NewMetrics()
	executor := worker.NewExecutor(&http.Client{Timeout: 15 * time.Second})

	var timerService *timer.Service
	workflowEngine := engine.New(repo, redisClient, executor, nil, metrics, logger, engine.Config{
		Stream:      cfg.RedisStream,
		Group:       cfg.RedisGroup,
		Concurrency: cfg.WorkerConcurrency,
	})
	timerService = timer.New(repo, redisClient, workflowEngine.Enqueue, logger, timer.Config{RedisDB: cfg.RedisDB})
	workflowEngine = engine.New(repo, redisClient, executor, timerService, metrics, logger, engine.Config{
		Stream:      cfg.RedisStream,
		Group:       cfg.RedisGroup,
		Concurrency: cfg.WorkerConcurrency,
	})

	apiHandler := api.New(repo, redisClient, workflowEngine, dsl.NewValidator())
	apiServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           apiHandler.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           telemetry.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 4)
	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)
		if err := apiServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("api server: %w", err)
		}
	}()
	go func() {
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()
	go func() {
		if err := workflowEngine.Run(rootCtx); err != nil {
			errCh <- fmt.Errorf("engine run: %w", err)
		}
	}()
	go func() {
		if err := timerService.Run(rootCtx); err != nil {
			errCh <- fmt.Errorf("timer run: %w", err)
		}
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		stop()
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown api server: %w", err)
	}
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown metrics server: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}
