package timer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aetherflow/aetherflow/internal/store"
)

type EnqueueFunc func(context.Context, string) error

type Service struct {
	repo      store.Repository
	redis     *redis.Client
	keyPrefix string
	enqueue   EnqueueFunc
	logger    *slog.Logger
	pollEvery time.Duration
	pollLimit int
	redisDB   int
}

type Config struct {
	KeyPrefix string
	PollEvery time.Duration
	PollLimit int
	RedisDB   int
}

func New(repo store.Repository, redisClient *redis.Client, enqueue EnqueueFunc, logger *slog.Logger, config Config) *Service {
	if config.KeyPrefix == "" {
		config.KeyPrefix = "aetherflow:timer:"
	}
	if config.PollEvery <= 0 {
		config.PollEvery = 5 * time.Second
	}
	if config.PollLimit <= 0 {
		config.PollLimit = 100
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		repo:      repo,
		redis:     redisClient,
		keyPrefix: config.KeyPrefix,
		enqueue:   enqueue,
		logger:    logger,
		pollEvery: config.PollEvery,
		pollLimit: config.PollLimit,
		redisDB:   config.RedisDB,
	}
}

func (s *Service) Schedule(ctx context.Context, timer store.Timer) error {
	if err := s.repo.UpsertTimer(ctx, timer); err != nil {
		return fmt.Errorf("persist timer: %w", err)
	}
	ttl := time.Until(timer.FireAt)
	if ttl <= 0 {
		if err := s.fire(ctx, timer.InstanceID); err != nil {
			return fmt.Errorf("fire due timer: %w", err)
		}
		return nil
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.redis.Set(queryCtx, s.key(timer.InstanceID), timer.StepID, ttl).Err(); err != nil {
		return fmt.Errorf("set redis timer key: %w", err)
	}
	return nil
}

func (s *Service) Clear(ctx context.Context, instanceID string) error {
	if err := s.repo.DeleteTimer(ctx, instanceID); err != nil {
		return fmt.Errorf("delete persisted timer: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.redis.Del(queryCtx, s.key(instanceID)).Err(); err != nil {
		return fmt.Errorf("delete redis timer key: %w", err)
	}
	return nil
}

func (s *Service) Run(ctx context.Context) error {
	pubsub := s.redis.PSubscribe(ctx, fmt.Sprintf("__keyevent@%d__:expired", s.redisDB))
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		s.logger.Warn("redis keyspace notification subscription failed", "error", err)
	}

	ticker := time.NewTicker(s.pollEvery)
	defer ticker.Stop()
	channel := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-channel:
			if message == nil {
				continue
			}
			if strings.HasPrefix(message.Payload, s.keyPrefix) {
				instanceID := strings.TrimPrefix(message.Payload, s.keyPrefix)
				if err := s.fire(ctx, instanceID); err != nil {
					s.logger.Error("fire redis keyspace timer", "instance_id", instanceID, "error", err)
				}
			}
		case <-ticker.C:
			if err := s.poll(ctx); err != nil {
				s.logger.Error("poll due timers", "error", err)
			}
		}
	}
}

func (s *Service) poll(ctx context.Context) error {
	due, err := s.repo.ClaimDueTimers(ctx, time.Now().UTC(), s.pollLimit)
	if err != nil {
		return fmt.Errorf("claim due timers: %w", err)
	}
	for _, timer := range due {
		if err := s.clearRedisKey(ctx, timer.InstanceID); err != nil {
			return fmt.Errorf("clear due timer key %s: %w", timer.InstanceID, err)
		}
		if s.enqueue == nil {
			continue
		}
		if err := s.enqueue(ctx, timer.InstanceID); err != nil {
			return fmt.Errorf("enqueue due timer %s: %w", timer.InstanceID, err)
		}
	}
	return nil
}

func (s *Service) fire(ctx context.Context, instanceID string) error {
	if _, ok, err := s.repo.FireTimer(ctx, instanceID); err != nil {
		return fmt.Errorf("claim fired timer: %w", err)
	} else if !ok {
		return s.clearRedisKey(ctx, instanceID)
	}
	if err := s.clearRedisKey(ctx, instanceID); err != nil {
		return fmt.Errorf("delete fired redis key: %w", err)
	}
	if s.enqueue == nil {
		return nil
	}
	if err := s.enqueue(ctx, instanceID); err != nil {
		return fmt.Errorf("enqueue fired timer: %w", err)
	}
	return nil
}

func (s *Service) clearRedisKey(ctx context.Context, instanceID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.redis.Del(queryCtx, s.key(instanceID)).Err(); err != nil {
		return fmt.Errorf("delete redis timer key: %w", err)
	}
	return nil
}

func (s *Service) key(instanceID string) string {
	return s.keyPrefix + instanceID
}
