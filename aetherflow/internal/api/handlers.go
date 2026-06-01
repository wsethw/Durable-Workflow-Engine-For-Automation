package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/aetherflow/aetherflow/internal/dsl"
	"github.com/aetherflow/aetherflow/internal/engine"
	"github.com/aetherflow/aetherflow/internal/store"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

type Handler struct {
	repo      store.Repository
	redis     *redis.Client
	engine    *engine.Engine
	validator dsl.Validator
}

func New(repo store.Repository, redisClient *redis.Client, engine *engine.Engine, validator dsl.Validator) *Handler {
	return &Handler{repo: repo, redis: redisClient, engine: engine, validator: validator}
}

func (h *Handler) Router() http.Handler {
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestLogger())

	router.GET("/healthz", h.healthz)
	router.GET("/readyz", h.readyz)
	router.POST("/definitions", h.createDefinition)
	router.POST("/instances", h.createInstance)
	router.GET("/instances/:id", h.getInstance)
	return router
}

func (h *Handler) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) readyz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	if err := h.repo.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "postgres": err.Error()})
		return
	}
	if err := h.redis.Ping(ctx).Err(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (h *Handler) createDefinition(c *gin.Context) {
	var request workflow.DefinitionDSL
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("decode definition request: %w", err).Error()})
		return
	}
	if err := h.validator.Validate(c.Request.Context(), request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	definition, err := h.repo.CreateDefinition(c.Request.Context(), request)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"id":         definition.ID,
		"name":       definition.Name,
		"version":    definition.Version,
		"created_at": definition.CreatedAt,
	})
}

type createInstanceRequest struct {
	DefinitionID string         `json:"definition_id"`
	Input        map[string]any `json:"input"`
}

func (h *Handler) createInstance(c *gin.Context) {
	var request createInstanceRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("decode instance request: %w", err).Error()})
		return
	}
	if request.DefinitionID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "definition_id is required"})
		return
	}
	if request.Input == nil {
		request.Input = map[string]any{}
	}
	definition, err := h.repo.GetDefinition(c.Request.Context(), request.DefinitionID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, pgx.ErrNoRows) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	instance, err := h.repo.CreateInstance(c.Request.Context(), definition, request.Input)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.engine.Enqueue(c.Request.Context(), instance.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"id":                 instance.ID,
		"definition_id":      instance.DefinitionID,
		"definition_version": instance.DefinitionVersion,
		"status":             instance.Status,
	})
}

func (h *Handler) getInstance(c *gin.Context) {
	instanceID := c.Param("id")
	instance, err := h.repo.GetInstance(c.Request.Context(), instanceID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, pgx.ErrNoRows) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	history, err := h.repo.ListHistory(c.Request.Context(), instanceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"id":                 instance.ID,
		"definition_id":      instance.DefinitionID,
		"definition_version": instance.DefinitionVersion,
		"status":             instance.Status,
		"input":              instance.Input,
		"current_step_id":    instance.CurrentStepID,
		"state":              instance.State,
		"version":            instance.Version,
		"created_at":         instance.CreatedAt,
		"updated_at":         instance.UpdatedAt,
		"history":            history,
	})
}

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		gin.DefaultWriter.Write([]byte(fmt.Sprintf("%s %s %d %s\n", c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start))))
	}
}
