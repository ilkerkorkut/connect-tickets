package route

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/handler"
	"github.com/ilkerkorkut/connect-tickets/internal/middleware"
)

type Dependencies struct {
	Repositories handlers.RepositoryService
	Batches      handlers.BatchService
	Changes      handlers.ChangeService
	Health       interface {
		Live() bool
		Ready(context.Context) error
	}
}

func NewRouter(dependencies Dependencies, cfg config.HTTPConfig, logger *slog.Logger) *gin.Engine {
	engine := gin.New()
	engine.Use(middleware.RequestID())
	engine.Use(declaredBodyLimit(cfg.MaxBodyBytes))
	engine.Use(middleware.BodyLimit(cfg.MaxBodyBytes))
	engine.Use(requestDeadline(cfg.RequestTimeout))
	engine.Use(middleware.AccessLog(logger))
	engine.Use(middleware.Recovery(func(c *gin.Context) {
		handlers.WriteError(c, http.StatusInternalServerError, "internal_error", "An unexpected error occurred.")
	}))
	engine.GET("/livez", healthHandler(dependencies.Health, false))
	engine.GET("/healthz", healthHandler(dependencies.Health, false))
	engine.GET("/readyz", healthHandler(dependencies.Health, true))
	engine.GET("/health/live", healthHandler(dependencies.Health, false))
	engine.GET("/health/ready", healthHandler(dependencies.Health, true))
	handler := handlers.New(dependencies.Repositories, dependencies.Batches, dependencies.Changes)
	api := engine.Group("/api")
	api.POST("/repos", handler.CreateRepository)
	api.GET("/repos", handler.ListRepositories)
	api.GET("/repos/stats", handler.Statistics)
	api.GET("/repos/changes", handler.ListChanges)
	api.POST("/repos/refresh-all", handler.RefreshAll)
	api.GET("/repos/:id", handler.GetRepository)
	api.PATCH("/repos/:id", handler.UpdateRepository)
	api.DELETE("/repos/:id", handler.DeleteRepository)
	api.POST("/repos/:id/refresh", handler.RefreshRepository)
	api.GET("/batches/:id", handler.BatchStatus)
	return engine
}

func healthHandler(health interface {
	Live() bool
	Ready(context.Context) error
}, ready bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if health == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		if !ready && health.Live() {
			c.JSON(http.StatusOK, gin.H{"status": "ok"})
			return
		}
		if err := health.Ready(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}

func NewServer(handler http.Handler, cfg config.HTTPConfig) *http.Server {
	return &http.Server{Addr: ":" + strconv.Itoa(cfg.Port), Handler: handler, ReadHeaderTimeout: cfg.ReadHeaderTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout}
}

func requestDeadline(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if timeout <= 0 {
			c.Next()
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

func declaredBodyLimit(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit > 0 && c.Request.ContentLength > limit {
			handlers.WriteError(c, http.StatusRequestEntityTooLarge, "body_too_large", "The request body is too large.")
			return
		}
		c.Next()
	}
}
