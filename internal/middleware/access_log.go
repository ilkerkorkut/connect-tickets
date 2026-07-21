package middleware

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

const errorCodeKey = "http_error_code"

func AccessLog(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		if logger != nil {
			c.Request = c.Request.WithContext(logging.WithLogger(c.Request.Context(), logger))
		}
		started := time.Now()
		c.Next()
		if logger == nil {
			return
		}
		errorCode, _ := c.Get(errorCodeKey)
		logging.ScopedLogger(c.Request.Context(), logger).Info("http request completed", "method", c.Request.Method, "route", c.FullPath(), "path", c.Request.URL.Path, "status", c.Writer.Status(), "duration", time.Since(started), "remote_ip", c.ClientIP(), "user_agent", c.Request.UserAgent(), "error_code", errorCode)
	}
}

func SetErrorCode(c *gin.Context, code string) {
	c.Set(errorCodeKey, code)
}
