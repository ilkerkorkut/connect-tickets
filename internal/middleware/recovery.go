package middleware

import (
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

func Recovery(render func(*gin.Context)) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				logging.Error(c.Request.Context(), "panic recovered", "panic", r, "stack", string(debug.Stack()), "route", c.FullPath(), "path", c.Request.URL.Path, "method", c.Request.Method)
				c.Abort()
				render(c)
			}
		}()
		c.Next()
	}
}
