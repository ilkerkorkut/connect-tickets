package middleware

import (
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

const requestIDHeader = "X-Request-ID"

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(requestIDHeader)
		if !requestIDPattern.MatchString(requestID) {
			requestID = uuid.NewString()
		}
		c.Request = c.Request.WithContext(logging.WithRequestID(c.Request.Context(), requestID))
		c.Header(requestIDHeader, requestID)
		c.Next()
	}
}
