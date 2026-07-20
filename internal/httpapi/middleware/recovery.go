package middleware

import "github.com/gin-gonic/gin"

func Recovery(render func(*gin.Context)) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recover() != nil {
				c.Abort()
				render(c)
			}
		}()
		c.Next()
	}
}
