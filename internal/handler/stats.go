package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *Handler) Statistics(c *gin.Context) {
	stats, err := h.repositories.Statistics(c.Request.Context())
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"total": stats.Total, "total_stars": stats.TotalStars, "top_language": stats.TopLanguage})
}
