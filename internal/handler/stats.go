package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

func (h *Handler) Statistics(c *gin.Context) {
	stats, err := h.repositories.Statistics(c.Request.Context())
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	logging.Debug(c.Request.Context(), "statistics computed", "total", stats.Total, "total_stars", stats.TotalStars)
	c.JSON(http.StatusOK, gin.H{"total": stats.Total, "total_stars": stats.TotalStars, "top_language": stats.TopLanguage})
}
