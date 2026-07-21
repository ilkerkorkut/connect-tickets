package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

func (h *Handler) RefreshAll(c *gin.Context) {
	id, err := h.batches.CreateRefreshAll(c.Request.Context())
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	logging.Event(c.Request.Context(), "refresh all scheduled", "batch_id", id)
	c.JSON(http.StatusAccepted, gin.H{"batch_id": id.String()})
}

func (h *Handler) BatchStatus(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		WriteError(c, http.StatusBadRequest, "validation_failed", "The request is invalid.", Detail{Field: "id", Reason: "invalid"})
		return
	}
	status, err := h.batches.Status(c.Request.Context(), id)
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	failed := make([]gin.H, 0, len(status.Failed))
	for _, failure := range status.Failed {
		code, message := safeFailure(failure.Code)
		failed = append(failed, gin.H{"repo_id": failure.RepositoryID.String(), "reason": gin.H{"code": code, "message": message}})
	}
	logging.Debug(c.Request.Context(), "batch status fetched", "batch_id", status.ID, "total", status.Total, "pending", status.Pending, "succeeded", status.Succeeded, "failed", len(failed))
	c.JSON(http.StatusOK, gin.H{"id": status.ID.String(), "total": status.Total, "pending": status.Pending, "succeeded": status.Succeeded, "failed": failed, "complete": status.CompletedAt != nil})
}

func safeFailure(code string) (string, string) {
	switch strings.TrimSpace(code) {
	case "github_repo_not_found":
		return "github_repo_not_found", "The repository was not found on GitHub."
	case "github_rate_limited":
		return "github_rate_limited", "GitHub is temporarily rate limited."
	case "github_unavailable":
		return "github_unavailable", "GitHub is temporarily unavailable."
	case "github_timeout":
		return "github_timeout", "GitHub did not respond in time."
	case "cache_unavailable":
		return "cache_unavailable", "The cache service is temporarily unavailable."
	default:
		return "refresh_failed", "The repository refresh failed."
	}
}
