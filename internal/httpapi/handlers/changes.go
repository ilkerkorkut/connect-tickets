package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *Handler) ListChanges(c *gin.Context) {
	limit, ok := parseLimit(c, "limit")
	if !ok {
		return
	}
	page, err := h.changes.List(c.Request.Context(), c.Query("since"), limit)
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	changes := make([]gin.H, 0, len(page.Changes))
	for _, change := range page.Changes {
		changes = append(changes, gin.H{"cursor": change.Cursor, "event_id": change.EventID, "operation": change.Operation, "repo": change.Repository})
	}
	c.JSON(http.StatusOK, gin.H{"changes": changes, "next_cursor": page.NextCursor, "has_more": page.HasMore})
}
