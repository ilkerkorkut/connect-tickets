package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/internal/repository"
)

const maxNotesLength = 4000

type createRepositoryRequest struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type updateNotesRequest struct {
	Notes string `json:"notes"`
}

type repositoryResponse struct {
	ID          string    `json:"id"`
	Owner       string    `json:"owner"`
	Name        string    `json:"name"`
	FullName    string    `json:"full_name"`
	Description *string   `json:"description"`
	Stars       int       `json:"stars"`
	Language    *string   `json:"language"`
	HTMLURL     string    `json:"html_url"`
	Notes       string    `json:"notes"`
	FetchedAt   time.Time `json:"fetched_at"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (h *Handler) CreateRepository(c *gin.Context) {
	var request createRepositoryRequest
	if !decodeJSON(c, &request) {
		return
	}
	if strings.TrimSpace(request.Owner) == "" || strings.TrimSpace(request.Name) == "" {
		WriteError(c, http.StatusBadRequest, "validation_failed", "The request is invalid.", validationDetails(request.Owner, request.Name)...)
		return
	}
	repo, err := h.repositories.Create(c.Request.Context(), request.Owner, request.Name)
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	c.JSON(http.StatusCreated, repositoryJSON(repo))
}

func (h *Handler) ListRepositories(c *gin.Context) {
	limit, ok := parseLimit(c, "limit")
	if !ok {
		return
	}
	page, err := h.repositories.List(c.Request.Context(), repository.ListInput{Language: c.Query("language"), Sort: repository.Sort(c.Query("sort")), Limit: limit, Cursor: c.Query("cursor")})
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	repos := make([]repositoryResponse, 0, len(page.Repositories))
	for _, repo := range page.Repositories {
		repos = append(repos, repositoryJSON(repo))
	}
	c.JSON(http.StatusOK, gin.H{"repos": repos, "next_cursor": page.NextCursor, "has_more": page.HasMore})
}

func (h *Handler) GetRepository(c *gin.Context) {
	repo, err := h.repositories.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, repositoryJSON(repo))
}

func (h *Handler) UpdateRepository(c *gin.Context) {
	var request updateNotesRequest
	if !decodeJSON(c, &request) {
		return
	}
	if len(request.Notes) > maxNotesLength {
		WriteError(c, http.StatusBadRequest, "validation_failed", "The request is invalid.", Detail{Field: "notes", Reason: "too_long"})
		return
	}
	repo, err := h.repositories.UpdateNotes(c.Request.Context(), c.Param("id"), request.Notes)
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, repositoryJSON(repo))
}

func (h *Handler) DeleteRepository(c *gin.Context) {
	if err := h.repositories.Delete(c.Request.Context(), c.Param("id")); err != nil {
		WriteServiceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) RefreshRepository(c *gin.Context) {
	repo, err := h.repositories.Refresh(c.Request.Context(), c.Param("id"))
	if err != nil {
		WriteServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, repositoryJSON(repo))
}

func decodeJSON(c *gin.Context, target any) bool {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			WriteError(c, http.StatusRequestEntityTooLarge, "body_too_large", "The request body is too large.")
		} else {
			WriteError(c, http.StatusBadRequest, "invalid_json", "The request body must be valid JSON.")
		}
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		WriteError(c, http.StatusBadRequest, "invalid_json", "The request body must contain one JSON value.")
		return false
	}
	return true
}

func parseLimit(c *gin.Context, key string) (int, bool) {
	value := c.Query(key)
	if value == "" {
		return 0, true
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 {
		WriteError(c, http.StatusBadRequest, "validation_failed", "The request is invalid.", Detail{Field: key, Reason: "invalid"})
		return 0, false
	}
	return limit, true
}

func validationDetails(owner, name string) []Detail {
	details := make([]Detail, 0, 2)
	if strings.TrimSpace(owner) == "" {
		details = append(details, Detail{Field: "owner", Reason: "required"})
	}
	if strings.TrimSpace(name) == "" {
		details = append(details, Detail{Field: "name", Reason: "required"})
	}
	return details
}

func repositoryJSON(repo repository.Repository) repositoryResponse {
	return repositoryResponse{ID: repo.ID.String(), Owner: repo.Owner, Name: repo.Name, FullName: repo.FullName, Description: repo.Description, Stars: repo.Stars, Language: repo.Language, HTMLURL: repo.HTMLURL, Notes: repo.Notes, FetchedAt: repo.FetchedAt, CreatedAt: repo.CreatedAt, UpdatedAt: repo.UpdatedAt}
}
