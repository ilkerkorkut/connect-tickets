package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/internal/batch"
	"github.com/ilkerkorkut/connect-tickets/internal/changefeed"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/internal/repository"
)

type repositoryFake struct {
	create     func(context.Context, string, string) (repository.Repository, error)
	get        func(context.Context, string) (repository.Repository, error)
	list       func(context.Context, repository.ListInput) (repository.Page, error)
	update     func(context.Context, string, string) (repository.Repository, error)
	delete     func(context.Context, string) error
	refresh    func(context.Context, string) (repository.Repository, error)
	statistics func(context.Context) (repository.Statistics, error)
}

func (f repositoryFake) Create(ctx context.Context, owner, name string) (repository.Repository, error) {
	return f.create(ctx, owner, name)
}
func (f repositoryFake) Get(ctx context.Context, id string) (repository.Repository, error) {
	return f.get(ctx, id)
}
func (f repositoryFake) List(ctx context.Context, input repository.ListInput) (repository.Page, error) {
	return f.list(ctx, input)
}
func (f repositoryFake) UpdateNotes(ctx context.Context, id, notes string) (repository.Repository, error) {
	return f.update(ctx, id, notes)
}
func (f repositoryFake) Delete(ctx context.Context, id string) error { return f.delete(ctx, id) }
func (f repositoryFake) Refresh(ctx context.Context, id string) (repository.Repository, error) {
	return f.refresh(ctx, id)
}
func (f repositoryFake) Statistics(ctx context.Context) (repository.Statistics, error) {
	return f.statistics(ctx)
}

type batchFake struct {
	create func(context.Context) (uuid.UUID, error)
	status func(context.Context, uuid.UUID) (batch.Status, error)
}

func (f batchFake) CreateRefreshAll(ctx context.Context) (uuid.UUID, error) { return f.create(ctx) }
func (f batchFake) Status(ctx context.Context, id uuid.UUID) (batch.Status, error) {
	return f.status(ctx, id)
}

type changeFake struct {
	list func(context.Context, string, int) (changefeed.Page, error)
}

func (f changeFake) List(ctx context.Context, since string, limit int) (changefeed.Page, error) {
	return f.list(ctx, since, limit)
}

func newTestRouter(t *testing.T, repositories repositoryFake, batches batchFake, changes changeFake, cfg config.HTTPConfig) http.Handler {
	t.Helper()
	return NewRouter(Dependencies{Repositories: repositories, Batches: batches, Changes: changes}, cfg, nil)
}

func baseDependencies() (repositoryFake, batchFake, changeFake) {
	repo := repository.Repository{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Owner: "octo", Name: "hello", FullName: "octo/hello", HTMLURL: "https://github.com/octo/hello", FetchedAt: time.Unix(1, 0).UTC(), CreatedAt: time.Unix(2, 0).UTC(), UpdatedAt: time.Unix(3, 0).UTC()}
	repositories := repositoryFake{
		create: func(context.Context, string, string) (repository.Repository, error) { return repo, nil },
		get:    func(context.Context, string) (repository.Repository, error) { return repo, nil },
		list: func(context.Context, repository.ListInput) (repository.Page, error) {
			return repository.Page{Repositories: []repository.Repository{repo}, NextCursor: "next", HasMore: true}, nil
		},
		update: func(_ context.Context, _ string, notes string) (repository.Repository, error) {
			repo.Notes = notes
			return repo, nil
		},
		delete:  func(context.Context, string) error { return nil },
		refresh: func(context.Context, string) (repository.Repository, error) { return repo, nil },
		statistics: func(context.Context) (repository.Statistics, error) {
			return repository.Statistics{Total: 1, TotalStars: 5}, nil
		},
	}
	batchID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	batches := batchFake{
		create: func(context.Context) (uuid.UUID, error) { return batchID, nil },
		status: func(context.Context, uuid.UUID) (batch.Status, error) {
			return batch.Status{ID: batchID, Total: 2, Pending: 1, Succeeded: 1}, nil
		},
	}
	changes := changeFake{list: func(context.Context, string, int) (changefeed.Page, error) {
		return changefeed.Page{Changes: []changefeed.Change{{Cursor: "cursor", EventID: "event", Operation: "created", Repository: map[string]any{"id": repo.ID.String()}}}, NextCursor: "cursor"}, nil
	}}
	return repositories, batches, changes
}

func TestEndpoints(t *testing.T) {
	repositories, batches, changes := baseDependencies()
	handler := newTestRouter(t, repositories, batches, changes, config.HTTPConfig{RequestTimeout: time.Second})
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{"create", http.MethodPost, "/api/repos", `{"owner":"octo","name":"hello"}`, http.StatusCreated},
		{"list", http.MethodGet, "/api/repos?sort=stars_desc&limit=1", "", http.StatusOK},
		{"get", http.MethodGet, "/api/repos/11111111-1111-1111-1111-111111111111", "", http.StatusOK},
		{"patch", http.MethodPatch, "/api/repos/11111111-1111-1111-1111-111111111111", `{"notes":"note"}`, http.StatusOK},
		{"delete", http.MethodDelete, "/api/repos/11111111-1111-1111-1111-111111111111", "", http.StatusNoContent},
		{"refresh", http.MethodPost, "/api/repos/11111111-1111-1111-1111-111111111111/refresh", "", http.StatusOK},
		{"refresh all", http.MethodPost, "/api/repos/refresh-all", "", http.StatusAccepted},
		{"batch", http.MethodGet, "/api/batches/22222222-2222-2222-2222-222222222222", "", http.StatusOK},
		{"stats", http.MethodGet, "/api/repos/stats", "", http.StatusOK},
		{"changes", http.MethodGet, "/api/repos/changes?limit=1", "", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
		})
	}
}

func TestValidationAndSafeErrors(t *testing.T) {
	_, batches, changes := baseDependencies()
	badID := "not-a-uuid"
	githubFailure := &githubgateway.RequestError{Kind: githubgateway.ErrRateLimited, RateLimit: githubgateway.RateLimit{RetryAfter: 1500 * time.Millisecond}}
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		setup  func(*repositoryFake)
		status int
		code   string
	}{
		{"malformed json", http.MethodPost, "/api/repos", `{`, nil, http.StatusBadRequest, "invalid_json"},
		{"unknown field", http.MethodPost, "/api/repos", `{"owner":"octo","name":"hello","extra":true}`, nil, http.StatusBadRequest, "invalid_json"},
		{"missing owner", http.MethodPost, "/api/repos", `{"name":"hello"}`, nil, http.StatusBadRequest, "validation_failed"},
		{"bad limit", http.MethodGet, "/api/repos?limit=x", "", nil, http.StatusBadRequest, "validation_failed"},
		{"bad repository id", http.MethodGet, "/api/repos/" + badID, "", func(f *repositoryFake) {
			f.get = func(context.Context, string) (repository.Repository, error) {
				return repository.Repository{}, repository.ErrInvalidInput
			}
		}, http.StatusBadRequest, "validation_failed"},
		{"bad cursor", http.MethodGet, "/api/repos?cursor=bad", "", func(f *repositoryFake) {
			f.list = func(context.Context, repository.ListInput) (repository.Page, error) {
				return repository.Page{}, repository.ErrInvalidCursor
			}
		}, http.StatusBadRequest, "invalid_cursor"},
		{"missing repository", http.MethodGet, "/api/repos/" + badID, "", func(f *repositoryFake) {
			f.get = func(context.Context, string) (repository.Repository, error) {
				return repository.Repository{}, repository.ErrNotFound
			}
		}, http.StatusNotFound, "tracked_repo_not_found"},
		{"duplicate", http.MethodPost, "/api/repos", `{"owner":"octo","name":"hello"}`, func(f *repositoryFake) {
			f.create = func(context.Context, string, string) (repository.Repository, error) {
				return repository.Repository{}, repository.ErrDuplicate
			}
		}, http.StatusConflict, "repo_already_tracked"},
		{"github rate limit", http.MethodPost, "/api/repos", `{"owner":"octo","name":"hello"}`, func(f *repositoryFake) {
			f.create = func(context.Context, string, string) (repository.Repository, error) {
				return repository.Repository{}, githubFailure
			}
		}, http.StatusServiceUnavailable, "github_rate_limited"},
		{"safe internal error", http.MethodGet, "/api/repos/" + badID, "", func(f *repositoryFake) {
			f.get = func(context.Context, string) (repository.Repository, error) {
				return repository.Repository{}, errors.New("postgres://token@host raw failure")
			}
		}, http.StatusInternalServerError, "internal_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repositories, _, _ := baseDependencies()
			if tc.setup != nil {
				tc.setup(&repositories)
			}
			handler := newTestRouter(t, repositories, batches, changes, config.HTTPConfig{RequestTimeout: time.Second})
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != tc.code {
				t.Fatalf("code = %q, want %q", body.Error.Code, tc.code)
			}
			if tc.name == "github rate limit" && response.Header().Get("Retry-After") != "2" {
				t.Fatalf("Retry-After = %q, want 2", response.Header().Get("Retry-After"))
			}
			if strings.Contains(response.Body.String(), "postgres://") {
				t.Fatalf("unsafe error leaked: %s", response.Body.String())
			}
		})
	}
}

func TestRequestIDBodyLimitAndDeadline(t *testing.T) {
	repositories, batches, changes := baseDependencies()
	var deadlineSeen bool
	repositories.create = func(ctx context.Context, _, _ string) (repository.Repository, error) {
		_, deadlineSeen = ctx.Deadline()
		return repository.Repository{}, repository.ErrDuplicate
	}
	handler := newTestRouter(t, repositories, batches, changes, config.HTTPConfig{MaxBodyBytes: 20, RequestTimeout: time.Second})
	request := httptest.NewRequest(http.MethodPost, "/api/repos", strings.NewReader(`{"owner":"octo","name":"hello"}`))
	request.Header.Set("X-Request-ID", "request-123")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	if response.Header().Get("X-Request-ID") != "request-123" {
		t.Fatalf("request id = %q", response.Header().Get("X-Request-ID"))
	}

	handler = newTestRouter(t, repositories, batches, changes, config.HTTPConfig{RequestTimeout: time.Second})
	request = httptest.NewRequest(http.MethodPost, "/api/repos", strings.NewReader(`{"owner":"octo","name":"hello"}`))
	request.Header.Set("X-Request-ID", "request-123")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !deadlineSeen {
		t.Fatalf("status = %d deadline = %t", response.Code, deadlineSeen)
	}
	if !strings.Contains(response.Body.String(), "request-123") {
		t.Fatalf("request id missing from error: %s", response.Body.String())
	}
}

func TestRecoveryUsesSafeError(t *testing.T) {
	repositories, batches, changes := baseDependencies()
	repositories.get = func(context.Context, string) (repository.Repository, error) { panic("secret stack value") }
	handler := newTestRouter(t, repositories, batches, changes, config.HTTPConfig{})
	request := httptest.NewRequest(http.MethodGet, "/api/repos/11111111-1111-1111-1111-111111111111", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	if strings.Contains(response.Body.String(), "secret stack value") || !strings.Contains(response.Body.String(), "internal_error") {
		t.Fatalf("unsafe recovery body: %s", response.Body.String())
	}
}

func TestBatchFailureIsSafe(t *testing.T) {
	repositories, batches, changes := baseDependencies()
	batches.status = func(_ context.Context, id uuid.UUID) (batch.Status, error) {
		return batch.Status{ID: id, Failed: []batch.Failure{{RepositoryID: uuid.New(), Code: "database_error", Message: "postgres://token@host"}}}, nil
	}
	handler := newTestRouter(t, repositories, batches, changes, config.HTTPConfig{})
	request := httptest.NewRequest(http.MethodGet, "/api/batches/22222222-2222-2222-2222-222222222222", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "postgres://") || !strings.Contains(response.Body.String(), "refresh_failed") {
		t.Fatalf("unsafe batch response: %d %s", response.Code, response.Body.String())
	}
}
