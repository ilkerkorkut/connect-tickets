package github

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

func TestFetchRepositoryMapsFieldsAndHeaders(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/repos/openai/gpt" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer token-value" {
			t.Errorf("Authorization = %q", got)
		}
		if got := request.Header.Get("User-Agent"); got != "test-agent" {
			t.Errorf("User-Agent = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"gpt","full_name":"openai/gpt","description":null,"stargazers_count":42,"language":"Go","html_url":"https://github.com/openai/gpt","owner":{"login":"openai"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, "token-value")
	now := time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return now }

	repository, err := client.FetchRepository(context.Background(), "openai", "gpt")
	if err != nil {
		t.Fatalf("FetchRepository() error = %v", err)
	}
	if repository.Owner != "openai" || repository.Name != "gpt" || repository.FullName != "openai/gpt" || repository.Stars != 42 || repository.HTMLURL != "https://github.com/openai/gpt" {
		t.Fatalf("repository = %#v", repository)
	}
	if repository.Description != nil || repository.Language == nil || *repository.Language != "Go" {
		t.Fatalf("optional fields = description:%v language:%v", repository.Description, repository.Language)
	}
	if !repository.FetchedAt.Equal(now) {
		t.Errorf("FetchedAt = %s, want %s", repository.FetchedAt, now)
	}
}

func TestFetchRepositoryClassifiesHTTPFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		status     int
		headers    map[string]string
		body       string
		want       error
		wantRetry  time.Duration
		wantRemain int
	}{
		{name: "not found", status: http.StatusNotFound, want: ErrNotFound},
		{name: "unauthorized", status: http.StatusUnauthorized, want: ErrUnauthorized},
		{name: "forbidden not rate limited", status: http.StatusForbidden, body: `{"message":"forbidden"}`, want: ErrUnauthorized},
		{name: "forbidden rate limited", status: http.StatusForbidden, headers: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1784541600"}, want: ErrRateLimited, wantRemain: 0},
		{name: "too many requests", status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "12"}, want: ErrRateLimited, wantRetry: 12 * time.Second},
		{name: "server error", status: http.StatusServiceUnavailable, want: ErrServer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for key, value := range tc.headers {
					w.Header().Set(key, value)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := newTestClient(t, server.URL, "")
			_, err := client.FetchRepository(context.Background(), "openai", "gpt")
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want errors.Is(%v)", err, tc.want)
			}
			var requestError *RequestError
			if !errors.As(err, &requestError) {
				t.Fatalf("error %T does not expose RequestError", err)
			}
			if requestError.Status != tc.status {
				t.Errorf("status = %d, want %d", requestError.Status, tc.status)
			}
			if requestError.RateLimit.RetryAfter != tc.wantRetry {
				t.Errorf("RetryAfter = %s, want %s", requestError.RateLimit.RetryAfter, tc.wantRetry)
			}
			if _, present := tc.headers["X-RateLimit-Remaining"]; present && (requestError.RateLimit.Remaining == nil || *requestError.RateLimit.Remaining != tc.wantRemain) {
				t.Errorf("Remaining = %v, want %d", requestError.RateLimit.Remaining, tc.wantRemain)
			}
		})
	}
}

func TestFetchRepositoryInvalidResponseAndDiagnosticLimit(t *testing.T) {
	t.Parallel()

	t.Run("invalid JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"name":`))
		}))
		defer server.Close()

		_, err := newTestClient(t, server.URL, "").FetchRepository(context.Background(), "openai", "gpt")
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want invalid response", err)
		}
	})

	t.Run("oversized body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat("x", int(maxResponseBodyBytes)+1)))
		}))
		defer server.Close()

		_, err := newTestClient(t, server.URL, "").FetchRepository(context.Background(), "openai", "gpt")
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("error = %v, want invalid response", err)
		}
	})

	t.Run("bounded diagnostic", func(t *testing.T) {
		body := strings.Repeat("x", 2048)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()

		_, err := newTestClient(t, server.URL, "").FetchRepository(context.Background(), "openai", "gpt")
		var requestError *RequestError
		if !errors.As(err, &requestError) {
			t.Fatalf("error %T does not expose RequestError", err)
		}
		if len(requestError.Diagnostic) != 1024 || strings.Contains(err.Error(), body) {
			t.Fatalf("diagnostic is not bounded and safe: len=%d error=%q", len(requestError.Diagnostic), err)
		}
	})
}

func TestFetchRepositoryDistinguishesCancellationAndTimeout(t *testing.T) {
	t.Parallel()

	client := newTestClientWithHTTP(t, "https://example.test", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})})
	client.timeout = 20 * time.Millisecond

	_, err := client.FetchRepository(context.Background(), "openai", "gpt")
	if !errors.Is(err, ErrTimeout) || errors.Is(err, ErrCanceled) {
		t.Fatalf("timeout error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.FetchRepository(ctx, "openai", "gpt")
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC)
	if got, ok := ParseRetryAfter("7", now); !ok || got != 7*time.Second {
		t.Fatalf("delta parse = %s, %v", got, ok)
	}
	when := now.Add(3 * time.Minute)
	if got, ok := ParseRetryAfter(when.Format(http.TimeFormat), now); !ok || got != 3*time.Minute {
		t.Fatalf("date parse = %s, %v", got, ok)
	}
	if _, ok := ParseRetryAfter("invalid", now); ok {
		t.Fatal("invalid retry-after parsed successfully")
	}
}

func newTestClient(t *testing.T, baseURL, token string) *Client {
	t.Helper()
	return newTestClientWithHTTP(t, baseURL, nil, token)
}

func newTestClientWithHTTP(t *testing.T, baseURL string, client *http.Client, tokens ...string) *Client {
	t.Helper()
	token := ""
	if len(tokens) > 0 {
		token = tokens[0]
	}
	githubClient, err := NewClient(config.GitHubConfig{
		BaseURL:             baseURL,
		Token:               token,
		Timeout:             time.Second,
		UserAgent:           "test-agent",
		MaxIdleConns:        2,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     time.Second,
	}, client)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return githubClient
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
