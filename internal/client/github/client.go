package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/ilkerkorkut/connect-tickets/config"
)

const maxResponseBodyBytes int64 = 1 << 20

type Client struct {
	resty     *resty.Client
	baseURL   *url.URL
	token     string
	userAgent string
	timeout   time.Duration
	now       func() time.Time
}

func NewClient(cfg config.GitHubConfig, httpClient *http.Client) (*Client, error) {
	if cfg.Timeout <= 0 || strings.TrimSpace(cfg.UserAgent) == "" {
		return nil, ErrInvalidConfig
	}
	baseURL, err := url.Parse(cfg.BaseURL)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return nil, ErrInvalidConfig
	}
	if httpClient == nil {
		httpClient = newHTTPClient(cfg)
	}
	client := resty.NewWithClient(httpClient).
		SetBaseURL(strings.TrimRight(baseURL.String(), "/")).
		SetTimeout(cfg.Timeout).
		SetResponseBodyLimit(int(maxResponseBodyBytes)).
		SetHeader("Accept", "application/vnd.github+json").
		SetHeader("User-Agent", cfg.UserAgent)
	if cfg.Token != "" {
		client.SetAuthToken(cfg.Token)
	}
	return &Client{
		resty:     client,
		baseURL:   baseURL,
		token:     cfg.Token,
		userAgent: cfg.UserAgent,
		timeout:   cfg.Timeout,
		now:       time.Now,
	}, nil
}

func newHTTPClient(cfg config.GitHubConfig) *http.Client {
	dialTimeout := min(cfg.Timeout, 10*time.Second)

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxIdleConnsPerHost * 2,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   dialTimeout,
		ResponseHeaderTimeout: cfg.Timeout,
		ExpectContinueTimeout: time.Second,
	}

	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c *Client) FetchRepository(ctx context.Context, owner, name string) (Repository, error) {
	if c == nil || c.resty == nil || c.baseURL == nil {
		return Repository{}, requestError(ErrInvalidConfig, 0, RateLimit{}, "client is not initialized", nil)
	}
	if !validPathSegment(owner) || !validPathSegment(name) {
		return Repository{}, requestError(ErrInvalidRequest, 0, RateLimit{}, "owner and name must be non-empty path segments", nil)
	}
	if err := ctx.Err(); err != nil {
		return Repository{}, cancellationError(err)
	}

	requestContext, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.resty.R().SetContext(requestContext).Get("/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(name))
	if err != nil {
		if errors.Is(err, resty.ErrResponseBodyTooLarge) {
			return Repository{}, requestError(ErrInvalidResponse, 0, RateLimit{}, "response body exceeds limit", err)
		}
		return Repository{}, c.transportError(ctx, requestContext, err)
	}

	if !response.IsSuccess() {
		return Repository{}, c.statusError(response.RawResponse, response.Body())
	}

	var payload repositoryResponse
	decoder := json.NewDecoder(strings.NewReader(string(response.Body())))
	if err := decoder.Decode(&payload); err != nil {
		return Repository{}, requestError(ErrInvalidResponse, response.StatusCode(), rateLimitFrom(response.RawResponse, c.now()), "decode repository response", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Repository{}, requestError(ErrInvalidResponse, response.StatusCode(), rateLimitFrom(response.RawResponse, c.now()), "repository response contains trailing data", err)
	}
	if payload.Owner.Login == nil || payload.Name == nil || payload.FullName == nil || payload.Stargazers == nil || payload.HTMLURL == nil || *payload.Owner.Login == "" || *payload.Name == "" || *payload.FullName == "" || *payload.HTMLURL == "" || *payload.Stargazers < 0 {
		return Repository{}, requestError(ErrInvalidResponse, response.StatusCode(), rateLimitFrom(response.RawResponse, c.now()), "repository response is missing required fields", nil)
	}

	return Repository{
		Owner:       *payload.Owner.Login,
		Name:        *payload.Name,
		FullName:    *payload.FullName,
		Description: payload.Description,
		Stars:       *payload.Stargazers,
		Language:    payload.Language,
		HTMLURL:     *payload.HTMLURL,
		FetchedAt:   c.now().UTC(),
	}, nil
}

func (c *Client) RefreshRepository(ctx context.Context, owner, name string) (Repository, error) {
	return c.FetchRepository(ctx, owner, name)
}

func (c *Client) Cooldown(context.Context) (time.Time, error) {
	return time.Time{}, nil
}

func (c *Client) transportError(callerContext, requestContext context.Context, err error) error {
	if callerErr := callerContext.Err(); callerErr != nil {
		return cancellationError(callerErr)
	}
	if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
		return requestError(ErrTimeout, 0, RateLimit{}, "request deadline exceeded", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return requestError(ErrTimeout, 0, RateLimit{}, "http client deadline exceeded", err)
	}
	return requestError(ErrNetwork, 0, RateLimit{}, "transport request failed", err)
}

func (c *Client) statusError(response *http.Response, body []byte) error {
	diagnostic := boundedDiagnostic(body)
	metadata := rateLimitFrom(response, c.now())
	switch {
	case response.StatusCode == http.StatusNotFound:
		return requestError(ErrNotFound, response.StatusCode, metadata, diagnostic, nil)
	case response.StatusCode == http.StatusUnauthorized:
		return requestError(ErrUnauthorized, response.StatusCode, metadata, diagnostic, nil)
	case response.StatusCode == http.StatusTooManyRequests:
		return requestError(ErrRateLimited, response.StatusCode, metadata, diagnostic, nil)
	case response.StatusCode == http.StatusForbidden && isRateLimited(response, diagnostic):
		return requestError(ErrRateLimited, response.StatusCode, metadata, diagnostic, nil)
	case response.StatusCode == http.StatusForbidden:
		return requestError(ErrUnauthorized, response.StatusCode, metadata, diagnostic, nil)
	case response.StatusCode >= http.StatusInternalServerError:
		return requestError(ErrServer, response.StatusCode, metadata, diagnostic, nil)
	default:
		return requestError(ErrInvalidResponse, response.StatusCode, metadata, diagnostic, nil)
	}
}

func validPathSegment(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.Contains(value, "/")
}

func boundedDiagnostic(body []byte) string {
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil && strings.TrimSpace(payload.Message) != "" {
		return truncateDiagnostic(payload.Message)
	}
	return truncateDiagnostic(string(body))
}

func truncateDiagnostic(value string) string {
	value = strings.TrimSpace(value)
	const maxDiagnosticBytes = 1024
	if len(value) > maxDiagnosticBytes {
		return value[:maxDiagnosticBytes]
	}
	return value
}

func isRateLimited(response *http.Response, diagnostic string) bool {
	if response.Header.Get("X-RateLimit-Remaining") == "0" {
		return true
	}
	return strings.Contains(strings.ToLower(diagnostic), "rate limit")
}

func cancellationError(cause error) error {
	return requestError(ErrCanceled, 0, RateLimit{}, "caller canceled request", cause)
}
