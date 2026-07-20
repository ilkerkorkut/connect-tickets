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

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

const maxResponseBodyBytes int64 = 1 << 20

var errResponseBodyTooLarge = errors.New("github response body exceeds limit")

type Client struct {
	httpClient *http.Client
	baseURL    *url.URL
	token      string
	userAgent  string
	timeout    time.Duration
	now        func() time.Time
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
	return &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		token:      cfg.Token,
		userAgent:  cfg.UserAgent,
		timeout:    cfg.Timeout,
		now:        time.Now,
	}, nil
}

func newHTTPClient(cfg config.GitHubConfig) *http.Client {
	dialTimeout := cfg.Timeout
	if dialTimeout > 10*time.Second {
		dialTimeout = 10 * time.Second
	}
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
	if c == nil || c.httpClient == nil || c.baseURL == nil {
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
	requestURL := *c.baseURL
	requestURL.Path = strings.TrimRight(c.baseURL.Path, "/") + "/repos/" + owner + "/" + name
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return Repository{}, requestError(ErrInvalidRequest, 0, RateLimit{}, "build request", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("User-Agent", c.userAgent)
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return Repository{}, c.transportError(ctx, requestContext, err)
	}

	body, err := readBody(response.Body)
	closeErr := response.Body.Close()
	if err != nil {
		if errors.Is(err, errResponseBodyTooLarge) {
			return Repository{}, requestError(ErrInvalidResponse, response.StatusCode, rateLimitFrom(response, c.now()), "response body exceeds limit", err)
		}
		return Repository{}, requestError(ErrNetwork, response.StatusCode, rateLimitFrom(response, c.now()), "read response body", err)
	}
	if closeErr != nil {
		return Repository{}, requestError(ErrNetwork, response.StatusCode, rateLimitFrom(response, c.now()), "close response body", closeErr)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return Repository{}, c.statusError(response, body)
	}

	var payload repositoryResponse
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	if err := decoder.Decode(&payload); err != nil {
		return Repository{}, requestError(ErrInvalidResponse, response.StatusCode, rateLimitFrom(response, c.now()), "decode repository response", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Repository{}, requestError(ErrInvalidResponse, response.StatusCode, rateLimitFrom(response, c.now()), "repository response contains trailing data", err)
	}
	if payload.Owner.Login == nil || payload.Name == nil || payload.FullName == nil || payload.Stargazers == nil || payload.HTMLURL == nil || *payload.Owner.Login == "" || *payload.Name == "" || *payload.FullName == "" || *payload.HTMLURL == "" || *payload.Stargazers < 0 {
		return Repository{}, requestError(ErrInvalidResponse, response.StatusCode, rateLimitFrom(response, c.now()), "repository response is missing required fields", nil)
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

func readBody(body io.Reader) ([]byte, error) {
	contents, err := io.ReadAll(io.LimitReader(body, maxResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > maxResponseBodyBytes {
		return nil, errResponseBodyTooLarge
	}
	return contents, nil
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
