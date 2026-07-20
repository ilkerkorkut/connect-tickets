package route

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/service/health"
)

func TestHealthRoutesSeparateLivenessAndReadiness(t *testing.T) {
	service := health.New(health.Check{Name: "database", Timeout: 1, Run: func(context.Context) error { return nil }})
	router := NewRouter(Dependencies{Health: service}, config.HTTPConfig{}, nil)
	for _, test := range []struct {
		path string
		want int
	}{
		{"/livez", http.StatusOK},
		{"/healthz", http.StatusOK},
		{"/health/live", http.StatusOK},
		{"/readyz", http.StatusServiceUnavailable},
		{"/health/ready", http.StatusServiceUnavailable},
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != test.want {
			t.Fatalf("%s status=%d want=%d", test.path, response.Code, test.want)
		}
	}
	service.SetReady(true)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("ready status=%d", response.Code)
	}
}

func TestReadinessFailureReturnsSafeResponse(t *testing.T) {
	service := health.New(health.Check{Name: "redis", Timeout: 1, Run: func(context.Context) error { return errors.New("redis password secret") }})
	service.SetReady(true)
	router := NewRouter(Dependencies{Health: service}, config.HTTPConfig{}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"status\":\"not_ready\"}" {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}
