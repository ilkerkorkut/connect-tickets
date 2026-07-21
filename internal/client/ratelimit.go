package client

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

type RateLimit struct {
	Status     int
	RetryAfter time.Duration
	Remaining  *int
	ResetAt    *time.Time
}

func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}

	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}

	if wait := when.Sub(now); wait > 0 {
		return wait, true
	}
	return 0, true
}

func RateLimitFrom(response *http.Response, now time.Time) RateLimit {
	metadata := RateLimit{Status: response.StatusCode}
	if retryAfter, ok := ParseRetryAfter(response.Header.Get("Retry-After"), now); ok {
		metadata.RetryAfter = retryAfter
	}
	if remaining, err := strconv.Atoi(response.Header.Get("X-RateLimit-Remaining")); err == nil && remaining >= 0 {
		metadata.Remaining = &remaining
	}
	if reset, err := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil && reset >= 0 {
		resetAt := time.Unix(reset, 0).UTC()
		metadata.ResetAt = &resetAt
	}
	return metadata
}
