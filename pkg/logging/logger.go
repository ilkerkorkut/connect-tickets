package logging

import (
	"errors"
	"io"
	"log/slog"
	"strings"

	"github.com/ilkerkorkut/connect-tickets/config"
)

var (
	ErrUnknownLevel  = errors.New("unknown log level")
	ErrUnknownFormat = errors.New("unknown log format")
)

var sensitiveKeys = map[string]struct{}{
	"token":         {},
	"tokens":        {},
	"password":      {},
	"passwd":        {},
	"secret":        {},
	"secrets":       {},
	"authorization": {},
	"auth":          {},
	"api_key":       {},
	"apikey":        {},
	"access_token":  {},
	"refresh_token": {},
	"database_url":  {},
	"db_url":        {},
	"dsn":           {},
	"credentials":   {},
	"credential":    {},
	"private_key":   {},
	"privatekey":    {},
	"cookie":        {},
	"cookies":       {},
	"session":       {},
}

const redactedValue = "[redacted]"

func NewLogger(cfg config.LogConfig, out io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: redactAttr,
	}

	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "", "json":
		handler = slog.NewJSONHandler(out, opts)
	case "text":
		handler = slog.NewTextHandler(out, opts)
	default:
		return nil, ErrUnknownFormat
	}

	return slog.New(handler), nil
}

func redactAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && isSensitiveKey(a.Key) {
		a.Value = slog.StringValue(redactedValue)
		return a
	}
	if a.Value.Kind() == slog.KindGroup {
		attrs := a.Value.Group()
		redacted := make([]slog.Attr, len(attrs))
		for i, child := range attrs {
			redacted[i] = redactAttr(groups, child)
		}
		a.Value = slog.GroupValue(redacted...)
		return a
	}
	if a.Value.Kind() == slog.KindAny {
		if redacted, ok := redactAny(a.Value.Any()); ok {
			a.Value = slog.AnyValue(redacted)
		}
	}
	return a
}

func redactAny(value any) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, child := range v {
			if isSensitiveKey(k) {
				out[k] = redactedValue
				continue
			}
			if nested, ok := redactAny(child); ok {
				out[k] = nested
			} else {
				out[k] = child
			}
		}
		return out, true
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			if nested, ok := redactAny(child); ok {
				out[i] = nested
			} else {
				out[i] = child
			}
		}
		return out, true
	}
	return nil, false
}

func isSensitiveKey(key string) bool {
	_, ok := sensitiveKeys[strings.ToLower(strings.TrimSpace(key))]
	return ok
}

func parseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, ErrUnknownLevel
	}
}
