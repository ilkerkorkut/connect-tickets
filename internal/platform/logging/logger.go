package logging

import (
	"errors"
	"io"
	"log/slog"
	"strings"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

var (
	ErrUnknownLevel  = errors.New("unknown log level")
	ErrUnknownFormat = errors.New("unknown log format")
)

var redactedAttrKeys = map[string]struct{}{
	"token":               {},
	"authorization":       {},
	"password":            {},
	"secret":              {},
	"credential":          {},
	"credentials":         {},
	"database_url":        {},
	"redis_password":      {},
	"kafka_sasl_password": {},
	"auth":                {},
	"api_key":             {},
	"apikey":              {},
	"private_key":         {},
}

func NewLogger(cfg config.LogConfig, out io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: redactAttrs,
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

func redactAttrs(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	key := strings.ToLower(a.Key)
	if _, sensitive := redactedAttrKeys[key]; sensitive {
		return slog.Attr{Key: a.Key, Value: slog.StringValue("[redacted]")}
	}
	return a
}
