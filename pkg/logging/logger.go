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

func NewLogger(cfg config.LogConfig, out io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level: level,
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
