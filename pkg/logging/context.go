package logging

import (
	"context"
	"log/slog"
)

type ctxKey string

const (
	keyRequestID     ctxKey = "request_id"
	keyJobID         ctxKey = "job_id"
	keyEventID       ctxKey = "event_id"
	keyBatchID       ctxKey = "batch_id"
	keyBatchJobID    ctxKey = "batch_job_id"
	keyRepoID        ctxKey = "repo_id"
	keyAttempt       ctxKey = "attempt"
	keyWorkerID      ctxKey = "worker_id"
	keyPublisherID   ctxKey = "publisher_id"
	keyConsumerGroup ctxKey = "consumer_group"
	keyTopic         ctxKey = "topic"
	keyPartition     ctxKey = "partition"
	keyOffset        ctxKey = "offset"
	keyLogger        ctxKey = "logger"
)

func WithValue(ctx context.Context, key, value string) context.Context {
	if value == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKey(key), value)
}

func valueFrom(ctx context.Context, key string) string {
	if v, ok := ctx.Value(ctxKey(key)).(string); ok {
		return v
	}
	return ""
}

func WithRequestID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyRequestID), id)
}

func RequestIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyRequestID))
}

func WithJobID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyJobID), id)
}

func JobIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyJobID))
}

func WithBatchJobID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyBatchJobID), id)
}

func BatchJobIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyBatchJobID))
}

func WithWorkerID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyWorkerID), id)
}

func WorkerIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyWorkerID))
}

func WithBatchID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyBatchID), id)
}

func BatchIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyBatchID))
}

func WithRepoID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyRepoID), id)
}

func RepoIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyRepoID))
}

func WithEventID(ctx context.Context, id string) context.Context {
	return WithValue(ctx, string(keyEventID), id)
}

func EventIDFrom(ctx context.Context) string {
	return valueFrom(ctx, string(keyEventID))
}

func WithAttempt(ctx context.Context, value string) context.Context {
	return WithValue(ctx, string(keyAttempt), value)
}

func WithTopic(ctx context.Context, value string) context.Context {
	return WithValue(ctx, string(keyTopic), value)
}

func WithPartition(ctx context.Context, value string) context.Context {
	return WithValue(ctx, string(keyPartition), value)
}

func WithOffset(ctx context.Context, value string) context.Context {
	return WithValue(ctx, string(keyOffset), value)
}

type scope struct {
	keys   []string
	values []string
}

func Scope() *scope {
	return &scope{}
}

func (s *scope) Add(key, value string) *scope {
	if value == "" {
		return s
	}
	s.keys = append(s.keys, key)
	s.values = append(s.values, value)
	return s
}

func (s *scope) Logger(base *slog.Logger) *slog.Logger {
	if base == nil || len(s.values) == 0 {
		return base
	}
	args := make([]any, 0, len(s.values)*2)
	for i, v := range s.values {
		args = append(args, s.keys[i], v)
	}
	return base.With(args...)
}

func ScopedLogger(ctx context.Context, base *slog.Logger) *slog.Logger {
	return Scope().
		Add(string(keyRequestID), RequestIDFrom(ctx)).
		Add(string(keyJobID), JobIDFrom(ctx)).
		Add(string(keyBatchJobID), BatchJobIDFrom(ctx)).
		Add(string(keyBatchID), BatchIDFrom(ctx)).
		Add(string(keyRepoID), RepoIDFrom(ctx)).
		Add(string(keyEventID), EventIDFrom(ctx)).
		Add(string(keyWorkerID), WorkerIDFrom(ctx)).
		Add(string(keyAttempt), valueFrom(ctx, string(keyAttempt))).
		Add(string(keyTopic), valueFrom(ctx, string(keyTopic))).
		Add(string(keyPartition), valueFrom(ctx, string(keyPartition))).
		Add(string(keyOffset), valueFrom(ctx, string(keyOffset))).
		Logger(base)
}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, keyLogger, logger)
}

func LoggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(keyLogger).(*slog.Logger); ok {
		return l
	}
	return nil
}
