package worker

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

type Store interface {
	RecordInvalid(context.Context, string, []byte, string, time.Time) error
	Claim(context.Context, kafka.RefreshMessage, string, time.Time, time.Duration) (Job, ClaimState, error)
	LoadRepository(context.Context, Job) (Repository, error)
	StartAttempt(context.Context, Job, string, time.Time, int) (int, error)
	CompleteSuccess(context.Context, Job, string, int, client.Repository, time.Time) error
	CompleteFailure(context.Context, Job, string, int, kafka.RefreshMessage, string, time.Time) error
	ScheduleRetry(context.Context, Job, string, int, kafka.RefreshMessage, string, time.Time, time.Time) error
}

type Consumer struct {
	store       Store
	github      client.GitHub
	owner       string
	lease       time.Duration
	maxAttempts int
	now         func() time.Time
	jitter      func(time.Duration) time.Duration
}

type processingResult struct {
	result    string
	errorCode string
}

func NewConsumer(store Store, github client.GitHub, owner string, lease time.Duration, maxAttempts int) (*Consumer, error) {
	if store == nil || github == nil || owner == "" || lease <= 0 || maxAttempts < 1 {
		return nil, ErrInvalidMessage
	}
	if maxAttempts > 3 {
		maxAttempts = 3
	}
	return &Consumer{store: store, github: github, owner: owner, lease: lease, maxAttempts: maxAttempts, now: time.Now, jitter: randomJitter}, nil
}

func (c *Consumer) Process(ctx context.Context, record *sarama.ConsumerMessage) (kafka.Settlement, error) {
	if c == nil || record == nil {
		return kafka.Settlement{}, ErrInvalidMessage
	}
	started := time.Now()
	ctx = recordContext(ctx, record)
	result, err := c.process(ctx, record)
	c.log(ctx, started, result, err)
	if err != nil {
		return kafka.Settlement{}, err
	}
	return kafka.Settlement{Commit: true}, nil
}

func (c *Consumer) process(ctx context.Context, record *sarama.ConsumerMessage) (processingResult, error) {
	message, err := kafka.DecodeRefresh(record.Value, 0)
	if err != nil {
		logging.Debug(ctx, "refresh payload invalid")
		return c.recordInvalid(ctx, invalidRecordID(record.Value), record.Value, "invalid_refresh_message", c.now().UTC())
	}
	ctx = messageContext(ctx, message, c.owner)
	now := c.now().UTC()
	job, state, err := c.store.Claim(ctx, message, c.owner, now, c.lease)
	if err != nil {
		return processingResult{}, err
	}
	logging.Debug(ctx, "refresh job claimed", "claim_state", claimStateName(state))
	if state == ClaimMissing {
		return c.recordInvalid(ctx, message.EventID, record.Value, "refresh_job_not_found", now)
	}
	if state == ClaimTerminal || state == ClaimDeferred || state == ClaimBusy {
		return processingResult{result: "duplicate_or_deferred"}, nil
	}
	if state != Claimed {
		return processingResult{}, ErrClaimLost
	}
	if err := ctx.Err(); err != nil {
		return processingResult{}, err
	}
	return c.processClaimed(ctx, job, message, now)
}

func (c *Consumer) processClaimed(ctx context.Context, job Job, message kafka.RefreshMessage, now time.Time) (processingResult, error) {
	cooldown, err := c.github.Cooldown(ctx)
	if err != nil {
		return processingResult{}, err
	}
	if cooldown.After(now) {
		logging.Debug(ctx, "refresh deferred by cooldown", "cooldown_until", cooldown)
		return c.scheduleRetry(ctx, job, message, 0, "github_rate_limited", cooldown.Add(boundedJitter(c.jitter, time.Second)), now)
	}
	repository, err := c.store.LoadRepository(ctx, job)
	if err != nil {
		if !errors.Is(err, ErrRepositoryNotFound) {
			return processingResult{}, err
		}
		return c.completeFailure(ctx, job, message, 0, "tracked_repo_not_found", now)
	}
	attempt, err := c.store.StartAttempt(ctx, job, c.owner, now, c.maxAttempts)
	if err != nil {
		return processingResult{}, err
	}
	if attempt == 0 {
		return c.completeFailure(ctx, job, message, 0, "max_attempts_exceeded", now)
	}
	return c.refresh(ctx, job, message, repository, attempt)
}

func (c *Consumer) refresh(ctx context.Context, job Job, message kafka.RefreshMessage, repository Repository, attempt int) (processingResult, error) {
	remote, err := c.github.RefreshRepository(ctx, repository.Owner, repository.Name)
	if err == nil {
		now := c.now().UTC()
		if err := c.store.CompleteSuccess(ctx, job, c.owner, attempt, remote, now); err != nil {
			return processingResult{}, err
		}
		return processingResult{result: "succeeded"}, nil
	}
	if ctx.Err() != nil {
		return processingResult{}, ctx.Err()
	}
	now := c.now().UTC()
	code, retryAt, retry := classify(err, now, attempt, c.maxAttempts, c.jitter)
	if retry {
		logging.Debug(ctx, "refresh retry scheduled", "attempt", attempt, "error_code", code, "retry_at", retryAt)
		return c.scheduleRetry(ctx, job, message, attempt, code, retryAt, now)
	}
	logging.Debug(ctx, "refresh failed permanently", "attempt", attempt, "error_code", code)
	return c.completeFailure(ctx, job, message, attempt, code, now)
}

func (c *Consumer) recordInvalid(ctx context.Context, eventID string, payload []byte, code string, now time.Time) (processingResult, error) {
	if err := c.store.RecordInvalid(ctx, eventID, payload, code, now); err != nil {
		return processingResult{}, err
	}
	logging.Warn(ctx, "refresh invalid recorded", "error_code", code)
	return processingResult{result: "invalid", errorCode: code}, nil
}

func (c *Consumer) completeFailure(ctx context.Context, job Job, message kafka.RefreshMessage, attempt int, code string, now time.Time) (processingResult, error) {
	if err := c.store.CompleteFailure(ctx, job, c.owner, attempt, message, code, now); err != nil {
		return processingResult{}, err
	}
	return processingResult{result: "failed", errorCode: code}, nil
}

func (c *Consumer) scheduleRetry(ctx context.Context, job Job, message kafka.RefreshMessage, attempt int, code string, retryAt, now time.Time) (processingResult, error) {
	if err := c.store.ScheduleRetry(ctx, job, c.owner, attempt, message, code, retryAt, now); err != nil {
		return processingResult{}, err
	}
	return processingResult{result: "retry_scheduled", errorCode: code}, nil
}

func (c *Consumer) log(ctx context.Context, started time.Time, result processingResult, processErr error) {
	logger := logging.LoggerFrom(ctx)
	if logger == nil {
		return
	}
	if result.result == "" {
		result.result = "replayable"
	}
	scoped := logging.ScopedLogger(ctx, logger)
	if processErr != nil {
		scoped.Error("refresh record processed", "duration", time.Since(started), "result", result.result, "error_code", result.errorCode, "err", processErr.Error())
		return
	}
	scoped.Info("refresh record processed", "duration", time.Since(started), "result", result.result, "error_code", result.errorCode)
}

func claimStateName(state ClaimState) string {
	switch state {
	case Claimed:
		return "claimed"
	case ClaimTerminal:
		return "terminal"
	case ClaimDeferred:
		return "deferred"
	case ClaimBusy:
		return "busy"
	case ClaimMissing:
		return "missing"
	}
	return "unknown"
}

func recordContext(ctx context.Context, record *sarama.ConsumerMessage) context.Context {
	ctx = logging.WithTopic(ctx, record.Topic)
	ctx = logging.WithPartition(ctx, strconv.FormatInt(int64(record.Partition), 10))
	return logging.WithOffset(ctx, strconv.FormatInt(record.Offset, 10))
}

func messageContext(ctx context.Context, message kafka.RefreshMessage, owner string) context.Context {
	ctx = logging.WithEventID(ctx, message.EventID)
	ctx = logging.WithBatchJobID(ctx, message.BatchJobID)
	ctx = logging.WithBatchID(ctx, message.BatchID)
	ctx = logging.WithRepoID(ctx, message.RepositoryID)
	ctx = logging.WithRequestID(ctx, message.CorrelationID)
	ctx = logging.WithWorkerID(ctx, owner)
	return logging.WithAttempt(ctx, strconv.Itoa(message.Attempt))
}

func classify(err error, now time.Time, attempt, maxAttempts int, jitter func(time.Duration) time.Duration) (string, time.Time, bool) {
	if errors.Is(err, client.ErrNotFound) {
		return "github_repo_not_found", time.Time{}, false
	}
	if errors.Is(err, client.ErrUnauthorized) {
		return "github_unauthorized", time.Time{}, false
	}
	if attempt >= maxAttempts {
		return "github_retry_exhausted", time.Time{}, false
	}
	if errors.Is(err, client.ErrRateLimited) {
		until := retryAt(now, attempt, jitter)
		var requestError *client.RequestError
		if errors.As(err, &requestError) {
			var required time.Time
			if requestError.RateLimit.RetryAfter > 0 {
				required = now.Add(requestError.RateLimit.RetryAfter)
			}
			if requestError.RateLimit.ResetAt != nil && requestError.RateLimit.ResetAt.After(required) {
				required = *requestError.RateLimit.ResetAt
			}
			if required.After(until) {
				until = required
			}
		}
		return "github_rate_limited", until, true
	}
	if errors.Is(err, client.ErrServer) || errors.Is(err, client.ErrNetwork) || errors.Is(err, client.ErrTimeout) || errors.Is(err, client.ErrCacheCoordinationUnavailable) {
		return "github_transient_failure", retryAt(now, attempt, jitter), true
	}
	return "github_refresh_failed", time.Time{}, false
}

func retryAt(now time.Time, attempt int, jitter func(time.Duration) time.Duration) time.Time {
	if attempt < 1 {
		attempt = 1
	}
	base := time.Second * time.Duration(1<<min(attempt-1, 5))
	return now.Add(base + boundedJitter(jitter, base/2))
}

func boundedJitter(jitter func(time.Duration) time.Duration, maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	if jitter == nil {
		jitter = randomJitter
	}
	additional := jitter(maximum)
	if additional < 0 {
		return 0
	}
	if additional > maximum {
		return maximum
	}
	return additional
}

func randomJitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(limit)+1))
	if err != nil {
		return 0
	}
	return time.Duration(value.Int64())
}
