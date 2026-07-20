package worker

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/kafka"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/logging"
)

type Store interface {
	RecordInvalid(context.Context, string, []byte, string, time.Time) error
	Claim(context.Context, kafka.RefreshMessage, string, time.Time, time.Duration) (Job, ClaimState, error)
	LoadRepository(context.Context, Job) (Repository, error)
	StartAttempt(context.Context, Job, string, time.Time, int) (int, error)
	CompleteSuccess(context.Context, Job, string, int, githubgateway.Repository, time.Time) error
	CompleteFailure(context.Context, Job, string, int, kafka.RefreshMessage, string, time.Time) error
	ScheduleRetry(context.Context, Job, string, int, kafka.RefreshMessage, string, time.Time, time.Time) error
}

type Consumer struct {
	store       Store
	github      Gateway
	owner       string
	lease       time.Duration
	maxAttempts int
	now         func() time.Time
	jitter      func(time.Duration) time.Duration
}

func NewConsumer(store Store, github Gateway, owner string, lease time.Duration, maxAttempts int) (*Consumer, error) {
	if store == nil || github == nil || owner == "" || lease <= 0 || maxAttempts < 1 {
		return nil, ErrInvalidMessage
	}
	if maxAttempts > 3 {
		maxAttempts = 3
	}
	return &Consumer{store: store, github: github, owner: owner, lease: lease, maxAttempts: maxAttempts, now: time.Now, jitter: randomJitter}, nil
}

func (c *Consumer) Process(ctx context.Context, record *sarama.ConsumerMessage) (settlement kafka.Settlement, processErr error) {
	started := time.Now()
	result, errorCode := "replayable", ""
	defer func() {
		logger := logging.LoggerFrom(ctx)
		if logger != nil {
			logging.ScopedLogger(ctx, logger).Info("refresh record processed", "duration", time.Since(started), "result", result, "error_code", errorCode)
		}
	}()
	if c == nil || record == nil {
		return kafka.Settlement{}, ErrInvalidMessage
	}
	ctx = logging.WithTopic(ctx, record.Topic)
	ctx = logging.WithPartition(ctx, strconv.FormatInt(int64(record.Partition), 10))
	ctx = logging.WithOffset(ctx, strconv.FormatInt(record.Offset, 10))
	message, err := kafka.DecodeRefresh(record.Value, 0)
	if err != nil {
		result, errorCode = "invalid", "invalid_refresh_message"
		if err := c.store.RecordInvalid(ctx, invalidRecordID(record.Value), record.Value, "invalid_refresh_message", c.now().UTC()); err != nil {
			return kafka.Settlement{}, err
		}
		return kafka.Settlement{Commit: true}, nil
	}
	ctx = logging.WithEventID(ctx, message.EventID)
	ctx = logging.WithBatchJobID(ctx, message.BatchJobID)
	ctx = logging.WithBatchID(ctx, message.BatchID)
	ctx = logging.WithRepoID(ctx, message.RepositoryID)
	ctx = logging.WithRequestID(ctx, message.CorrelationID)
	ctx = logging.WithWorkerID(ctx, c.owner)
	ctx = logging.WithAttempt(ctx, strconv.Itoa(message.Attempt))
	now := c.now().UTC()
	job, state, err := c.store.Claim(ctx, message, c.owner, now, c.lease)
	if err != nil {
		return kafka.Settlement{}, err
	}
	if state == ClaimMissing {
		result, errorCode = "invalid", "refresh_job_not_found"
		if err := c.store.RecordInvalid(ctx, message.EventID, record.Value, "refresh_job_not_found", now); err != nil {
			return kafka.Settlement{}, err
		}
		return kafka.Settlement{Commit: true}, nil
	}
	if state == ClaimTerminal || state == ClaimDeferred || state == ClaimBusy {
		result = "duplicate_or_deferred"
		return kafka.Settlement{Commit: true}, nil
	}
	if state != Claimed {
		return kafka.Settlement{}, ErrClaimLost
	}
	if err := ctx.Err(); err != nil {
		return kafka.Settlement{}, err
	}
	cooldown, err := c.github.Cooldown(ctx)
	if err != nil {
		return kafka.Settlement{}, err
	}
	if cooldown.After(now) {
		if err := c.store.ScheduleRetry(ctx, job, c.owner, 0, message, "github_rate_limited", cooldown.Add(boundedJitter(c.jitter, time.Second)), now); err != nil {
			return kafka.Settlement{}, err
		}
		result, errorCode = "retry_scheduled", "github_rate_limited"
		return kafka.Settlement{Commit: true}, nil
	}
	repository, err := c.store.LoadRepository(ctx, job)
	if err != nil {
		if err := c.store.CompleteFailure(ctx, job, c.owner, 0, message, "tracked_repo_not_found", now); err != nil {
			return kafka.Settlement{}, err
		}
		result, errorCode = "failed", "tracked_repo_not_found"
		return kafka.Settlement{Commit: true}, nil
	}
	attempt, err := c.store.StartAttempt(ctx, job, c.owner, now, c.maxAttempts)
	if err != nil {
		return kafka.Settlement{}, err
	}
	if attempt == 0 {
		if err := c.store.CompleteFailure(ctx, job, c.owner, 0, message, "max_attempts_exceeded", now); err != nil {
			return kafka.Settlement{}, err
		}
		result, errorCode = "failed", "max_attempts_exceeded"
		return kafka.Settlement{Commit: true}, nil
	}
	remote, err := c.github.RefreshRepository(ctx, repository.Owner, repository.Name)
	if err == nil {
		if err := c.store.CompleteSuccess(ctx, job, c.owner, attempt, remote, c.now().UTC()); err != nil {
			return kafka.Settlement{}, err
		}
		result = "succeeded"
		return kafka.Settlement{Commit: true}, nil
	}
	if ctx.Err() != nil {
		return kafka.Settlement{}, ctx.Err()
	}
	code, retryAt, retry := classify(err, c.now().UTC(), attempt, c.maxAttempts, c.jitter)
	if retry {
		if err := c.store.ScheduleRetry(ctx, job, c.owner, attempt, message, code, retryAt, c.now().UTC()); err != nil {
			return kafka.Settlement{}, err
		}
		result, errorCode = "retry_scheduled", code
		return kafka.Settlement{Commit: true}, nil
	}
	if err := c.store.CompleteFailure(ctx, job, c.owner, attempt, message, code, c.now().UTC()); err != nil {
		return kafka.Settlement{}, err
	}
	result, errorCode = "failed", code
	return kafka.Settlement{Commit: true}, nil
}

func classify(err error, now time.Time, attempt, maxAttempts int, jitter func(time.Duration) time.Duration) (string, time.Time, bool) {
	if errors.Is(err, githubgateway.ErrNotFound) {
		return "github_repo_not_found", time.Time{}, false
	}
	if errors.Is(err, githubgateway.ErrUnauthorized) {
		return "github_unauthorized", time.Time{}, false
	}
	if attempt >= maxAttempts {
		return "github_retry_exhausted", time.Time{}, false
	}
	if errors.Is(err, githubgateway.ErrRateLimited) {
		until := retryAt(now, attempt, jitter)
		var requestError *githubgateway.RequestError
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
	if errors.Is(err, githubgateway.ErrServer) || errors.Is(err, githubgateway.ErrNetwork) || errors.Is(err, githubgateway.ErrTimeout) || errors.Is(err, githubgateway.ErrCacheCoordinationUnavailable) {
		return "github_transient_failure", retryAt(now, attempt, jitter), true
	}
	return "github_refresh_failed", time.Time{}, false
}

func (c *Consumer) WorkerID() string { return c.owner }

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
