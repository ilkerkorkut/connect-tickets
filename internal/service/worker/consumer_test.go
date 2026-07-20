package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/client/github"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
)

type fakeStore struct {
	job         Job
	state       ClaimState
	start       int
	starts      int
	successes   int
	failures    int
	retries     int
	invalids    int
	successErr  error
	loadErr     error
	retryAt     time.Time
	retryCode   string
	failureCode string
}

func (s *fakeStore) RecordInvalid(context.Context, string, []byte, string, time.Time) error {
	s.invalids++
	return nil
}

func (s *fakeStore) Claim(context.Context, kafka.RefreshMessage, string, time.Time, time.Duration) (Job, ClaimState, error) {
	return s.job, s.state, nil
}
func (s *fakeStore) LoadRepository(context.Context, Job) (Repository, error) {
	if s.loadErr != nil {
		return Repository{}, s.loadErr
	}
	return Repository{ID: s.job.RepositoryID, Owner: "owner", Name: "repo"}, nil
}
func (s *fakeStore) StartAttempt(context.Context, Job, string, time.Time, int) (int, error) {
	s.starts++
	return s.start, nil
}
func (s *fakeStore) CompleteSuccess(context.Context, Job, string, int, githubgateway.Repository, time.Time) error {
	s.successes++
	return s.successErr
}
func (s *fakeStore) CompleteFailure(_ context.Context, _ Job, _ string, _ int, _ kafka.RefreshMessage, code string, _ time.Time) error {
	s.failures++
	s.failureCode = code
	return nil
}
func (s *fakeStore) ScheduleRetry(_ context.Context, _ Job, _ string, _ int, _ kafka.RefreshMessage, code string, retryAt, _ time.Time) error {
	s.retries++
	s.retryCode = code
	s.retryAt = retryAt
	return nil
}

type fakeGateway struct {
	cooldown time.Time
	err      error
	calls    int
}

func (g *fakeGateway) Cooldown(context.Context) (time.Time, error) { return g.cooldown, nil }
func (g *fakeGateway) FetchRepository(context.Context, string, string) (githubgateway.Repository, error) {
	return githubgateway.Repository{}, nil
}
func (g *fakeGateway) RefreshRepository(context.Context, string, string) (githubgateway.Repository, error) {
	g.calls++
	if g.err != nil {
		return githubgateway.Repository{}, g.err
	}
	return githubgateway.Repository{Owner: "owner", Name: "repo", FullName: "owner/repo", HTMLURL: "https://github.com/owner/repo", FetchedAt: time.Now()}, nil
}

func workerRecord(t *testing.T) *sarama.ConsumerMessage {
	t.Helper()
	jobID, batchID, repoID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	payload, err := kafka.EncodeRefresh(kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: uuid.NewString(), BatchJobID: jobID, BatchID: batchID, RepositoryID: repoID, CreatedAt: time.Now(), CorrelationID: "req"}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return &sarama.ConsumerMessage{Value: payload}
}

func TestDuplicateTerminalDeliveryDoesNotCallGitHub(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: ClaimTerminal}
	gateway := &fakeGateway{}
	consumer, err := NewConsumer(store, gateway, "worker", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := consumer.Process(context.Background(), record)
	if err != nil || !settlement.Commit || gateway.calls != 0 {
		t.Fatalf("settlement=%#v err=%v calls=%d", settlement, err, gateway.calls)
	}
}

func TestSuccessIsDurableBeforeSettlement(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, start: 1}
	gateway := &fakeGateway{}
	consumer, _ := NewConsumer(store, gateway, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), record)
	if err != nil || !settlement.Commit || store.successes != 1 || gateway.calls != 1 {
		t.Fatalf("settlement=%#v err=%v success=%d calls=%d", settlement, err, store.successes, gateway.calls)
	}
}

func TestTransientFailureSchedulesRetry(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, start: 1}
	gateway := &fakeGateway{err: githubgateway.ErrServer}
	consumer, _ := NewConsumer(store, gateway, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), record)
	if err != nil || !settlement.Commit || store.retries != 1 {
		t.Fatalf("settlement=%#v err=%v retries=%d", settlement, err, store.retries)
	}
}

func TestInvalidPayloadIsRecordedBeforeSettlement(t *testing.T) {
	store := &fakeStore{}
	consumer, _ := NewConsumer(store, &fakeGateway{}, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), &sarama.ConsumerMessage{Value: []byte("not-json")})
	if err != nil || !settlement.Commit || store.invalids != 1 {
		t.Fatalf("settlement=%#v err=%v invalids=%d", settlement, err, store.invalids)
	}
}

func TestCooldownDefersWithoutConsumingAttempt(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, start: 1}
	gateway := &fakeGateway{cooldown: time.Now().Add(time.Minute)}
	consumer, _ := NewConsumer(store, gateway, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), record)
	if err != nil || !settlement.Commit || store.retries != 1 || store.starts != 0 || gateway.calls != 0 {
		t.Fatalf("settlement=%#v err=%v retries=%d starts=%d calls=%d", settlement, err, store.retries, store.starts, gateway.calls)
	}
}

func TestMissingRepositoryCompletesAsPermanentFailure(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, loadErr: ErrRepositoryNotFound}
	consumer, _ := NewConsumer(store, &fakeGateway{}, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), record)
	if err != nil || !settlement.Commit || store.failures != 1 || store.failureCode != "tracked_repo_not_found" {
		t.Fatalf("settlement=%#v err=%v failures=%d code=%q", settlement, err, store.failures, store.failureCode)
	}
}

func TestRepositoryLoadFailureLeavesRecordReplayable(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, loadErr: errors.New("database unavailable")}
	consumer, _ := NewConsumer(store, &fakeGateway{}, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), record)
	if err == nil || settlement.Commit || store.failures != 0 {
		t.Fatalf("settlement=%#v err=%v failures=%d", settlement, err, store.failures)
	}
}

func TestCancellationLeavesRecordReplayable(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, start: 1}
	consumer, _ := NewConsumer(store, &fakeGateway{}, "worker", time.Minute, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	settlement, err := consumer.Process(ctx, record)
	if err == nil || settlement.Commit || store.starts != 0 {
		t.Fatalf("settlement=%#v err=%v starts=%d", settlement, err, store.starts)
	}
}

func TestOutcomeFailureLeavesRecordReplayable(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, start: 1, successErr: context.DeadlineExceeded}
	consumer, _ := NewConsumer(store, &fakeGateway{}, "worker", time.Minute, 3)
	settlement, err := consumer.Process(context.Background(), record)
	if err == nil || settlement.Commit || store.successes != 1 {
		t.Fatalf("settlement=%#v err=%v successes=%d", settlement, err, store.successes)
	}
}

func TestTransientRetryUsesBoundedExponentialJitter(t *testing.T) {
	record := workerRecord(t)
	message, _ := kafka.DecodeRefresh(record.Value, 4096)
	jobID, _ := uuid.Parse(message.BatchJobID)
	batchID, _ := uuid.Parse(message.BatchID)
	repoID, _ := uuid.Parse(message.RepositoryID)
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{job: Job{ID: jobID, BatchID: batchID, RepositoryID: repoID}, state: Claimed, start: 1}
	consumer, _ := NewConsumer(store, &fakeGateway{err: githubgateway.ErrServer}, "worker", time.Minute, 3)
	consumer.now = func() time.Time { return now }
	consumer.jitter = func(limit time.Duration) time.Duration { return limit }
	settlement, err := consumer.Process(context.Background(), record)
	want := now.Add(1500 * time.Millisecond)
	if err != nil || !settlement.Commit || store.retryCode != "github_transient_failure" || !store.retryAt.Equal(want) {
		t.Fatalf("settlement=%#v err=%v code=%q retryAt=%s want=%s", settlement, err, store.retryCode, store.retryAt, want)
	}
}

func TestRateLimitRetryNeverPrecedesUpstreamRequirement(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	resetAt := now.Add(8 * time.Second)
	err := &githubgateway.RequestError{Kind: githubgateway.ErrRateLimited, RateLimit: githubgateway.RateLimit{RetryAfter: 5 * time.Second, ResetAt: &resetAt}}
	code, retryAt, retry := classify(err, now, 1, 3, func(time.Duration) time.Duration { return 0 })
	if !retry || code != "github_rate_limited" || retryAt.Before(resetAt) {
		t.Fatalf("code=%q retry=%t retryAt=%s reset=%s", code, retry, retryAt, resetAt)
	}
}

func TestRateLimitRetryAlsoHonorsExponentialBackoff(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	err := &githubgateway.RequestError{Kind: githubgateway.ErrRateLimited, RateLimit: githubgateway.RateLimit{RetryAfter: 500 * time.Millisecond}}
	_, retryAt, retry := classify(err, now, 1, 3, func(time.Duration) time.Duration { return 0 })
	want := now.Add(time.Second)
	if !retry || !retryAt.Equal(want) {
		t.Fatalf("retry=%t retryAt=%s want=%s", retry, retryAt, want)
	}
}

func TestPermanentFailureAndAttemptExhaustionAreTerminal(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		err  error
		try  int
		code string
	}{
		{err: githubgateway.ErrNotFound, try: 1, code: "github_repo_not_found"},
		{err: githubgateway.ErrUnauthorized, try: 1, code: "github_unauthorized"},
		{err: githubgateway.ErrServer, try: 3, code: "github_retry_exhausted"},
	} {
		code, _, retry := classify(test.err, now, test.try, 3, func(time.Duration) time.Duration { return 0 })
		if retry || code != test.code {
			t.Fatalf("error=%v code=%q retry=%t", test.err, code, retry)
		}
	}
}
