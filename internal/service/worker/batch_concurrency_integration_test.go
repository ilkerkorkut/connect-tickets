//go:build testcontainers

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/batchjob"
	"github.com/ilkerkorkut/connect-tickets/ent/outboxevent"
	"github.com/ilkerkorkut/connect-tickets/ent/repositorychange"
	"github.com/ilkerkorkut/connect-tickets/internal/cache"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	githubclient "github.com/ilkerkorkut/connect-tickets/internal/client/github"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
	"github.com/ilkerkorkut/connect-tickets/internal/testkit"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

const (
	concurrentBatchWorkers = 5
	batchWorkerLease       = time.Minute
	batchWorkerMaxAttempts = 3
)

type batchScenarioLogger struct {
	t    *testing.T
	mu   sync.Mutex
	step int
}

func newBatchScenarioLogger(t *testing.T) *batchScenarioLogger {
	t.Helper()
	return &batchScenarioLogger{t: t}
}

func (logger *batchScenarioLogger) Log(format string, args ...any) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.step++
	logger.t.Logf("step=%02d "+format, append([]any{logger.step}, args...)...)
}

func (logger *batchScenarioLogger) Logf(format string, args ...any) {
	logger.Log(format, args...)
}

type batchAPIStub struct {
	github client.GitHub
}

func (stub batchAPIStub) GitHub() client.GitHub { return stub.github }

type batchGitHubRoute struct {
	owner string
	name  string
}

type batchGitHubMock struct {
	server          *httptest.Server
	calls           atomic.Int32
	byPath          sync.Map
	notFoundCount   atomic.Int32
	rateLimitCount  atomic.Int32
	retryAtUnixNano atomic.Int64
	retryAfterSec   int
}

func newBatchGitHubMock(t *testing.T, log *batchScenarioLogger) *batchGitHubMock {
	t.Helper()
	mock := &batchGitHubMock{retryAfterSec: 20}
	mock.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call := mock.calls.Add(1)
		route, ok := mock.lookup(request.URL.Path)
		log.Logf("phase=github_mock call=%d method=%s path=%s route=%t", call, request.Method, request.URL.Path, ok)
		if !ok {
			mock.notFoundCount.Add(1)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		switch route.outcome {
		case "not_found":
			mock.notFoundCount.Add(1)
			writer.WriteHeader(http.StatusNotFound)
			return
		case "rate_limited":
			mock.rateLimitCount.Add(1)
			retryAt := time.Now().UTC().Add(time.Duration(mock.retryAfterSec) * time.Second).Truncate(time.Second)
			mock.retryAtUnixNano.Store(retryAt.UnixNano())
			writer.Header().Set("Retry-After", fmt.Sprintf("%d", mock.retryAfterSec))
			writer.Header().Set("X-RateLimit-Remaining", "0")
			writer.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", retryAt.Unix()))
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		default:
			writer.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(writer, `{"name":%q,"full_name":%q,"description":null,"stargazers_count":%d,"language":%q,"html_url":%q,"owner":{"login":%q}}`, route.name, route.owner+"/"+route.name, route.stars, route.language, "https://github.example/"+route.owner+"/"+route.name, route.owner)
		}
	}))
	t.Cleanup(mock.server.Close)
	return mock
}

type batchRouteOutcome struct {
	outcome  string
	owner    string
	name     string
	stars    int
	language string
}

func (mock *batchGitHubMock) register(owner, name, outcome string, stars int, language string) {
	mock.byPath.Store("/repos/"+owner+"/"+name, batchRouteOutcome{outcome: outcome, owner: owner, name: name, stars: stars, language: language})
}

func (mock *batchGitHubMock) lookup(path string) (batchRouteOutcome, bool) {
	value, ok := mock.byPath.Load(path)
	if !ok {
		return batchRouteOutcome{}, false
	}
	return value.(batchRouteOutcome), true
}

func (mock *batchGitHubMock) retryUntil() time.Time {
	return time.Unix(0, mock.retryAtUnixNano.Load()).UTC()
}

func newBatchEntClient(t *testing.T, databaseURL string) *ent.Client {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, stdlib.OpenDBFromPool(pool))))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newBatchRedisClient(t *testing.T, address string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func newBatchCacheGateway(t *testing.T, log *batchScenarioLogger, redisClient *redis.Client, baseURL string) *cache.CacheGateway {
	t.Helper()
	upstream, err := githubclient.NewClient(config.GitHubConfig{BaseURL: baseURL, Timeout: 2 * time.Second, UserAgent: "batch-concurrency", MaxIdleConns: concurrentBatchWorkers, MaxIdleConnsPerHost: concurrentBatchWorkers, IdleConnTimeout: time.Minute}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := cache.NewCacheGateway(batchAPIStub{github: upstream}, redisClient, config.CacheConfig{TTL: time.Minute, LockTTL: 3 * time.Second, LockWait: 4 * time.Second}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	log.Log("phase=setup action=cache_gateway_ready ttl=1m lock_ttl=3s lock_wait=4s github_timeout=2s")
	return gateway
}

func newBatchKafka(t *testing.T, log *batchScenarioLogger, brokers []string, partitions int) (sarama.SyncProducer, map[int32]sarama.PartitionConsumer) {
	t.Helper()
	configuration := sarama.NewConfig()
	configuration.Version = sarama.V2_1_0_0
	configuration.Producer.RequiredAcks = sarama.WaitForAll
	configuration.Producer.Return.Successes = true
	configuration.Producer.Return.Errors = true
	configuration.Producer.Partitioner = sarama.NewManualPartitioner
	configuration.Consumer.Return.Errors = true
	configuration.Consumer.Offsets.Initial = sarama.OffsetOldest

	admin, err := sarama.NewClusterAdmin(brokers, configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := kafka.EnsureTopics(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	producer, err := sarama.NewSyncProducer(brokers, configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = producer.Close() })
	consumer, err := sarama.NewConsumer(brokers, configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	consumers := make(map[int32]sarama.PartitionConsumer, partitions)
	for partition := int32(0); partition < int32(partitions); partition++ {
		partitionConsumer, err := consumer.ConsumePartition(kafka.RefreshTopic, partition, sarama.OffsetOldest)
		if err != nil {
			t.Fatal(err)
		}
		consumers[partition] = partitionConsumer
		t.Cleanup(func() { _ = partitionConsumer.Close() })
	}
	log.Logf("phase=setup action=kafka_ready topic=%s partitions=%d", kafka.RefreshTopic, partitions)
	return producer, consumers
}

type batchJobFixture struct {
	jobID        uuid.UUID
	batchID      uuid.UUID
	repositoryID uuid.UUID
	owner        string
	name         string
	payload      []byte
	partition    int32
}

func seedBatchJobs(t *testing.T, log *batchScenarioLogger, client *ent.Client, count int, naming func(int) (owner, name string)) ([]batchJobFixture, *batchGitHubMock) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	batchID := uuid.New()
	if _, err := client.Batch.Create().SetID(batchID).SetCreatedAt(now).Save(ctx); err != nil {
		t.Fatal(err)
	}
	fixtures := make([]batchJobFixture, 0, count)
	for index := range count {
		owner, name := naming(index)
		fullName := owner + "/" + name
		repository, err := client.Repository.Create().SetID(uuid.New()).SetOwner(owner).SetName(name).SetFullName(fullName).SetNormalizedFullName(fullName).SetStars(0).SetHTMLURL("https://github.example/" + fullName).SetFetchedAt(now).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		job, err := client.BatchJob.Create().SetID(uuid.New()).SetBatchID(batchID).SetRepoID(repository.ID).SetStatus(batchjob.StatusPending).SetAttemptCount(0).SetCreatedAt(now).SetUpdatedAt(now).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		message := kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: uuid.NewString(), BatchJobID: job.ID.String(), BatchID: batchID.String(), RepositoryID: repository.ID.String(), CreatedAt: now, CorrelationID: "batch-concurrency"}
		payload, err := kafka.EncodeRefresh(message, 4096)
		if err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, batchJobFixture{jobID: job.ID, batchID: batchID, repositoryID: repository.ID, owner: owner, name: name, payload: payload, partition: int32(index)})
	}
	mock := newBatchGitHubMock(t, log)
	for _, fixture := range fixtures {
		mock.register(fixture.owner, fixture.name, "ok", 7+int(fixture.partition), "Go")
	}
	log.Logf("phase=seed action=batch_ready batch_id=%s jobs=%d repos=%d", batchID, count, count)
	return fixtures, mock
}

func publishBatchJob(t *testing.T, log *batchScenarioLogger, producer sarama.SyncProducer, fixture batchJobFixture) {
	t.Helper()
	_, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: kafka.RefreshTopic, Partition: fixture.partition, Key: sarama.StringEncoder(fixture.repositoryID.String()), Value: sarama.ByteEncoder(fixture.payload)})
	if err != nil {
		t.Fatal(err)
	}
	log.Logf("phase=kafka action=publish partition=%d repo_id=%s", fixture.partition, fixture.repositoryID)
}

func consumeBatchRecord(t *testing.T, log *batchScenarioLogger, partitionConsumer sarama.PartitionConsumer, partition int32) *sarama.ConsumerMessage {
	t.Helper()
	select {
	case message := <-partitionConsumer.Messages():
		if message == nil {
			t.Fatal("kafka partition closed before receiving refresh message")
		}
		log.Logf("phase=kafka action=consume partition=%d offset=%d", partition, message.Offset)
		return message
	case consumerError := <-partitionConsumer.Errors():
		if consumerError == nil {
			t.Fatal("kafka partition error channel closed")
		}
		t.Fatal(consumerError)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Kafka refresh message")
	}
	return nil
}

type batchWorkerResult struct {
	partition  int32
	settlement kafka.Settlement
	err        error
}

type workerSlogHandler struct {
	log *batchScenarioLogger
}

func (h *workerSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelDebug
}

func (h *workerSlogHandler) Handle(_ context.Context, record slog.Record) error {
	h.log.mu.Lock()
	step := h.log.step + 1
	h.log.step = step
	h.log.mu.Unlock()
	msg := record.Message
	fields := ""
	record.Attrs(func(a slog.Attr) bool {
		fields += fmt.Sprintf(" %s=%v", a.Key, a.Value.Any())
		return true
	})
	h.log.t.Logf("step=%02d phase=worker %s%s", step, msg, fields)
	return nil
}

func (h *workerSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *workerSlogHandler) WithGroup(string) slog.Handler { return h }

func newWorkerLogger(log *batchScenarioLogger) *slog.Logger {
	return slog.New(&workerSlogHandler{log: log})
}

func processBatchWorkers(t *testing.T, log *batchScenarioLogger, consumer *Consumer, records map[int32]*sarama.ConsumerMessage, workerLogger *slog.Logger) []batchWorkerResult {
	t.Helper()
	start := make(chan struct{})
	results := make(chan batchWorkerResult, len(records))
	var workers sync.WaitGroup
	log.Logf("phase=execution action=launch_workers count=%d synchronization=shared_start_gate", len(records))
	for partition, record := range records {
		workers.Add(1)
		go func(partition int32, record *sarama.ConsumerMessage) {
			defer workers.Done()
			<-start
			ctx := logging.WithLogger(context.Background(), workerLogger)
			log.Logf("phase=execution worker=%d action=process partition=%d offset=%d", partition, partition, record.Offset)
			settlement, err := consumer.Process(ctx, record)
			results <- batchWorkerResult{partition: partition, settlement: settlement, err: err}
		}(partition, record)
	}
	log.Log("phase=execution action=release_shared_start_gate")
	close(start)
	workers.Wait()
	close(results)
	out := make([]batchWorkerResult, 0, len(results))
	for result := range results {
		out = append(out, result)
	}
	return out
}

func TestBatchConcurrentRefreshSucceedsWithExactlyOnceGitHub(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		t.Skip("testcontainers-only")
	}
	log := newBatchScenarioLogger(t)
	log.Logf("scenario=concurrent_batch_success workers=%d jobs=%d partitions=%d expected_github_requests=%d expected_change_rows=%d", concurrentBatchWorkers, concurrentBatchWorkers, concurrentBatchWorkers, concurrentBatchWorkers, concurrentBatchWorkers)
	log.Log("scenario_detail=five_refresh_jobs_across_five_partitions; batch_jobs_for_update_locks_each_step; change_sequences_singleton_serializes_change_feed; cache_gateway_coalesces_one_github_call_per_repo; batch_auto_completes_on_last_success")

	environment := testkit.Start(t)
	log.Log("phase=setup action=dependencies_ready components=postgres,redis,kafka")
	client := newBatchEntClient(t, environment.DatabaseURL)
	redisClient := newBatchRedisClient(t, environment.RedisAddress)
	producer, partitions := newBatchKafka(t, log, environment.KafkaBrokers, concurrentBatchWorkers)

	owner := "batch-success-" + uuid.NewString()
	fixtures, mock := seedBatchJobs(t, log, client, concurrentBatchWorkers, func(index int) (string, string) {
		return owner, fmt.Sprintf("repo-%d", index+1)
	})
	gateway := newBatchCacheGateway(t, log, redisClient, mock.server.URL)
	consumer, err := NewConsumer(NewStore(client), gateway, "batch-success-worker", batchWorkerLease, batchWorkerMaxAttempts)
	if err != nil {
		t.Fatal(err)
	}

	records := make(map[int32]*sarama.ConsumerMessage, len(fixtures))
	for _, fixture := range fixtures {
		publishBatchJob(t, log, producer, fixture)
		records[fixture.partition] = consumeBatchRecord(t, log, partitions[fixture.partition], fixture.partition)
	}

	results := processBatchWorkers(t, log, consumer, records, newWorkerLogger(log))
	for _, result := range results {
		if result.err != nil || !result.settlement.Commit {
			t.Fatalf("worker partition=%d settlement=%#v err=%v", result.partition, result.settlement, result.err)
		}
		log.Logf("phase=execution worker=%d action=settlement commit=true", result.partition)
	}

	ctx := context.Background()
	if calls := mock.calls.Load(); calls != int32(concurrentBatchWorkers) {
		t.Fatalf("github requests=%d want=%d", calls, concurrentBatchWorkers)
	}
	log.Logf("phase=assertion github_requests=%d exactly_once=true", mock.calls.Load())

	for _, fixture := range fixtures {
		job, err := client.BatchJob.Get(ctx, fixture.jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status != batchjob.StatusSucceeded || job.AttemptCount != 1 || job.LastErrorCode != "" || job.CompletedAt == nil {
			t.Fatalf("job=%s status=%s attempts=%d error_code=%q completed_at=%v", fixture.jobID, job.Status, job.AttemptCount, job.LastErrorCode, job.CompletedAt)
		}
	}
	log.Logf("phase=assertion jobs_succeeded=%d attempt_count=1 error_codes=none", len(fixtures))

	batch, err := client.Batch.Get(ctx, fixtures[0].batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.CompletedAt == nil {
		t.Fatalf("batch=%s completed_at=nil want=set", fixtures[0].batchID)
	}
	log.Logf("phase=assertion batch_completed=true completed_at=%s", batch.CompletedAt.Format(time.RFC3339Nano))

	changes, err := client.RepositoryChange.Query().Order(repositorychange.BySequence()).All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != concurrentBatchWorkers {
		t.Fatalf("repository_changes=%d want=%d", len(changes), concurrentBatchWorkers)
	}
	seenSequences := make(map[int64]struct{}, len(changes))
	seenEventIDs := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		if change.Operation != repositorychange.OperationRefreshed {
			t.Fatalf("change=%s operation=%s want=refreshed", change.ID, change.Operation)
		}
		if _, dup := seenSequences[change.Sequence]; dup {
			t.Fatalf("duplicate change sequence=%d", change.Sequence)
		}
		seenSequences[change.Sequence] = struct{}{}
		if _, dup := seenEventIDs[change.EventID]; dup {
			t.Fatalf("duplicate change event_id=%s", change.EventID)
		}
		seenEventIDs[change.EventID] = struct{}{}
	}
	for expected := int64(1); expected <= int64(concurrentBatchWorkers); expected++ {
		if _, ok := seenSequences[expected]; !ok {
			t.Fatalf("missing change sequence=%d among=%d", expected, len(seenSequences))
		}
	}
	log.Logf("phase=assertion change_feed_rows=%d sequences=1..%d monotonic=true no_gaps=true no_dupes=true", len(changes), concurrentBatchWorkers)

	perRepoChanges := make(map[uuid.UUID]int, len(fixtures))
	for _, change := range changes {
		if change.RepoID == nil {
			t.Fatalf("change=%s repo_id=nil", change.ID)
		}
		perRepoChanges[*change.RepoID]++
	}
	for _, fixture := range fixtures {
		if perRepoChanges[fixture.repositoryID] != 1 {
			t.Fatalf("repo=%s change_rows=%d want=1", fixture.repositoryID, perRepoChanges[fixture.repositoryID])
		}
	}
	log.Logf("phase=assertion per_repo_change_rows=1 repos=%d", len(fixtures))

	for _, fixture := range fixtures {
		repo, err := client.Repository.Get(ctx, fixture.repositoryID)
		if err != nil {
			t.Fatal(err)
		}
		if repo.Owner != fixture.owner || repo.Name != fixture.name || repo.FullName != fixture.owner+"/"+fixture.name {
			t.Fatalf("repo=%s stored owner/name/full_name mismatch: %#v", fixture.repositoryID, repo)
		}
		if repo.Stars != 7+int(fixture.partition) {
			t.Fatalf("repo=%s stars=%d want=%d", fixture.repositoryID, repo.Stars, 7+int(fixture.partition))
		}
	}
	log.Logf("phase=summary github_requests=%d succeeded=%d change_feed_sequences=1..%d batch_completed=true", mock.calls.Load(), len(fixtures), concurrentBatchWorkers)
}

func TestBatchRetryAndDLQWithExactlyOnceSemantics(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") != "" {
		t.Skip("testcontainers-only")
	}
	log := newBatchScenarioLogger(t)
	log.Log("scenario=batch_retry_and_dlq jobs=3 partitions=3 outcomes=success,not_found,rate_limited")
	log.Log("scenario_detail=jobs_processed_sequentially_success_then_not_found_then_rate_limited; shared_rate_limit_cooldown_is_global_so_rate_limited_runs_last_to_avoid_blocking_success; job_a_advances_change_feed; job_b_permanent_failure_emits_dlq; job_c_schedules_retry_via_outbox")

	environment := testkit.Start(t)
	log.Log("phase=setup action=dependencies_ready components=postgres,redis,kafka")
	client := newBatchEntClient(t, environment.DatabaseURL)
	redisClient := newBatchRedisClient(t, environment.RedisAddress)
	producer, partitions := newBatchKafka(t, log, environment.KafkaBrokers, 3)

	owner := "batch-mixed-" + uuid.NewString()
	fixtures, mock := seedBatchJobs(t, log, client, 3, func(index int) (string, string) {
		return owner, []string{"success", "rate-limited", "missing"}[index]
	})
	mock.register(fixtures[0].owner, fixtures[0].name, "ok", 11, "Go")
	mock.register(fixtures[1].owner, fixtures[1].name, "rate_limited", 0, "")
	mock.register(fixtures[2].owner, fixtures[2].name, "not_found", 0, "")

	gateway := newBatchCacheGateway(t, log, redisClient, mock.server.URL)
	consumer, err := NewConsumer(NewStore(client), gateway, "batch-mixed-worker", batchWorkerLease, batchWorkerMaxAttempts)
	if err != nil {
		t.Fatal(err)
	}

	records := make(map[int32]*sarama.ConsumerMessage, len(fixtures))
	for _, fixture := range fixtures {
		publishBatchJob(t, log, producer, fixture)
		records[fixture.partition] = consumeBatchRecord(t, log, partitions[fixture.partition], fixture.partition)
	}

	workerLogger := newWorkerLogger(log)
	processingOrder := []int{0, 2, 1}
	for _, fixtureIndex := range processingOrder {
		fixture := fixtures[fixtureIndex]
		record := records[fixture.partition]
		log.Logf("phase=execution worker=%d action=process_sequential partition=%d outcome=%s", fixture.partition, fixture.partition, []string{"success", "rate_limited", "not_found"}[fixtureIndex])
		ctx := logging.WithLogger(context.Background(), workerLogger)
		settlement, processErr := consumer.Process(ctx, record)
		if processErr != nil || !settlement.Commit {
			t.Fatalf("worker partition=%d settlement=%#v err=%v", fixture.partition, settlement, processErr)
		}
		log.Logf("phase=execution worker=%d action=settlement commit=true", fixture.partition)
	}

	ctx := context.Background()

	jobA, err := client.BatchJob.Get(ctx, fixtures[0].jobID)
	if err != nil {
		t.Fatal(err)
	}
	if jobA.Status != batchjob.StatusSucceeded || jobA.AttemptCount != 1 || jobA.LastErrorCode != "" {
		t.Fatalf("jobA=%s status=%s attempts=%d error_code=%q want=succeeded/1/empty", fixtures[0].jobID, jobA.Status, jobA.AttemptCount, jobA.LastErrorCode)
	}
	log.Logf("phase=assertion job=success status=succeeded attempts=1 change_feed_advanced=true")

	jobB, err := client.BatchJob.Get(ctx, fixtures[1].jobID)
	if err != nil {
		t.Fatal(err)
	}
	if jobB.Status != batchjob.StatusRetryWait || jobB.AttemptCount != 1 || jobB.LastErrorCode != "github_rate_limited" || jobB.NextAttemptAt == nil {
		t.Fatalf("jobB=%s status=%s attempts=%d error_code=%q next_attempt_at=%v want=retry_wait/1/github_rate_limited", fixtures[1].jobID, jobB.Status, jobB.AttemptCount, jobB.LastErrorCode, jobB.NextAttemptAt)
	}
	if jobB.NextAttemptAt.Before(mock.retryUntil()) {
		t.Fatalf("jobB next_attempt_at=%s before cooldown_until=%s", jobB.NextAttemptAt, mock.retryUntil())
	}
	retryCount, err := client.OutboxEvent.Query().Where(outboxevent.AggregateIDEQ(fixtures[1].jobID), outboxevent.EventTypeEQ(kafka.RefreshTopic)).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 {
		t.Fatalf("jobB retry outbox events=%d want=1", retryCount)
	}
	retryPending, err := client.OutboxEvent.Query().Where(outboxevent.AggregateIDEQ(fixtures[1].jobID), outboxevent.EventTypeEQ(kafka.RefreshTopic), outboxevent.StatusEQ(outboxevent.StatusPending)).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retryPending != 1 {
		t.Fatalf("jobB pending retry outbox events=%d want=1", retryPending)
	}
	log.Logf("phase=assertion job=rate_limited status=retry_wait attempts=1 retry_outbox_events=1 cooldown_honored=true")

	jobC, err := client.BatchJob.Get(ctx, fixtures[2].jobID)
	if err != nil {
		t.Fatal(err)
	}
	if jobC.Status != batchjob.StatusFailed || jobC.AttemptCount != 1 || jobC.LastErrorCode != "github_repo_not_found" {
		t.Fatalf("jobC=%s status=%s attempts=%d error_code=%q want=failed/1/github_repo_not_found", fixtures[2].jobID, jobC.Status, jobC.AttemptCount, jobC.LastErrorCode)
	}
	dlqCount, err := client.OutboxEvent.Query().Where(outboxevent.AggregateIDEQ(fixtures[2].jobID), outboxevent.EventTypeEQ(kafka.DLQTopic)).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dlqCount != 1 {
		t.Fatalf("jobC dlq outbox events=%d want=1", dlqCount)
	}
	log.Logf("phase=assertion job=not_found status=failed attempts=1 dlq_outbox_events=1")

	if calls := mock.calls.Load(); calls != 3 {
		t.Fatalf("github requests=%d want=3 (exactly one per repo)", calls)
	}
	if mock.notFoundCount.Load() != 1 {
		t.Fatalf("github 404 count=%d want=1", mock.notFoundCount.Load())
	}
	if mock.rateLimitCount.Load() != 1 {
		t.Fatalf("github 429 count=%d want=1", mock.rateLimitCount.Load())
	}
	log.Logf("phase=assertion github_requests=%d 200=1 404=1 429=1 exactly_once=true", mock.calls.Load())

	changes, err := client.RepositoryChange.Query().All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("repository_changes=%d want=1 (only success advances change feed)", len(changes))
	}
	if changes[0].Sequence != 1 || changes[0].RepoID == nil || *changes[0].RepoID != fixtures[0].repositoryID {
		t.Fatalf("change=%s sequence=%d repo_id=%v want sequence=1 repo=%s", changes[0].ID, changes[0].Sequence, changes[0].RepoID, fixtures[0].repositoryID)
	}
	log.Logf("phase=assertion change_feed_rows=1 sequence=1 only_success_repo=%s", fixtures[0].repositoryID)

	batch, err := client.Batch.Get(ctx, fixtures[0].batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.CompletedAt != nil {
		t.Fatalf("batch=%s completed_at=%v want=nil (jobB still retry_wait)", fixtures[0].batchID, batch.CompletedAt)
	}
	log.Log("phase=assertion batch_completed=false reason=jobB_still_retry_wait")

	log.Logf("phase=summary succeeded=1 retry_wait=1 failed=1 github_requests=%d dlq_events=1 retry_events=1 change_feed_sequences=1", mock.calls.Load())
}
