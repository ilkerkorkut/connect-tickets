//go:build testcontainers

package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	githubclient "github.com/ilkerkorkut/connect-tickets/internal/client/github"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
	"github.com/ilkerkorkut/connect-tickets/internal/testkit"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

const concurrentRateLimitWorkers = 5

func TestConcurrentWorkerRateLimitSchedulesRetries(t *testing.T) {
	log := newRateLimitScenarioLogger(t)
	log.Logf("scenario=concurrent_worker_rate_limit workers=%d expected_github_requests=1 expected_retry_wait_jobs=%d", concurrentRateLimitWorkers, concurrentRateLimitWorkers+1)
	log.Log("scenario_detail=one_worker_receives_github_429; redis_persists_the_shared_retry_after_cooldown; five_workers_start_from_separate_kafka_partitions_and_schedule_durable_retries_without_hammering_github")

	environment := testkit.Start(t)
	client := newRateLimitEntClient(t, environment.DatabaseURL)
	cache := newRateLimitRedisClient(t, environment.RedisAddress)
	producer, partitions := newRateLimitKafka(t, log, environment.KafkaBrokers)
	mock := newRateLimitedGitHubMock(t, log)
	gateway := newRateLimitGateway(t, cache, mock.server.URL)
	jobs := seedRateLimitJobs(t, client, concurrentRateLimitWorkers+1)
	workerConsumer, err := NewConsumer(NewStore(client), gateway, "rate-limit-concurrency-worker", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}

	log.Log("phase=kafka action=publish_trigger_worker partition=0")
	publishRateLimitJob(t, producer, jobs[0], 0)
	triggerRecord := consumeRateLimitRecord(t, partitions[0])
	log.Log("phase=execution action=process_rate_limited_worker")
	settlement, err := workerConsumer.Process(context.Background(), triggerRecord)
	if err != nil || !settlement.Commit {
		t.Fatalf("rate-limited worker settlement=%#v err=%v", settlement, err)
	}
	cooldown, err := gateway.Cooldown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cooldown.Before(mock.retryUntil()) {
		t.Fatalf("cooldown=%s retry_until=%s", cooldown, mock.retryUntil())
	}
	log.Logf("phase=execution action=shared_cooldown_persisted cooldown_until=%s", cooldown.Format(time.RFC3339Nano))

	records := make([]*sarama.ConsumerMessage, 0, concurrentRateLimitWorkers)
	for index, job := range jobs[1:] {
		partition := int32(index)
		log.Logf("phase=kafka action=publish_deferred_worker partition=%d", partition)
		publishRateLimitJob(t, producer, job, partition)
		records = append(records, consumeRateLimitRecord(t, partitions[partition]))
	}
	results := processDeferredWorkers(t, log, workerConsumer, records)
	for _, result := range results {
		if result.err != nil || !result.settlement.Commit {
			t.Fatalf("worker=%d settlement=%#v err=%v", result.number, result.settlement, result.err)
		}
	}
	if calls := mock.calls.Load(); calls != 1 {
		t.Fatalf("github requests=%d, want 1", calls)
	}
	assertRateLimitRetryJobs(t, log, client, jobs, cooldown)
}

func newRateLimitEntClient(t *testing.T, databaseURL string) *ent.Client {
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

func newRateLimitRedisClient(t *testing.T, address string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func newRateLimitGateway(t *testing.T, cache *redis.Client, baseURL string) *cache.Gateway {
	t.Helper()
	upstream, err := githubclient.NewClient(config.GitHubConfig{BaseURL: baseURL, Timeout: 2 * time.Second, UserAgent: "rate-limit-concurrency", MaxIdleConns: concurrentRateLimitWorkers, MaxIdleConnsPerHost: concurrentRateLimitWorkers, IdleConnTimeout: time.Minute}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := cache.NewGateway(upstream, cache, config.CacheConfig{TTL: time.Minute, LockTTL: 3 * time.Second, LockWait: 4 * time.Second}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

type rateLimitedGitHubMock struct {
	server  *httptest.Server
	calls   atomic.Int32
	retryAt atomic.Int64
}

func newRateLimitedGitHubMock(t *testing.T, log *rateLimitScenarioLogger) *rateLimitedGitHubMock {
	t.Helper()
	mock := &rateLimitedGitHubMock{}
	mock.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call := mock.calls.Add(1)
		retryAt := time.Now().UTC().Add(20 * time.Second).Truncate(time.Second)
		mock.retryAt.Store(retryAt.UnixNano())
		log.Logf("phase=github_mock call=%d method=%s path=%s outcome=429 retry_after=20s", call, request.Method, request.URL.Path)
		writer.Header().Set("Retry-After", "20")
		writer.Header().Set("X-RateLimit-Remaining", "0")
		writer.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", retryAt.Unix()))
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(mock.server.Close)
	return mock
}

func (mock *rateLimitedGitHubMock) retryUntil() time.Time {
	return time.Unix(0, mock.retryAt.Load()).UTC()
}

type rateLimitJob struct {
	id           uuid.UUID
	repositoryID uuid.UUID
	payload      []byte
}

func seedRateLimitJobs(t *testing.T, client *ent.Client, count int) []rateLimitJob {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	batchID := uuid.New()
	if _, err := client.Batch.Create().SetID(batchID).SetCreatedAt(now).Save(ctx); err != nil {
		t.Fatal(err)
	}
	owner := "rate-limit-" + uuid.NewString()
	jobs := make([]rateLimitJob, 0, count)
	for index := range count {
		name := fmt.Sprintf("repo-%d", index+1)
		fullName := owner + "/" + name
		repository, err := client.Repository.Create().SetID(uuid.New()).SetOwner(owner).SetName(name).SetFullName(fullName).SetNormalizedFullName(fullName).SetStars(0).SetHTMLURL("https://github.example/" + fullName).SetFetchedAt(now).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		job, err := client.BatchJob.Create().SetID(uuid.New()).SetBatchID(batchID).SetRepoID(repository.ID).SetStatus(batchjob.StatusPending).SetAttemptCount(0).SetCreatedAt(now).SetUpdatedAt(now).Save(ctx)
		if err != nil {
			t.Fatal(err)
		}
		message := kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: uuid.NewString(), BatchJobID: job.ID.String(), BatchID: batchID.String(), RepositoryID: repository.ID.String(), CreatedAt: now, CorrelationID: "rate-limit-concurrency"}
		payload, err := kafka.EncodeRefresh(message, 4096)
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, rateLimitJob{id: job.ID, repositoryID: repository.ID, payload: payload})
	}
	return jobs
}

type workerProcessResult struct {
	number     int
	settlement kafka.Settlement
	err        error
}

func processDeferredWorkers(t *testing.T, log *rateLimitScenarioLogger, consumer *Consumer, records []*sarama.ConsumerMessage) []workerProcessResult {
	t.Helper()
	start := make(chan struct{})
	results := make(chan workerProcessResult, len(records))
	var workers sync.WaitGroup
	log.Logf("phase=execution action=launch_workers count=%d synchronization=shared_start_gate kafka_partitions=%d", len(records), len(records))
	for index, record := range records {
		workers.Add(1)
		go func(number int, record *sarama.ConsumerMessage) {
			defer workers.Done()
			<-start
			log.Logf("phase=execution worker=%d action=process_refresh_message partition=%d offset=%d", number, record.Partition, record.Offset)
			settlement, err := consumer.Process(context.Background(), record)
			log.Logf("phase=execution worker=%d action=settlement commit=%t error=%t", number, settlement.Commit, err != nil)
			results <- workerProcessResult{number: number, settlement: settlement, err: err}
		}(index+1, record)
	}
	log.Log("phase=execution action=release_shared_start_gate")
	close(start)
	workers.Wait()
	close(results)
	processed := make([]workerProcessResult, 0, len(records))
	for result := range results {
		processed = append(processed, result)
	}
	return processed
}

func newRateLimitKafka(t *testing.T, log *rateLimitScenarioLogger, brokers []string) (sarama.SyncProducer, map[int32]sarama.PartitionConsumer) {
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
	partitions := make(map[int32]sarama.PartitionConsumer, concurrentRateLimitWorkers)
	for partition := int32(0); partition < concurrentRateLimitWorkers; partition++ {
		partitionConsumer, err := consumer.ConsumePartition(kafka.RefreshTopic, partition, sarama.OffsetOldest)
		if err != nil {
			t.Fatal(err)
		}
		partitions[partition] = partitionConsumer
		t.Cleanup(func() { _ = partitionConsumer.Close() })
	}
	log.Logf("phase=setup action=kafka_ready topic=%s partitions=%d", kafka.RefreshTopic, concurrentRateLimitWorkers)
	return producer, partitions
}

func publishRateLimitJob(t *testing.T, producer sarama.SyncProducer, job rateLimitJob, partition int32) {
	t.Helper()
	_, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: kafka.RefreshTopic, Partition: partition, Key: sarama.StringEncoder(job.repositoryID.String()), Value: sarama.ByteEncoder(job.payload)})
	if err != nil {
		t.Fatal(err)
	}
}

func consumeRateLimitRecord(t *testing.T, partitionConsumer sarama.PartitionConsumer) *sarama.ConsumerMessage {
	t.Helper()
	select {
	case message := <-partitionConsumer.Messages():
		if message == nil {
			t.Fatal("kafka partition closed before receiving refresh message")
		}
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

func assertRateLimitRetryJobs(t *testing.T, log *rateLimitScenarioLogger, client *ent.Client, jobs []rateLimitJob, cooldown time.Time) {
	t.Helper()
	ctx := context.Background()
	for index, fixture := range jobs {
		job, err := client.BatchJob.Get(ctx, fixture.id)
		if err != nil {
			t.Fatal(err)
		}
		wantAttempts := 0
		if index == 0 {
			wantAttempts = 1
		}
		if job.Status != batchjob.StatusRetryWait || job.AttemptCount != wantAttempts || job.LastErrorCode != "github_rate_limited" || job.NextAttemptAt == nil || job.NextAttemptAt.Before(cooldown) {
			t.Fatalf("job=%s status=%s attempts=%d error_code=%q next_attempt_at=%v cooldown=%s", fixture.id, job.Status, job.AttemptCount, job.LastErrorCode, job.NextAttemptAt, cooldown)
		}
		outboxCount, err := client.OutboxEvent.Query().Where(outboxevent.AggregateIDEQ(fixture.id), outboxevent.EventTypeEQ(kafka.RefreshTopic), outboxevent.StatusEQ(outboxevent.StatusPending)).Count(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if outboxCount != 1 {
			t.Fatalf("job=%s retry outbox events=%d, want 1", fixture.id, outboxCount)
		}
	}
	log.Logf("phase=assertion github_requests=1 retry_wait_jobs=%d durable_retry_outbox_events=%d attempts_consumed=1 cooldown_honored=true", len(jobs), len(jobs))
}

type rateLimitScenarioLogger struct {
	t    *testing.T
	mu   sync.Mutex
	step int
}

func newRateLimitScenarioLogger(t *testing.T) *rateLimitScenarioLogger {
	t.Helper()
	return &rateLimitScenarioLogger{t: t}
}

func (logger *rateLimitScenarioLogger) Log(format string, args ...any) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.step++
	logger.t.Logf("step=%02d "+format, append([]any{logger.step}, args...)...)
}

func (logger *rateLimitScenarioLogger) Logf(format string, args ...any) {
	logger.Log(format, args...)
}
