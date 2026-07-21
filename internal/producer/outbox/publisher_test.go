package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

type fakeStore struct {
	mu           sync.Mutex
	events       []Event
	claimErr     error
	markErr      error
	releaseErr   error
	claimedOwner string
	published    []uuid.UUID
	released     []releaseCall
	actions      []string
}

type releaseCall struct {
	id          uuid.UUID
	owner       string
	availableAt time.Time
	now         time.Time
	safeError   string
}

func (s *fakeStore) ClaimDue(_ context.Context, owner string, _ time.Time, _ time.Duration, _ int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, "claim")
	s.claimedOwner = owner
	return append([]Event(nil), s.events...), s.claimErr
}

func (s *fakeStore) MarkPublished(_ context.Context, id uuid.UUID, _ string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, "mark")
	s.published = append(s.published, id)
	return s.markErr
}

func (s *fakeStore) Release(_ context.Context, id uuid.UUID, owner string, availableAt, now time.Time, safeError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, "release")
	s.released = append(s.released, releaseCall{id: id, owner: owner, availableAt: availableAt, now: now, safeError: safeError})
	return s.releaseErr
}

type fakeProducer struct {
	mu      sync.Mutex
	err     error
	records []*sarama.ProducerMessage
}

func (p *fakeProducer) Publish(_ context.Context, record *sarama.ProducerMessage) (kafka.Delivery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, record)
	if p.err != nil {
		return kafka.Delivery{}, p.err
	}
	return kafka.Delivery{Partition: 1, Offset: 2}, nil
}

func publisherConfig() config.OutboxConfig {
	return config.OutboxConfig{PollInterval: time.Second, BatchSize: 10, PublishLease: time.Minute, MaxPublishAttempts: 3, ShutdownTimeout: time.Second}
}

func refreshEvent(t *testing.T) Event {
	t.Helper()
	message := kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: uuid.NewString(), BatchJobID: uuid.NewString(), BatchID: uuid.NewString(), RepositoryID: uuid.NewString(), Attempt: 0, CreatedAt: time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC), CorrelationID: "request-123"}
	payload, err := kafka.EncodeRefresh(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return Event{ID: uuid.New(), EventID: message.EventID, EventType: kafka.RefreshTopic, PartitionKey: message.RepositoryID, Payload: payload, PublishAttempts: 1}
}

func TestPublisherMarksOnlyAcknowledgedEvents(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []Event{refreshEvent(t)}}
	producer := &fakeProducer{}
	publisher, err := NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC)
	publisher.now = func() time.Time { return now }
	count, err := publisher.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(store.published) != 1 || len(store.released) != 0 || len(producer.records) != 1 {
		t.Fatalf("count=%d published=%d released=%d produced=%d", count, len(store.published), len(store.released), len(producer.records))
	}
	if got, want := store.actions, []string{"claim", "mark"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("actions = %v", got)
	}
}

func TestPublisherReturnsKafkaFailureToPending(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []Event{refreshEvent(t)}}
	producer := &fakeProducer{err: errors.New("kafka unavailable")}
	publisher, err := NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC)
	publisher.now = func() time.Time { return now }
	count, err := publisher.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(store.published) != 0 || len(store.released) != 1 {
		t.Fatalf("count=%d published=%d released=%d", count, len(store.published), len(store.released))
	}
	release := store.released[0]
	if release.safeError != "kafka_publish_failed" || !release.availableAt.After(now) || release.owner != publisher.owner {
		t.Fatalf("release = %#v", release)
	}
}

func TestPublisherRepublishesAfterFinalizeCrashWindow(t *testing.T) {
	t.Parallel()
	event := refreshEvent(t)
	store := &fakeStore{events: []Event{event}, markErr: errors.New("database lost after acknowledgement")}
	producer := &fakeProducer{}
	publisher, err := NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.RunOnce(context.Background()); err == nil {
		t.Fatal("expected finalize error")
	}
	store.markErr = nil
	publisher, err = NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(producer.records) != 2 || len(store.published) != 2 {
		t.Fatalf("produced=%d marked=%d", len(producer.records), len(store.published))
	}
}

func TestPublisherRejectsInvalidPayloadWithoutKafka(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []Event{{ID: uuid.New(), EventID: uuid.NewString(), EventType: kafka.RefreshTopic, PartitionKey: uuid.NewString(), Payload: []byte(`{}`), PublishAttempts: 1}}}
	producer := &fakeProducer{}
	publisher, err := NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(producer.records) != 0 || len(store.released) != 1 || store.released[0].safeError != "invalid_outbox_payload" {
		t.Fatalf("records=%d releases=%#v", len(producer.records), store.released)
	}
}

func newPublisherCaptureLogger(t *testing.T) (*bytes.Buffer, context.Context) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger, err := logging.NewLogger(config.LogConfig{Level: "debug", Format: "json"}, buf)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	return buf, logging.WithLogger(context.Background(), logger)
}

func findPublisherLog(buf *bytes.Buffer, message string) map[string]any {
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == message {
			return record
		}
	}
	return nil
}

func TestPublisherLogsPublishedEvent(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []Event{refreshEvent(t)}}
	producer := &fakeProducer{}
	publisher, err := NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	publisher.now = func() time.Time { return time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC) }
	buf, ctx := newPublisherCaptureLogger(t)
	ctx = logging.WithPublisherID(ctx, publisher.owner)
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record := findPublisherLog(buf, "outbox event published")
	if record == nil {
		t.Fatalf("missing published log: %s", buf.String())
	}
	if record["event_type"] != kafka.RefreshTopic {
		t.Errorf("event_type = %v", record["event_type"])
	}
	if record["publisher_id"] != publisher.owner {
		t.Errorf("publisher_id = %v, want %s", record["publisher_id"], publisher.owner)
	}
}

func TestPublisherLogsPublishFailureSafeError(t *testing.T) {
	t.Parallel()
	store := &fakeStore{events: []Event{refreshEvent(t)}}
	producer := &fakeProducer{err: errors.New("kafka unavailable")}
	publisher, err := NewPublisher(store, producer, publisherConfig())
	if err != nil {
		t.Fatal(err)
	}
	publisher.now = func() time.Time { return time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC) }
	buf, ctx := newPublisherCaptureLogger(t)
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record := findPublisherLog(buf, "outbox publish failed")
	if record == nil {
		t.Fatalf("missing publish-failed log: %s", buf.String())
	}
	if record["safe_error"] != "kafka_publish_failed" {
		t.Errorf("safe_error = %v", record["safe_error"])
	}
	if strings.Contains(buf.String(), "kafka unavailable") {
		t.Fatalf("raw kafka error leaked: %s", buf.String())
	}
}
