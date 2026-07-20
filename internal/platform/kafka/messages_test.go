package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

func refreshFixture() RefreshMessage {
	return RefreshMessage{
		SchemaVersion: RefreshSchemaVersion,
		EventID:       uuid.MustParse("11111111-1111-1111-1111-111111111111").String(),
		BatchJobID:    uuid.MustParse("22222222-2222-2222-2222-222222222222").String(),
		BatchID:       uuid.MustParse("33333333-3333-3333-3333-333333333333").String(),
		RepositoryID:  uuid.MustParse("44444444-4444-4444-4444-444444444444").String(),
		Attempt:       1,
		CreatedAt:     time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC),
		CorrelationID: "request-123",
	}
}

func TestRefreshMessageRoundTripAndHeaders(t *testing.T) {
	t.Parallel()
	message := refreshFixture()
	payload, err := EncodeRefresh(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRefresh(payload, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != message {
		t.Fatalf("decoded = %#v, want %#v", decoded, message)
	}
	record, err := RefreshRecord(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	key, err := record.Key.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != message.RepositoryID {
		t.Fatalf("key = %q, want %q", key, message.RepositoryID)
	}
	headers := Headers(&sarama.ConsumerMessage{Headers: toConsumerHeaders(record.Headers)})
	if headers["x-correlation-id"] != message.CorrelationID || headers["x-event-id"] != message.EventID {
		t.Fatalf("headers = %#v", headers)
	}
}

func TestMessageValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(*RefreshMessage)
		max     int
		wantErr error
	}{
		{"schema version", func(m *RefreshMessage) { m.SchemaVersion = 2 }, 4096, ErrSchemaVersion},
		{"missing repository", func(m *RefreshMessage) { m.RepositoryID = "" }, 4096, ErrInvalidMessage},
		{"negative attempt", func(m *RefreshMessage) { m.Attempt = -1 }, 4096, ErrInvalidMessage},
		{"oversize", func(m *RefreshMessage) { m.CorrelationID = string(make([]byte, 1024)) }, 32, ErrMessageTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			message := refreshFixture()
			tc.mutate(&message)
			_, err := EncodeRefresh(message, tc.max)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestDLQSchemaRejectsUnsafeIncompleteData(t *testing.T) {
	t.Parallel()
	refresh := refreshFixture()
	message := DLQMessage{SchemaVersion: DLQSchemaVersion, EventID: uuid.NewString(), OriginalEventID: refresh.EventID, BatchJobID: refresh.BatchJobID, BatchID: refresh.BatchID, RepositoryID: refresh.RepositoryID, Attempt: 3, FailedAt: refresh.CreatedAt, ErrorCode: "github_timeout", CorrelationID: refresh.CorrelationID}
	payload, err := EncodeDLQ(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeDLQ(payload, 4096); err != nil {
		t.Fatal(err)
	}
	message.ErrorCode = ""
	if _, err := EncodeDLQ(message, 4096); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("error = %v", err)
	}
}

func TestDLQPayloadContainsOnlySafeFields(t *testing.T) {
	t.Parallel()
	refresh := refreshFixture()
	message := DLQMessage{SchemaVersion: DLQSchemaVersion, EventID: uuid.NewString(), OriginalEventID: refresh.EventID, BatchJobID: refresh.BatchJobID, BatchID: refresh.BatchID, RepositoryID: refresh.RepositoryID, Attempt: 3, FailedAt: refresh.CreatedAt, ErrorCode: "github_timeout", CorrelationID: refresh.CorrelationID}
	payload, err := EncodeDLQ(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"token", "authorization", "diagnostic", "body", "error_message"} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("DLQ payload contains %q", forbidden)
		}
	}
	if len(fields) != 10 {
		t.Fatalf("DLQ field count = %d, want 10", len(fields))
	}
}

func TestProducerDeliversAcknowledgedResult(t *testing.T) {
	t.Parallel()
	fake := &fakeAsyncProducer{input: make(chan *sarama.ProducerMessage, 1), successes: make(chan *sarama.ProducerMessage, 1), errors: make(chan *sarama.ProducerError, 1)}
	producer := newProducer(fake, 4096)
	message := refreshFixture()
	record, err := RefreshRecord(message, 4096)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		produced := <-fake.input
		produced.Partition = 2
		produced.Offset = 9
		fake.successes <- produced
		close(done)
	}()
	delivery, err := producer.Publish(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	<-done
	if delivery != (Delivery{Partition: 2, Offset: 9}) {
		t.Fatalf("delivery = %#v", delivery)
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func toConsumerHeaders(headers []sarama.RecordHeader) []*sarama.RecordHeader {
	result := make([]*sarama.RecordHeader, 0, len(headers))
	for i := range headers {
		result = append(result, &headers[i])
	}
	return result
}

type fakeAsyncProducer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
	once      sync.Once
}

func (p *fakeAsyncProducer) Input() chan<- *sarama.ProducerMessage     { return p.input }
func (p *fakeAsyncProducer) Successes() <-chan *sarama.ProducerMessage { return p.successes }
func (p *fakeAsyncProducer) Errors() <-chan *sarama.ProducerError      { return p.errors }
func (p *fakeAsyncProducer) AsyncClose() {
	p.once.Do(func() {
		close(p.successes)
		close(p.errors)
	})
}

type processorFunc func(context.Context, *sarama.ConsumerMessage) (Settlement, error)

func (f processorFunc) Process(ctx context.Context, message *sarama.ConsumerMessage) (Settlement, error) {
	return f(ctx, message)
}

type fakeSession struct {
	ctx       context.Context
	marked    int
	committed int
}

func (s *fakeSession) Claims() map[string][]int32                  { return map[string][]int32{RefreshTopic: {0}} }
func (s *fakeSession) MemberID() string                            { return "member" }
func (s *fakeSession) GenerationID() int32                         { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)     {}
func (s *fakeSession) Commit()                                     { s.committed++ }
func (s *fakeSession) ResetOffset(string, int32, int64, string)    {}
func (s *fakeSession) MarkMessage(*sarama.ConsumerMessage, string) { s.marked++ }
func (s *fakeSession) Context() context.Context                    { return s.ctx }

type fakeClaim struct{ messages chan *sarama.ConsumerMessage }

func (c fakeClaim) Topic() string                            { return RefreshTopic }
func (c fakeClaim) Partition() int32                         { return 0 }
func (c fakeClaim) InitialOffset() int64                     { return 0 }
func (c fakeClaim) HighWaterMarkOffset() int64               { return 1 }
func (c fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func TestConsumerSettlementIsExplicit(t *testing.T) {
	t.Parallel()
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "replayable", true: "committed"}[commit], func(t *testing.T) {
			messages := make(chan *sarama.ConsumerMessage, 1)
			messages <- &sarama.ConsumerMessage{Topic: RefreshTopic, Partition: 0, Offset: 7}
			close(messages)
			session := &fakeSession{ctx: context.Background()}
			handler := groupHandler{processor: processorFunc(func(context.Context, *sarama.ConsumerMessage) (Settlement, error) {
				return Settlement{Commit: commit}, nil
			})}
			if err := handler.ConsumeClaim(session, fakeClaim{messages: messages}); err != nil {
				t.Fatal(err)
			}
			if commit && (session.marked != 1 || session.committed != 1) {
				t.Fatalf("marked = %d, committed = %d", session.marked, session.committed)
			}
			if !commit && (session.marked != 0 || session.committed != 0) {
				t.Fatalf("marked = %d, committed = %d", session.marked, session.committed)
			}
		})
	}
}
