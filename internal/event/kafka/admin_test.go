package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

type fakeTopicAdmin struct {
	topics  map[string]sarama.TopicDetail
	created []string
	err     error
}

func (a *fakeTopicAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return a.topics, a.err
}

func (a *fakeTopicAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, _ bool) error {
	a.created = append(a.created, topic)
	a.topics[topic] = *detail
	return nil
}

func (a *fakeTopicAdmin) Close() error { return nil }

func TestEnsureTopicsAndValidateMainTopic(t *testing.T) {
	t.Parallel()
	admin := &fakeTopicAdmin{topics: map[string]sarama.TopicDetail{}}
	if err := EnsureTopics(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	if len(admin.created) != 2 {
		t.Fatalf("created = %v", admin.created)
	}
	if err := ValidateMainTopic(context.Background(), admin); err != nil {
		t.Fatal(err)
	}
	admin.topics[RefreshTopic] = sarama.TopicDetail{NumPartitions: ExpectedPartitions - 1}
	if err := ValidateMainTopic(context.Background(), admin); !errors.Is(err, ErrPartitionCount) {
		t.Fatalf("error = %v", err)
	}
}
