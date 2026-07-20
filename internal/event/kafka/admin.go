package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/ilkerkorkut/connect-tickets/config"
)

type TopicAdmin interface {
	ListTopics() (map[string]sarama.TopicDetail, error)
	CreateTopic(string, *sarama.TopicDetail, bool) error
	Close() error
}

func NewTopicAdmin(cfg config.KafkaConfig, saramaConfig *sarama.Config) (TopicAdmin, error) {
	return sarama.NewClusterAdmin(cfg.Brokers, saramaConfig)
}

func EnsureTopics(ctx context.Context, admin TopicAdmin) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}
	if err := ensureTopic(admin, topics, RefreshTopic, ExpectedPartitions); err != nil {
		return err
	}
	if err := ensureDLQTopic(admin, topics); err != nil {
		return err
	}
	return ctx.Err()
}

func ValidateMainTopic(ctx context.Context, admin TopicAdmin) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}
	detail, ok := topics[RefreshTopic]
	if !ok || detail.NumPartitions != ExpectedPartitions {
		return ErrPartitionCount
	}
	return nil
}

func ensureTopic(admin TopicAdmin, topics map[string]sarama.TopicDetail, topic string, partitions int32) error {
	if detail, ok := topics[topic]; ok {
		if detail.NumPartitions != partitions {
			return ErrPartitionCount
		}
		return nil
	}
	return admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: partitions, ReplicationFactor: -1}, false)
}

func ensureDLQTopic(admin TopicAdmin, topics map[string]sarama.TopicDetail) error {
	if detail, ok := topics[DLQTopic]; ok {
		if detail.NumPartitions <= 0 {
			return ErrPartitionCount
		}
		return nil
	}
	return admin.CreateTopic(DLQTopic, &sarama.TopicDetail{NumPartitions: DLQPartitions, ReplicationFactor: -1}, false)
}
