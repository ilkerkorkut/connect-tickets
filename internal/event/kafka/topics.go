package kafka

import "github.com/ilkerkorkut/connect-tickets/config"

const (
	RefreshTopic       = "github.repo.refresh.v1"
	DLQTopic           = "github.repo.refresh.dlq.v1"
	ConsumerGroup      = "github-repo-refresh-v1"
	ExpectedPartitions = 5
	MessageKeyRepoID   = "repo_id"
	DLQPartitions      = 1
)

func ValidateTopics(cfg config.KafkaConfig) error {
	if cfg.RefreshTopic != RefreshTopic {
		return ErrTopicMismatch
	}
	if cfg.DLQTopic != DLQTopic {
		return ErrTopicMismatch
	}
	if cfg.ConsumerGroup != ConsumerGroup {
		return ErrTopicMismatch
	}
	if cfg.Partitions != ExpectedPartitions {
		return ErrTopicMismatch
	}
	return nil
}
