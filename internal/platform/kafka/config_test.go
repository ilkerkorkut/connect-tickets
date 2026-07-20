package kafka_test

import (
	"testing"

	"github.com/IBM/sarama"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/kafka"
)

func validKafkaConfig() config.KafkaConfig {
	return config.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		ClientID:      "github-tracker",
		ConsumerGroup: "github-repo-refresh-v1",
		RefreshTopic:  "github.repo.refresh.v1",
		DLQTopic:      "github.repo.refresh.dlq.v1",
		Partitions:    5,

		DialTimeout:      10_000_000_000,
		RequestTimeout:   10_000_000_000,
		ProduceTimeout:   10_000_000_000,
		FetchMaxBytes:    1 << 20,
		MessageMaxBytes:  1 << 20,
		ProducerMaxBytes: 1 << 20,
	}
}

func TestNewSaramaConfigValid(t *testing.T) {
	t.Parallel()

	cfg := validKafkaConfig()
	sc, err := kafka.NewSaramaConfig(cfg)
	if err != nil {
		t.Fatalf("NewSaramaConfig: %v", err)
	}
	if sc.ClientID != "github-tracker" {
		t.Errorf("ClientID = %q", sc.ClientID)
	}
	if sc.Producer.RequiredAcks != sarama.WaitForAll {
		t.Error("expected WaitForAll producer acks for durability")
	}
	if !sc.Producer.Idempotent {
		t.Error("expected idempotent producer")
	}
	if sc.Consumer.Offsets.AutoCommit.Enable {
		t.Error("auto-commit must be disabled per architecture rule")
	}
	if !sc.Producer.Return.Successes {
		t.Error("producer must return successes for outbox publication")
	}
}

func TestNewSaramaConfigRejectsTopicMismatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*config.KafkaConfig)
	}{
		{"wrong refresh topic", func(c *config.KafkaConfig) { c.RefreshTopic = "wrong.topic" }},
		{"wrong dlq topic", func(c *config.KafkaConfig) { c.DLQTopic = "wrong.dlq" }},
		{"wrong consumer group", func(c *config.KafkaConfig) { c.ConsumerGroup = "wrong-group" }},
		{"wrong partition count", func(c *config.KafkaConfig) { c.Partitions = 3 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validKafkaConfig()
			tc.mutate(&cfg)

			_, err := kafka.NewSaramaConfig(cfg)
			if err == nil {
				t.Fatal("expected topic mismatch error")
			}
		})
	}
}

func TestNewSaramaConfigRejectsEmptyBrokers(t *testing.T) {
	t.Parallel()

	cfg := validKafkaConfig()
	cfg.Brokers = nil

	_, err := kafka.NewSaramaConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing brokers")
	}
}

func TestNewSaramaConfigRejectsUnsafeLimitsAndSASL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*config.KafkaConfig)
	}{
		{"zero produce timeout", func(c *config.KafkaConfig) { c.ProduceTimeout = 0 }},
		{"producer exceeds message limit", func(c *config.KafkaConfig) { c.ProducerMaxBytes = c.MessageMaxBytes + 1 }},
		{"unsupported SASL", func(c *config.KafkaConfig) {
			c.SASLMechanism = "GSSAPI"
			c.SASLUsername = "user"
			c.SASLPassword = "password"
		}},
		{"missing SASL credential", func(c *config.KafkaConfig) { c.SASLMechanism = "PLAIN" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validKafkaConfig()
			tc.mutate(&cfg)
			if _, err := kafka.NewSaramaConfig(cfg); err == nil {
				t.Fatal("expected invalid configuration error")
			}
		})
	}
}

func TestLockedTopicConstants(t *testing.T) {
	t.Parallel()

	if kafka.RefreshTopic != "github.repo.refresh.v1" {
		t.Errorf("RefreshTopic = %q", kafka.RefreshTopic)
	}
	if kafka.DLQTopic != "github.repo.refresh.dlq.v1" {
		t.Errorf("DLQTopic = %q", kafka.DLQTopic)
	}
	if kafka.ConsumerGroup != "github-repo-refresh-v1" {
		t.Errorf("ConsumerGroup = %q", kafka.ConsumerGroup)
	}
	if kafka.ExpectedPartitions != 5 {
		t.Errorf("ExpectedPartitions = %d, want 5", kafka.ExpectedPartitions)
	}
}
