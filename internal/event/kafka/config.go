package kafka

import (
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/ilkerkorkut/connect-tickets/config"
)

func NewSaramaConfig(cfg config.KafkaConfig) (*sarama.Config, error) {
	if err := ValidateTopics(cfg); err != nil {
		return nil, err
	}
	if !validConfig(cfg) {
		return nil, ErrInvalidConfig
	}

	c := sarama.NewConfig()
	c.ClientID = cfg.ClientID
	c.Version = sarama.V2_1_0_0

	configureNet(c, cfg)
	configureProducer(c, cfg)
	configureConsumer(c, cfg)
	configureAdmin(c, cfg)

	if err := c.Validate(); err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	return c, nil
}

func configureNet(c *sarama.Config, cfg config.KafkaConfig) {
	c.Net.DialTimeout = cfg.DialTimeout
	c.Net.ReadTimeout = cfg.RequestTimeout
	c.Net.WriteTimeout = cfg.RequestTimeout
	c.Net.MaxOpenRequests = 5

	if cfg.TLSEnabled {
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	if cfg.SASLMechanism != "" {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = cfg.SASLUsername
		c.Net.SASL.Password = cfg.SASLPassword
		c.Net.SASL.Mechanism = toSASLMechanism(cfg.SASLMechanism)
	}
}

func configureProducer(c *sarama.Config, cfg config.KafkaConfig) {
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.MaxMessageBytes = int(cfg.ProducerMaxBytes)
	c.Producer.Timeout = cfg.ProduceTimeout
	c.Producer.Retry.Max = 3
	c.Producer.Retry.Backoff = 100 * time.Millisecond
	c.Producer.Flush.Frequency = 100 * time.Millisecond
	c.Producer.Flush.MaxMessages = 500
	c.Producer.Idempotent = true
	c.Net.MaxOpenRequests = 1
}

func configureConsumer(c *sarama.Config, cfg config.KafkaConfig) {
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.AutoCommit.Enable = false
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.Fetch.Min = 1
	c.Consumer.Fetch.Default = int32(cfg.FetchMaxBytes)
	c.Consumer.Fetch.MaxBytes = int32(cfg.FetchMaxBytes)
	c.Consumer.MaxProcessingTime = 2 * time.Minute

	c.Consumer.Group.Session.Timeout = 30 * time.Second
	c.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	c.Consumer.Group.Rebalance.Timeout = cfg.RequestTimeout
	c.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategySticky()
}

func configureAdmin(c *sarama.Config, _ config.KafkaConfig) {
	c.Admin.Timeout = 10 * time.Second
}

func toSASLMechanism(raw string) sarama.SASLMechanism {
	switch strings.ToUpper(raw) {
	case "SCRAM-SHA-256":
		return sarama.SASLTypeSCRAMSHA256
	case "SCRAM-SHA-512":
		return sarama.SASLTypeSCRAMSHA512
	default:
		return sarama.SASLTypePlaintext
	}
}

func validConfig(cfg config.KafkaConfig) bool {
	if strings.TrimSpace(cfg.ClientID) == "" || len(cfg.Brokers) == 0 || cfg.DialTimeout <= 0 || cfg.RequestTimeout <= 0 || cfg.ProduceTimeout <= 0 || cfg.FetchMaxBytes <= 0 || cfg.MessageMaxBytes <= 0 || cfg.ProducerMaxBytes <= 0 || cfg.ProducerMaxBytes > cfg.MessageMaxBytes {
		return false
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return false
		}
	}
	if cfg.SASLMechanism == "" {
		return true
	}
	if strings.TrimSpace(cfg.SASLUsername) == "" || cfg.SASLPassword == "" {
		return false
	}
	switch strings.ToUpper(cfg.SASLMechanism) {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		return true
	default:
		return false
	}
}
