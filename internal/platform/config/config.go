package config

import (
	"fmt"
	"time"
)

type Config struct {
	App      AppConfig
	Log      LogConfig
	HTTP     HTTPConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Cache    CacheConfig
	Kafka    KafkaConfig
	GitHub   GitHubConfig
	Worker   WorkerConfig
	Outbox   OutboxConfig
}

type AppConfig struct {
	Environment     string
	Mode            string
	Version         string
	ShutdownTimeout time.Duration
}

type LogConfig struct {
	Level  string
	Format string
}

type HTTPConfig struct {
	Port              int
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxBodyBytes      int64
	RequestTimeout    time.Duration
}

type DatabaseConfig struct {
	URL               string
	MaxOpenConns      int32
	MaxIdleConns      int32
	ConnMaxLifetime   time.Duration
	HealthCheckPeriod time.Duration
	QueryTimeout      time.Duration
}

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type CacheConfig struct {
	TTL      time.Duration
	LockTTL  time.Duration
	LockWait time.Duration
}

type KafkaConfig struct {
	Brokers       []string
	ClientID      string
	ConsumerGroup string
	RefreshTopic  string
	DLQTopic      string
	Partitions    int

	DialTimeout      time.Duration
	RequestTimeout   time.Duration
	ProduceTimeout   time.Duration
	FetchMaxBytes    int32
	MessageMaxBytes  int32
	ProducerMaxBytes int32

	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool
}

type GitHubConfig struct {
	Token     string
	Timeout   time.Duration
	UserAgent string

	BaseURL             string
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
}

type WorkerConfig struct {
	LeaseDuration   time.Duration
	MaxAttempts     int
	Concurrency     int
	ShutdownTimeout time.Duration
}

type OutboxConfig struct {
	PollInterval       time.Duration
	BatchSize          int
	PublishLease       time.Duration
	MaxPublishAttempts int
	ShutdownTimeout    time.Duration
}

type Loader func(key string) string

func Load() (Config, error) {
	return LoadFrom(getenvOS)
}

func LoadFrom(getenv Loader) (Config, error) {
	cfg := defaults()
	v := &ValidationError{}

	cfg.App.Environment = stringOr(getenv, "APP_ENV", cfg.App.Environment)
	cfg.App.Mode = stringOr(getenv, "APP_MODE", cfg.App.Mode)
	cfg.App.Version = stringOr(getenv, "APP_VERSION", cfg.App.Version)
	cfg.App.ShutdownTimeout = durationOr(v, getenv, "SHUTDOWN_TIMEOUT", cfg.App.ShutdownTimeout)

	cfg.Log.Level = stringOr(getenv, "LOG_LEVEL", cfg.Log.Level)
	cfg.Log.Format = stringOr(getenv, "LOG_FORMAT", cfg.Log.Format)

	cfg.HTTP.Port = intOr(v, getenv, "HTTP_PORT", cfg.HTTP.Port)
	cfg.HTTP.ReadHeaderTimeout = durationOr(v, getenv, "HTTP_READ_HEADER_TIMEOUT", cfg.HTTP.ReadHeaderTimeout)
	cfg.HTTP.ReadTimeout = durationOr(v, getenv, "HTTP_READ_TIMEOUT", cfg.HTTP.ReadTimeout)
	cfg.HTTP.WriteTimeout = durationOr(v, getenv, "HTTP_WRITE_TIMEOUT", cfg.HTTP.WriteTimeout)
	cfg.HTTP.IdleTimeout = durationOr(v, getenv, "HTTP_IDLE_TIMEOUT", cfg.HTTP.IdleTimeout)
	cfg.HTTP.MaxBodyBytes = int64Or(v, getenv, "HTTP_MAX_BODY_BYTES", cfg.HTTP.MaxBodyBytes)
	cfg.HTTP.RequestTimeout = durationOr(v, getenv, "HTTP_REQUEST_TIMEOUT", cfg.HTTP.RequestTimeout)

	cfg.Database.URL = getenv("DATABASE_URL")
	cfg.Database.MaxOpenConns = int32Or(v, getenv, "DATABASE_MAX_OPEN_CONNS", cfg.Database.MaxOpenConns)
	cfg.Database.MaxIdleConns = int32Or(v, getenv, "DATABASE_MAX_IDLE_CONNS", cfg.Database.MaxIdleConns)
	cfg.Database.ConnMaxLifetime = durationOr(v, getenv, "DATABASE_CONN_MAX_LIFETIME", cfg.Database.ConnMaxLifetime)
	cfg.Database.HealthCheckPeriod = durationOr(v, getenv, "DATABASE_HEALTH_CHECK_PERIOD", cfg.Database.HealthCheckPeriod)
	cfg.Database.QueryTimeout = durationOr(v, getenv, "DATABASE_QUERY_TIMEOUT", cfg.Database.QueryTimeout)

	cfg.Redis.Addr = stringOr(getenv, "REDIS_ADDR", cfg.Redis.Addr)
	cfg.Redis.Password = getenv("REDIS_PASSWORD")
	cfg.Redis.DB = intOr(v, getenv, "REDIS_DB", cfg.Redis.DB)
	cfg.Redis.PoolSize = intOr(v, getenv, "REDIS_POOL_SIZE", cfg.Redis.PoolSize)
	cfg.Redis.MinIdleConns = intOr(v, getenv, "REDIS_MIN_IDLE_CONNS", cfg.Redis.MinIdleConns)
	cfg.Redis.DialTimeout = durationOr(v, getenv, "REDIS_DIAL_TIMEOUT", cfg.Redis.DialTimeout)
	cfg.Redis.ReadTimeout = durationOr(v, getenv, "REDIS_READ_TIMEOUT", cfg.Redis.ReadTimeout)
	cfg.Redis.WriteTimeout = durationOr(v, getenv, "REDIS_WRITE_TIMEOUT", cfg.Redis.WriteTimeout)

	cfg.Cache.TTL = durationOr(v, getenv, "CACHE_TTL", cfg.Cache.TTL)
	cfg.Cache.LockTTL = durationOr(v, getenv, "CACHE_LOCK_TTL", cfg.Cache.LockTTL)
	cfg.Cache.LockWait = durationOr(v, getenv, "CACHE_LOCK_WAIT", cfg.Cache.LockWait)

	cfg.Kafka.Brokers = stringsCSVOr(getenv, "KAFKA_BROKERS", cfg.Kafka.Brokers)
	cfg.Kafka.ClientID = stringOr(getenv, "KAFKA_CLIENT_ID", cfg.Kafka.ClientID)
	cfg.Kafka.ConsumerGroup = stringOr(getenv, "KAFKA_CONSUMER_GROUP", cfg.Kafka.ConsumerGroup)
	cfg.Kafka.RefreshTopic = stringOr(getenv, "KAFKA_REFRESH_TOPIC", cfg.Kafka.RefreshTopic)
	cfg.Kafka.DLQTopic = stringOr(getenv, "KAFKA_DLQ_TOPIC", cfg.Kafka.DLQTopic)
	cfg.Kafka.Partitions = intOr(v, getenv, "KAFKA_EXPECTED_REFRESH_PARTITIONS", cfg.Kafka.Partitions)
	cfg.Kafka.DialTimeout = durationOr(v, getenv, "KAFKA_DIAL_TIMEOUT", cfg.Kafka.DialTimeout)
	cfg.Kafka.RequestTimeout = durationOr(v, getenv, "KAFKA_REQUEST_TIMEOUT", cfg.Kafka.RequestTimeout)
	cfg.Kafka.ProduceTimeout = durationOr(v, getenv, "KAFKA_PRODUCE_TIMEOUT", cfg.Kafka.ProduceTimeout)
	cfg.Kafka.FetchMaxBytes = int32Or(v, getenv, "KAFKA_FETCH_MAX_BYTES", cfg.Kafka.FetchMaxBytes)
	cfg.Kafka.MessageMaxBytes = int32Or(v, getenv, "KAFKA_MESSAGE_MAX_BYTES", cfg.Kafka.MessageMaxBytes)
	cfg.Kafka.ProducerMaxBytes = int32Or(v, getenv, "KAFKA_PRODUCER_MAX_BYTES", cfg.Kafka.ProducerMaxBytes)
	cfg.Kafka.SASLMechanism = getenv("KAFKA_SASL_MECHANISM")
	cfg.Kafka.SASLUsername = getenv("KAFKA_SASL_USERNAME")
	cfg.Kafka.SASLPassword = getenv("KAFKA_SASL_PASSWORD")
	cfg.Kafka.TLSEnabled = boolOr(v, getenv, "KAFKA_TLS_ENABLED", cfg.Kafka.TLSEnabled)

	cfg.GitHub.Token = getenv("GITHUB_TOKEN")
	cfg.GitHub.Timeout = durationOr(v, getenv, "GITHUB_TIMEOUT", cfg.GitHub.Timeout)
	cfg.GitHub.UserAgent = stringOr(getenv, "GITHUB_USER_AGENT", cfg.GitHub.UserAgent)
	cfg.GitHub.BaseURL = stringOr(getenv, "GITHUB_BASE_URL", cfg.GitHub.BaseURL)
	cfg.GitHub.MaxIdleConns = intOr(v, getenv, "GITHUB_MAX_IDLE_CONNS", cfg.GitHub.MaxIdleConns)
	cfg.GitHub.MaxIdleConnsPerHost = intOr(v, getenv, "GITHUB_MAX_IDLE_CONNS_PER_HOST", cfg.GitHub.MaxIdleConnsPerHost)
	cfg.GitHub.IdleConnTimeout = durationOr(v, getenv, "GITHUB_IDLE_CONN_TIMEOUT", cfg.GitHub.IdleConnTimeout)

	cfg.Worker.LeaseDuration = durationOr(v, getenv, "WORKER_LEASE_DURATION", cfg.Worker.LeaseDuration)
	cfg.Worker.MaxAttempts = intOr(v, getenv, "WORKER_MAX_ATTEMPTS", cfg.Worker.MaxAttempts)
	cfg.Worker.Concurrency = intOr(v, getenv, "WORKER_CONCURRENCY", cfg.Worker.Concurrency)
	cfg.Worker.ShutdownTimeout = durationOr(v, getenv, "WORKER_SHUTDOWN_TIMEOUT", cfg.Worker.ShutdownTimeout)

	cfg.Outbox.PollInterval = durationOr(v, getenv, "OUTBOX_POLL_INTERVAL", cfg.Outbox.PollInterval)
	cfg.Outbox.BatchSize = intOr(v, getenv, "OUTBOX_BATCH_SIZE", cfg.Outbox.BatchSize)
	cfg.Outbox.PublishLease = durationOr(v, getenv, "OUTBOX_PUBLISH_LEASE", cfg.Outbox.PublishLease)
	cfg.Outbox.MaxPublishAttempts = intOr(v, getenv, "OUTBOX_MAX_PUBLISH_ATTEMPTS", cfg.Outbox.MaxPublishAttempts)
	cfg.Outbox.ShutdownTimeout = durationOr(v, getenv, "OUTBOX_SHUTDOWN_TIMEOUT", cfg.Outbox.ShutdownTimeout)

	if parseErr := v.AsError(); parseErr != nil {
		return Config{}, parseErr
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func stringOr(getenv Loader, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}

func (c Config) String() string {
	return fmt.Sprintf("config{app=%s mode=%s env=%s}", c.App.Version, c.App.Mode, c.App.Environment)
}
