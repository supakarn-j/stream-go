package stream

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrEmptyStreamName is returned when a stream name is required but empty.
	ErrEmptyStreamName = errors.New("stream name cannot be empty")
)

// DefaultProducerMaxLen is the default approximate maximum number of entries kept in a stream.
const DefaultProducerMaxLen int64 = 1000

// Producer writes messages to Redis Streams.
type Producer struct {
	client      Client
	ownsClient  bool
	redisConfig *RedisConfig
	streamName  string
	maxLen      int64
	logger      Logger
}

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	Name   string // Optional: Default stream name for Push.
	MaxLen int64  // Optional: Maximum stream length, default is 1000
}

func (pc *ProducerConfig) validate() error {
	if pc.MaxLen <= 0 {
		pc.MaxLen = DefaultProducerMaxLen
	}

	return nil
}

type ProducerOption interface {
	applyProducer(*Producer)
}

type producerOptionFunc func(*Producer)

func (f producerOptionFunc) applyProducer(p *Producer) {
	f(p)
}

type clientOption struct {
	client Client
}

// WithClient configures a producer or consumer to use an existing client.
func WithClient(client Client) clientOption {
	return clientOption{client: client}
}

func (o clientOption) applyProducer(p *Producer) {
	p.client = o.client
	p.ownsClient = false
}

type newRedisClientOption struct {
	config RedisConfig
}

// WithNewRedisClient configures a producer or consumer to create and own a new Redis client.
func WithNewRedisClient(config RedisConfig) newRedisClientOption {
	return newRedisClientOption{config: config}
}

func (o newRedisClientOption) applyProducer(p *Producer) {
	p.redisConfig = &o.config
}

type loggerOption struct {
	logger Logger
}

// WithLogger configures a producer or consumer to use the provided logger.
func WithLogger(logger Logger) loggerOption {
	return loggerOption{logger: logger}
}

func (o loggerOption) applyProducer(p *Producer) {
	p.logger = o.logger
}

// NewProducer creates a producer using the provided configuration and options.
func NewProducer(config ProducerConfig, opts ...ProducerOption) (*Producer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	producer := &Producer{
		streamName: config.Name,
		maxLen:     config.MaxLen,
	}

	for _, opt := range opts {
		opt.applyProducer(producer)
	}

	if producer.client == nil {
		redisConfig := RedisConfig{}
		if producer.redisConfig != nil {
			redisConfig = *producer.redisConfig
		}

		client, err := NewRedisClientWithContext(context.Background(), redisConfig)
		if err != nil {
			return nil, err
		}
		producer.client = client
		producer.ownsClient = true
	}
	if producer.logger == nil {
		producer.logger = newDefaultLogger()
	}

	return producer, nil
}

// Close closes the producer's owned client, if it created one.
func (p *Producer) Close() {
	if p.client != nil && p.ownsClient {
		p.client.Close()
	}
}

// Push writes a message to the producer's configured default stream.
func (p *Producer) Push(ctx context.Context, message map[string]interface{}) error {
	return p.PushTo(ctx, p.streamName, message)
}

// PushTo writes a message to the named stream and adds an automatic timestamp field.
func (p *Producer) PushTo(ctx context.Context, stream string, message map[string]interface{}) error {
	if stream == "" {
		return ErrEmptyStreamName
	}

	event := make(map[string]interface{}, len(message)+1)
	for key, value := range message {
		event[key] = value
	}
	event["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)

	p.logger.Debugf("Pushing message to stream '%s': %v", stream, event)
	return p.client.Push(ctx, stream, p.maxLen, event)
}
