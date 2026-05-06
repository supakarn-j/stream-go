package stream

import (
	"context"
	"errors"
)

var (
	ErrEmptyStreamName = errors.New("stream name cannot be empty")
)

const DefaultProducerMaxLen int64 = 1000

type Producer struct {
	client     Client
	ownsClient bool
	streamName string
	maxLen     int64
	logger     Logger
}

type ProducerConfig struct {
	RedisConfig
	Name   string // Required: Stream name
	MaxLen int64  // Optional: Maximum stream length, default is 1000
}

func (pc *ProducerConfig) validate() error {
	if pc.Name == "" {
		return ErrEmptyStreamName
	}

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

func WithClient(client Client) clientOption {
	return clientOption{client: client}
}

func (o clientOption) applyProducer(p *Producer) {
	p.client = o.client
	p.ownsClient = false
}

type loggerOption struct {
	logger Logger
}

func WithLogger(logger Logger) loggerOption {
	return loggerOption{logger: logger}
}

func (o loggerOption) applyProducer(p *Producer) {
	p.logger = o.logger
}

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
		client, err := NewRedisClientWithContext(context.Background(), config.RedisConfig)
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

func (p *Producer) Close() {
	if p.client != nil && p.ownsClient {
		p.client.Close()
	}
}

func (p *Producer) Push(ctx context.Context, message map[string]interface{}) error {
	p.logger.Debugf("Pushing message to stream '%s': %v", p.streamName, message)
	return p.client.Push(ctx, p.streamName, p.maxLen, message)
}
