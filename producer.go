package stream

import (
	"context"
	"errors"
)

var (
	ErrEmptyStreamName = errors.New("stream name cannot be empty")
)

type Producer struct {
	client     Client
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
		pc.MaxLen = 1000
	}

	return nil
}

// func NewProducer(conn Client, config ProducerConfig) (*Producer, error) {
func NewProducer(config ProducerConfig) (*Producer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	client := NewRedisClient(config.RedisConfig)

	producer := &Producer{
		client:     client,
		streamName: config.Name,
		maxLen:     config.MaxLen,
		logger:     newDefaultLogger(),
	}

	return producer, nil
}

func (p *Producer) Close() {
	p.client.Close()
}

func (p *Producer) Push(ctx context.Context, message map[string]interface{}) error {
	p.logger.Debugf("Pushing message to stream '%s': %v", p.streamName, message)
	return p.client.Push(ctx, p.streamName, p.maxLen, message)
}
