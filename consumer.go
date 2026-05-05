package stream

import (
	"context"
	"errors"
	"time"
)

var (
	ErrEmptyConsumerName = errors.New("consumer name cannot be empty")
	ErrEmptyGroupName    = errors.New("group name cannot be empty")
)

type Consumer struct {
	client  Client
	stream  string
	group   string
	name    string
	retryIn time.Duration
	logger  Logger
}

type ConsumerConfig struct {
	RedisConfig
	Stream  string        // Required: Stream name
	Group   string        // Required: Consumer group name/
	Name    string        // Required: Consumer name
	RetryIn time.Duration // Optional: Time to wait before retrying failed messages, default is 1 minute
}

type Message struct {
	ID      string
	Values  map[string]interface{}
	ackFunc func(ctx context.Context, id string)
}

func (m *Message) Ack(ctx context.Context) {
	m.ackFunc(ctx, m.ID)
}

func (cc *ConsumerConfig) validate() error {
	if cc.Stream == "" {
		return ErrEmptyStreamName
	}

	if cc.Group == "" {
		return ErrEmptyGroupName
	}

	if cc.Name == "" {
		return ErrEmptyConsumerName
	}

	if cc.RetryIn <= 0 {
		cc.RetryIn = 1 * time.Minute
	}

	return nil
}

func NewConsumer(conf ConsumerConfig) (*Consumer, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}

	client := NewRedisClient(conf.RedisConfig)
	return &Consumer{
		client:  client,
		stream:  conf.Stream,
		group:   conf.Group,
		name:    conf.Name,
		retryIn: conf.RetryIn,
	}, nil
}

func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) Start(ctx context.Context, msgCount int64) <-chan Message {
	out := make(chan Message, msgCount)

	go func() {
		defer close(out)
		defer func() {
			if r := recover(); r != nil {
				c.logger.Errorf("Consumer '%s' panicked: %v", c.name, r)
			}
		}()

		c.logger.Infof("Consumer '%s' started for stream '%s' in group '%s'", c.name, c.stream, c.group)
		for {
			messages, err := c.client.Read(ctx, c.stream, c.group, c.name, msgCount, c.retryIn)
			if err != nil {
				panic(err)
			}

			for _, msg := range messages {
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}
