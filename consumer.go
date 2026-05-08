package stream

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrEmptyConsumerName is returned when a consumer name is required but empty.
	ErrEmptyConsumerName = errors.New("consumer name cannot be empty")
	// ErrEmptyGroupName is returned when a consumer group name is required but empty.
	ErrEmptyGroupName = errors.New("group name cannot be empty")
)

// DefaultConsumerRetryIn is the default delay before retrying pending messages.
const DefaultConsumerRetryIn = time.Minute

// Consumer reads messages from one or more Redis Streams in a consumer group.
type Consumer struct {
	client      Client
	ownsClient  bool
	redisConfig *RedisConfig
	streams     []string
	group       string
	name        string
	retryIn     time.Duration
	logger      Logger
}

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	Streams []string      // Required: Stream names.
	Group   string        // Required: Consumer group name/
	Name    string        // Required: Consumer name
	RetryIn time.Duration // Optional: Time to wait before retrying failed messages, default is 1 minute
}

func (cc *ConsumerConfig) validate() error {
	if len(cc.streams()) == 0 {
		return ErrEmptyStreamName
	}

	if cc.Group == "" {
		return ErrEmptyGroupName
	}

	if cc.Name == "" {
		return ErrEmptyConsumerName
	}

	return nil
}

func (cc *ConsumerConfig) streams() []string {
	return compactStrings(cc.Streams)
}

func compactStrings(values []string) []string {
	compacted := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			compacted = append(compacted, value)
		}
	}

	return compacted
}

type ConsumerOption interface {
	applyConsumer(*Consumer)
}

type consumerOptionFunc func(*Consumer)

func (f consumerOptionFunc) applyConsumer(c *Consumer) {
	f(c)
}

func (o clientOption) applyConsumer(c *Consumer) {
	c.client = o.client
	c.ownsClient = false
}

func (o newRedisClientOption) applyConsumer(c *Consumer) {
	c.redisConfig = &o.config
}

func (o loggerOption) applyConsumer(c *Consumer) {
	c.logger = o.logger
}

// WithRetryDuration configures how long the consumer waits before retrying failed messages.
func WithRetryDuration(d time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.retryIn = d
	})
}

// NewConsumer creates a consumer and registers it for each configured stream.
func NewConsumer(conf ConsumerConfig, opts ...ConsumerOption) (*Consumer, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}

	retryIn := conf.RetryIn
	if retryIn <= 0 {
		retryIn = DefaultConsumerRetryIn
	}

	c := &Consumer{
		streams: conf.streams(),
		group:   conf.Group,
		name:    conf.Name,
		retryIn: retryIn,
	}

	for _, opt := range opts {
		opt.applyConsumer(c)
	}

	if c.client == nil {
		redisConfig := RedisConfig{}
		if c.redisConfig != nil {
			redisConfig = *c.redisConfig
		}

		client, err := NewRedisClientWithContext(context.Background(), redisConfig)
		if err != nil {
			return nil, err
		}
		c.client = client
		c.ownsClient = true
	}

	for _, stream := range c.streams {
		if err := c.client.RegisterConsumer(context.Background(), stream, c.group, c.name); err != nil {
			if c.ownsClient {
				c.client.Close()
			}
			return nil, err
		}
	}
	if c.logger == nil {
		c.logger = newDefaultLogger()
	}

	return c, nil
}

// Close closes the consumer's owned client, if it created one.
func (c *Consumer) Close() {
	if c.client != nil && c.ownsClient {
		c.client.Close()
	}
}

// Start begins reading messages and logs read errors internally.
func (c *Consumer) Start(ctx context.Context, msgCount int64) <-chan Message {
	out, errs := c.StartWithErrors(ctx, msgCount)

	go func() {
		for err := range errs {
			c.logger.Errorf("Consumer '%s' read failed: %v", c.name, err)
		}
	}()

	return out
}

// StartWithErrors begins reading messages and returns separate channels for messages and read errors.
func (c *Consumer) StartWithErrors(ctx context.Context, msgCount int64) (<-chan Message, <-chan error) {
	out := make(chan Message, msgCount)
	errs := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errs)

		c.logger.Infof("Consumer '%s' started for streams '%v' in group '%s'", c.name, c.streams, c.group)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			messages, err := c.client.ReadStreams(ctx, c.streams, c.group, c.name, msgCount, c.retryIn)
			if err != nil {
				select {
				case errs <- err:
				case <-ctx.Done():
				}
				return
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

	return out, errs
}
