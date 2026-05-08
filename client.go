package stream

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client interface {
	Close()
	Push(ctx context.Context, stream string, maxLen int64, message map[string]interface{}) error
	RegisterConsumer(ctx context.Context, stream, group, name string) error
	ReadStreams(ctx context.Context, streams []string, group, consumerName string, count int64, retryIn time.Duration) ([]Message, error)
	Ack(ctx context.Context, stream, group string, ids ...string) error
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(conn RedisConfig) *RedisClient {
	client, err := NewRedisClientWithContext(context.Background(), conn)
	if err != nil {
		panic(err)
	}

	return client
}

func NewRedisClientWithContext(ctx context.Context, conn RedisConfig) (*RedisClient, error) {
	options := &redis.Options{
		Addr:     conn.Addr,
		Password: conn.Password,
		DB:       conn.DB,
	}
	rdb := redis.NewClient(options)

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}

	return &RedisClient{client: rdb}, nil
}

func (r *RedisClient) Close() {
	_ = r.client.Close()
}

func (r *RedisClient) Push(ctx context.Context, stream string, maxLen int64, message map[string]interface{}) error {
	values, err := normalizeStreamValues(message)
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}
	if maxLen > 0 {
		args.MaxLen = maxLen
		args.Approx = true
	}

	return r.client.XAdd(ctx, args).Err()
}

func normalizeStreamValues(message map[string]interface{}) (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(message))
	for key, value := range message {
		normalized, err := normalizeStreamValue(value)
		if err != nil {
			return nil, fmt.Errorf("stream field %q: %w", key, err)
		}
		values[key] = normalized
	}

	return values, nil
}

func normalizeStreamValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case nil:
		return "null", nil
	case string, []byte, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return v, nil
	case encoding.BinaryMarshaler:
		return v, nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}
}

func (r *RedisClient) RegisterConsumer(ctx context.Context, stream, group, name string) error {
	err := r.client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !redis.HasErrorPrefix(err, "BUSYGROUP") {
		return err
	}

	if err := r.client.XGroupCreateConsumer(ctx, stream, group, name).Err(); err != nil {
		return err
	}

	return nil
}

func (r *RedisClient) ReadStreams(ctx context.Context, streams []string, group, consumerName string, count int64, retryIn time.Duration) ([]Message, error) {
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerName,
		Streams:  redisReadGroupStreams(streams),
		Count:    count,
		Claim:    retryIn,
	}
	res, err := r.client.XReadGroup(ctx, args).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var messages []Message
	for _, stream := range res {
		for _, msg := range stream.Messages {
			streamName := stream.Stream
			messages = append(messages, Message{
				Stream:    streamName,
				ID:        msg.ID,
				Type:      streamMessageType(msg.Values),
				Source:    streamMessageStringField(msg.Values, "source"),
				Timestamp: streamMessageTimestamp(msg.Values),
				Values:    msg.Values,
				ackFunc: func(ctx context.Context, id string) error {
					return r.Ack(ctx, streamName, group, id)
				},
			})
		}
	}

	return messages, nil
}

func streamMessageType(values map[string]interface{}) string {
	return streamMessageStringField(values, "type")
}

func streamMessageTimestamp(values map[string]interface{}) time.Time {
	value := streamMessageStringField(values, "timestamp")
	if value == "" {
		return time.Time{}
	}

	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}

	return timestamp
}

func streamMessageStringField(values map[string]interface{}, field string) string {
	value, ok := values[field]
	if !ok {
		return ""
	}

	if typ, ok := value.(string); ok {
		return typ
	}

	return fmt.Sprint(value)
}

func redisReadGroupStreams(streams []string) []string {
	args := make([]string, 0, len(streams)*2)
	args = append(args, streams...)
	for range streams {
		args = append(args, ">")
	}

	return args
}

func (r *RedisClient) Ack(ctx context.Context, stream, group string, ids ...string) error {
	return r.client.XAck(ctx, stream, group, ids...).Err()
}
