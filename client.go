package stream

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client interface {
	Close()
	Push(ctx context.Context, stream string, maxLen int64, message map[string]interface{}) error
	RegisterConsumer(ctx context.Context, stream, group, name string) error
	Read(ctx context.Context, streamName, group, consunerName string, count int64, retryIn time.Duration) ([]Message, error)
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
	options := &redis.Options{
		Addr:     conn.Addr,
		Password: conn.Password,
		DB:       conn.DB,
	}
	rdb := redis.NewClient(options)

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	return &RedisClient{client: rdb}
}

func (r *RedisClient) Close() {
	r.client.Close()
}

func (r *RedisClient) Push(ctx context.Context, stream string, maxLen int64, message map[string]interface{}) error {
	args := &redis.XAddArgs{
		Stream: stream,
		Values: message,
	}
	if maxLen > 0 {
		args.MaxLen = maxLen
		args.Approx = true
	}

	return r.client.XAdd(ctx, args).Err()
}

func (r *RedisClient) RegisterConsumer(ctx context.Context, stream, group, name string) error {
	err := r.client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	if err := r.client.XGroupCreateConsumer(ctx, stream, group, name).Err(); err != nil {
		return err
	}

	return nil
}

func (r *RedisClient) Read(ctx context.Context, streamName, group, consunerName string, count int64, retryIn time.Duration) ([]Message, error) {
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consunerName,
		Streams:  []string{streamName, ">"},
		Count:    count,
		Claim:    retryIn,
	}
	res, err := r.client.XReadGroup(ctx, args).Result()
	if err != nil {
		return nil, err
	}

	var messages []Message
	for _, stream := range res {
		for _, msg := range stream.Messages {
			messages = append(messages, Message{
				ID:     msg.ID,
				Values: msg.Values,
				ackFunc: func(ctx context.Context, id string) {
					r.client.XAck(ctx, streamName, group, id)
				},
			})
		}
	}

	return messages, nil
}

func (r *RedisClient) Ack(ctx context.Context, stream, group string, ids ...string) error {
	return r.client.XAck(ctx, stream, group, ids...).Err()
}
