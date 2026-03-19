package stream

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestProducerPush(t *testing.T) {
	conn := NewRedisClient(
		ConnectionConfig{
			Addr: "localhost:6379",
		},
	)
	defer conn.Close()
	ctx := context.Background()

	tests := []struct {
		name    string
		stream  string
		message map[string]interface{}
		wantErr error
	}{
		{"valid stream", "test_stream", map[string]interface{}{"field1": "value1"}, nil},
		{"empty stream name", "", map[string]interface{}{"field1": "value1"}, ErrEmptyStreamName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewProducer(conn, ProducerConfig{Name: tt.stream})
			if err != nil {
				if err != tt.wantErr {
					t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if tt.wantErr != nil {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			err = producer.Push(ctx, tt.message)
			if err != tt.wantErr {
				t.Errorf("Producer.Push() error = %v, wantErr %v", err, tt.wantErr)
			}
			st, err := conn.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{tt.stream, "0"},
				Count:   1,
				Block:   0,
			}).Result()
			if err != nil {
				t.Errorf("Failed to read from stream: %v", err)
				return
			}
			if len(st) == 0 || len(st[0].Messages) == 0 {
				t.Errorf("Expected message not found in stream")
				return
			}
			msg := st[0].Messages[0]
			for k, v := range tt.message {
				if msg.Values[k] != v {
					t.Errorf("Expected field %s to be %v, got %v", k, v, msg.Values[k])
				}
			}
			conn.client.Del(ctx, tt.stream)
		})
	}
}
