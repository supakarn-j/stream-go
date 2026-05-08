package stream

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewProducerAllowsEmptyDefaultStream(t *testing.T) {
	_, err := NewProducer(ProducerConfig{}, WithClient(&fakeClient{}))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
}

func TestProducerPushUsesDefaultMaxLen(t *testing.T) {
	client := &fakeClient{}
	producer, err := NewProducer(ProducerConfig{Name: "test_stream"}, WithClient(client))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	message := map[string]interface{}{"field1": "value1"}
	if err := producer.Push(context.Background(), message); err != nil {
		t.Fatalf("Producer.Push() error = %v", err)
	}

	if client.pushStream != "test_stream" {
		t.Fatalf("Push stream = %q, want %q", client.pushStream, "test_stream")
	}
	if client.pushMaxLen != DefaultProducerMaxLen {
		t.Fatalf("Push maxLen = %d, want %d", client.pushMaxLen, DefaultProducerMaxLen)
	}
	if client.pushMessage["field1"] != "value1" {
		t.Fatalf("Push message = %v, want field1=value1", client.pushMessage)
	}
}

func TestProducerPushAddsTimestamp(t *testing.T) {
	client := &fakeClient{}
	producer, err := NewProducer(ProducerConfig{Name: "test_stream"}, WithClient(client))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	message := map[string]interface{}{"field1": "value1"}
	before := time.Now().UTC()
	if err := producer.Push(context.Background(), message); err != nil {
		t.Fatalf("Producer.Push() error = %v", err)
	}
	after := time.Now().UTC()

	value, ok := client.pushMessage["timestamp"].(string)
	if !ok {
		t.Fatalf("timestamp = %v, want RFC3339Nano string", client.pushMessage["timestamp"])
	}
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("timestamp parse error = %v", err)
	}
	if timestamp.Before(before) || timestamp.After(after) {
		t.Fatalf("timestamp = %v, want between %v and %v", timestamp, before, after)
	}
	if _, ok := message["timestamp"]; ok {
		t.Fatal("Producer.Push() mutated caller message with timestamp")
	}
}

func TestProducerPushToUsesExplicitStream(t *testing.T) {
	client := &fakeClient{}
	producer, err := NewProducer(ProducerConfig{}, WithClient(client))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	if err := producer.PushTo(context.Background(), "stream-a", map[string]interface{}{"field": "value"}); err != nil {
		t.Fatalf("Producer.PushTo() error = %v", err)
	}

	if client.pushStream != "stream-a" {
		t.Fatalf("Push stream = %q, want %q", client.pushStream, "stream-a")
	}
	if client.pushMessage["field"] != "value" {
		t.Fatalf("Push message = %v, want field=value", client.pushMessage)
	}
}

func TestProducerPushWithoutDefaultStreamReturnsError(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{}, WithClient(&fakeClient{}))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	if err := producer.Push(context.Background(), map[string]interface{}{"field": "value"}); err != ErrEmptyStreamName {
		t.Fatalf("Producer.Push() error = %v, want %v", err, ErrEmptyStreamName)
	}
}

func TestProducerPushToRejectsEmptyStream(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{}, WithClient(&fakeClient{}))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	if err := producer.PushTo(context.Background(), "", map[string]interface{}{"field": "value"}); err != ErrEmptyStreamName {
		t.Fatalf("Producer.PushTo() error = %v, want %v", err, ErrEmptyStreamName)
	}
}

func TestProducerCloseDoesNotCloseInjectedClient(t *testing.T) {
	client := &fakeClient{}
	producer, err := NewProducer(ProducerConfig{Name: "test_stream"}, WithClient(client))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	producer.Close()
	if client.closed {
		t.Fatal("Producer.Close() closed an injected client")
	}
}

func TestProducerUsesSharedLoggerOption(t *testing.T) {
	logger := &fakeLogger{}
	producer, err := NewProducer(
		ProducerConfig{Name: "test_stream"},
		WithClient(&fakeClient{}),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	if err := producer.Push(context.Background(), map[string]interface{}{"field": "value"}); err != nil {
		t.Fatalf("Producer.Push() error = %v", err)
	}
	if !logger.debugfCalled {
		t.Fatal("WithLogger() logger was not used by producer")
	}
}

func TestProducerUsesDefaultLoggerWhenLoggerIsNil(t *testing.T) {
	producer, err := NewProducer(
		ProducerConfig{Name: "test_stream"},
		WithClient(&fakeClient{}),
		WithLogger(nil),
	)
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if producer.logger == nil {
		t.Fatal("producer logger is nil, want default logger")
	}
}

func TestProducerPushIntegration(t *testing.T) {
	if os.Getenv("STREAM_GO_INTEGRATION") != "1" {
		t.Skip("set STREAM_GO_INTEGRATION=1 to run Redis integration tests")
	}

	conn := NewRedisClient(
		RedisConfig{
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
			producer, err := NewProducer(
				ProducerConfig{
					Name: tt.stream,
				},
				WithNewRedisClient(RedisConfig{
					Addr: "localhost:6379",
				}),
			)

			if err != nil {
				t.Errorf("NewProducer() error = %v", err)
				return
			}
			defer producer.Close()
			err = producer.Push(ctx, tt.message)
			if err != tt.wantErr {
				t.Errorf("Producer.Push() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
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
