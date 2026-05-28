package stream

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestNewRedisClientWithContextIntegration(t *testing.T) {
	if os.Getenv("STREAM_GO_INTEGRATION") != "1" {
		t.Skip("set STREAM_GO_INTEGRATION=1 to run Redis integration tests")
	}

	tests := []struct {
		name           string
		addr, password string
		wantErr        bool
	}{
		{"valid connection", "localhost:6379", "", false},
		{"invalid connection", "localhost:1234", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRedisClientWithContext(
				context.Background(),
				RedisConfig{
					Addr:     tt.addr,
					Password: tt.password,
				})
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewRedisClientWithContext() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRedisClientWithContext() error = %v", err)
			}
			defer got.Close()
			if got == nil {
				t.Fatal("NewRedisClientWithContext() returned nil client")
			}
		})
	}
}

func TestRedisClientRegisterConsumerBusyGroupIntegration(t *testing.T) {
	if os.Getenv("STREAM_GO_INTEGRATION") != "1" {
		t.Skip("set STREAM_GO_INTEGRATION=1 to run Redis integration tests")
	}

	ctx := context.Background()
	client := NewRedisClient(RedisConfig{Addr: "localhost:6379"})
	defer client.Close()

	stream := "stream_go_register_consumer_test"
	group := "stream_go_group"
	consumer := "consumer-1"
	defer client.client.Del(ctx, stream)

	if err := client.RegisterConsumer(ctx, stream, group, consumer); err != nil {
		t.Fatalf("RegisterConsumer() first call error = %v", err)
	}
	if err := client.RegisterConsumer(ctx, stream, group, consumer); err != nil {
		t.Fatalf("RegisterConsumer() second call error = %v", err)
	}
}

func TestNormalizeStreamValuesEncodesComplexFields(t *testing.T) {
	values, err := normalizeStreamValues(map[string]interface{}{
		"id":      "some-unique-id",
		"type":    "ticket.created",
		"version": 1,
		"metadata": map[string]interface{}{
			"source": "jira",
		},
		"data": map[string]interface{}{
			"recipient":  "user@example.com",
			"ticket_key": "ABC-123",
		},
		"tags": []string{"support", "urgent"},
		"nil":  nil,
	})
	if err != nil {
		t.Fatalf("normalizeStreamValues() error = %v", err)
	}

	if values["id"] != "some-unique-id" {
		t.Fatalf("id = %v, want scalar value preserved", values["id"])
	}
	if values["version"] != 1 {
		t.Fatalf("version = %v, want scalar value preserved", values["version"])
	}
	if values["metadata"] != `{"source":"jira"}` {
		t.Fatalf("metadata = %v, want JSON object", values["metadata"])
	}
	if values["data"] != `{"recipient":"user@example.com","ticket_key":"ABC-123"}` {
		t.Fatalf("data = %v, want JSON object", values["data"])
	}
	if values["tags"] != `["support","urgent"]` {
		t.Fatalf("tags = %v, want JSON array", values["tags"])
	}
	if values["nil"] != "null" {
		t.Fatalf("nil = %v, want JSON null string", values["nil"])
	}
}

func TestNormalizeStreamValuesReturnsJSONError(t *testing.T) {
	_, err := normalizeStreamValues(map[string]interface{}{
		"bad": make(chan struct{}),
	})
	if err == nil {
		t.Fatal("normalizeStreamValues() error = nil, want error")
	}

	var marshalTypeError *json.UnsupportedTypeError
	if !errors.As(err, &marshalTypeError) {
		t.Fatalf("normalizeStreamValues() error = %v, want json.UnsupportedTypeError", err)
	}
}

func TestRedisReadGroupStreams(t *testing.T) {
	got := redisReadGroupStreams([]string{"stream-a", "stream-b"})
	want := []string{"stream-a", "stream-b", ">", ">"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redisReadGroupStreams() = %v, want %v", got, want)
	}
}

func TestAckLogValueEncodesMetadataAsJSON(t *testing.T) {
	timestamp := time.Date(2026, 5, 11, 10, 8, 30, 123, time.UTC)

	got, err := ackLogValue("consumer-1", timestamp)
	if err != nil {
		t.Fatalf("ackLogValue() error = %v", err)
	}

	var metadata map[string]string
	if err := json.Unmarshal([]byte(got), &metadata); err != nil {
		t.Fatalf("ackLogValue() returned invalid JSON %q: %v", got, err)
	}
	if metadata["consumer"] != "consumer-1" {
		t.Fatalf("consumer = %q, want %q", metadata["consumer"], "consumer-1")
	}
	if metadata["timestamp"] != timestamp.Format(time.RFC3339Nano) {
		t.Fatalf("timestamp = %q, want %q", metadata["timestamp"], timestamp.Format(time.RFC3339Nano))
	}
}

func TestStreamMessageType(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]interface{}
		want   string
	}{
		{"string type", map[string]interface{}{"type": "ticket.created"}, "ticket.created"},
		{"numeric type", map[string]interface{}{"type": 123}, "123"},
		{"missing type", map[string]interface{}{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := streamMessageType(tt.values); got != tt.want {
				t.Fatalf("streamMessageType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStreamMessageTimestamp(t *testing.T) {
	want := time.Date(2026, 5, 8, 12, 34, 56, 789, time.UTC)

	tests := []struct {
		name   string
		values map[string]interface{}
		want   time.Time
	}{
		{"valid timestamp", map[string]interface{}{"timestamp": want.Format(time.RFC3339Nano)}, want},
		{"missing timestamp", map[string]interface{}{}, time.Time{}},
		{"invalid timestamp", map[string]interface{}{"timestamp": "not-a-time"}, time.Time{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := streamMessageTimestamp(tt.values); !got.Equal(tt.want) {
				t.Fatalf("streamMessageTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamMessageStringField(t *testing.T) {
	values := map[string]interface{}{
		"type":   "ticket.created",
		"source": "jira",
		"count":  3,
	}

	if got := streamMessageStringField(values, "source"); got != "jira" {
		t.Fatalf("streamMessageStringField(source) = %q, want %q", got, "jira")
	}
	if got := streamMessageStringField(values, "count"); got != "3" {
		t.Fatalf("streamMessageStringField(count) = %q, want %q", got, "3")
	}
	if got := streamMessageStringField(values, "missing"); got != "" {
		t.Fatalf("streamMessageStringField(missing) = %q, want empty string", got)
	}
}
