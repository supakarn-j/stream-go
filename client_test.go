package stream

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
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
