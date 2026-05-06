package stream

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNewConsumerValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  ConsumerConfig
		wantErr error
	}{
		{"empty stream", ConsumerConfig{Group: "group", Name: "consumer"}, ErrEmptyStreamName},
		{"empty group", ConsumerConfig{Stream: "stream", Name: "consumer"}, ErrEmptyGroupName},
		{"empty consumer", ConsumerConfig{Stream: "stream", Group: "group"}, ErrEmptyConsumerName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsumer(tt.config, WithClient(&fakeClient{}))
			if err != tt.wantErr {
				t.Fatalf("NewConsumer() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewConsumerRegistersAndHonorsRetryIn(t *testing.T) {
	client := &fakeClient{}
	retryIn := 5 * time.Second

	consumer, err := NewConsumer(ConsumerConfig{
		Stream:  "stream",
		Group:   "group",
		Name:    "consumer",
		RetryIn: retryIn,
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	if consumer.retryIn != retryIn {
		t.Fatalf("retryIn = %v, want %v", consumer.retryIn, retryIn)
	}
	if client.registerStream != "stream" || client.registerGroup != "group" || client.registerName != "consumer" {
		t.Fatalf("RegisterConsumer() got stream=%q group=%q name=%q", client.registerStream, client.registerGroup, client.registerName)
	}
}

func TestNewConsumerRegistersMultipleStreams(t *testing.T) {
	client := &fakeClient{}

	consumer, err := NewConsumer(ConsumerConfig{
		Streams: []string{"stream-a", "stream-b"},
		Group:   "group",
		Name:    "consumer",
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	if !reflect.DeepEqual(consumer.streams, []string{"stream-a", "stream-b"}) {
		t.Fatalf("streams = %v, want [stream-a stream-b]", consumer.streams)
	}
	if !reflect.DeepEqual(client.registerStreams, []string{"stream-a", "stream-b"}) {
		t.Fatalf("registered streams = %v, want [stream-a stream-b]", client.registerStreams)
	}
}

func TestNewConsumerRejectsEmptyStreams(t *testing.T) {
	_, err := NewConsumer(ConsumerConfig{
		Streams: []string{"", ""},
		Group:   "group",
		Name:    "consumer",
	}, WithClient(&fakeClient{}))
	if err != ErrEmptyStreamName {
		t.Fatalf("NewConsumer() error = %v, want %v", err, ErrEmptyStreamName)
	}
}

func TestNewConsumerDefaultsRetryIn(t *testing.T) {
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "stream",
		Group:  "group",
		Name:   "consumer",
	}, WithClient(&fakeClient{}))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	if consumer.retryIn != DefaultConsumerRetryIn {
		t.Fatalf("retryIn = %v, want %v", consumer.retryIn, DefaultConsumerRetryIn)
	}
}

func TestNewConsumerReturnsRegisterError(t *testing.T) {
	wantErr := errors.New("register failed")
	client := &fakeClient{registerErr: wantErr}

	_, err := NewConsumer(ConsumerConfig{
		Stream: "stream",
		Group:  "group",
		Name:   "consumer",
	}, WithClient(client))
	if !errors.Is(err, wantErr) {
		t.Fatalf("NewConsumer() error = %v, want %v", err, wantErr)
	}
	if client.closed {
		t.Fatal("NewConsumer() closed an injected client after register error")
	}
}

func TestConsumerCloseDoesNotCloseInjectedClient(t *testing.T) {
	client := &fakeClient{}
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "stream",
		Group:  "group",
		Name:   "consumer",
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	consumer.Close()
	if client.closed {
		t.Fatal("Consumer.Close() closed an injected client")
	}
}

func TestConsumerUsesSharedLoggerOption(t *testing.T) {
	logger := &fakeLogger{}
	consumer, err := NewConsumer(
		ConsumerConfig{
			Stream: "stream",
			Group:  "group",
			Name:   "consumer",
		},
		WithClient(&fakeClient{}),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	messages, errs := consumer.StartWithErrors(ctx, 1)
	for range messages {
	}
	for range errs {
	}

	if !logger.infofCalled {
		t.Fatal("WithLogger() logger was not used by consumer")
	}
}

func TestConsumerUsesDefaultLoggerWhenLoggerIsNil(t *testing.T) {
	consumer, err := NewConsumer(
		ConsumerConfig{
			Stream: "stream",
			Group:  "group",
			Name:   "consumer",
		},
		WithClient(&fakeClient{}),
		WithLogger(nil),
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	if consumer.logger == nil {
		t.Fatal("consumer logger is nil, want default logger")
	}
}

func TestConsumerStartWithErrorsReturnsReadError(t *testing.T) {
	wantErr := errors.New("read failed")
	client := &fakeClient{readErr: wantErr}
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "stream",
		Group:  "group",
		Name:   "consumer",
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	messages, errs := consumer.StartWithErrors(ctx, 10)

	select {
	case err := <-errs:
		if !errors.Is(err, wantErr) {
			t.Fatalf("StartWithErrors() error = %v, want %v", err, wantErr)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for StartWithErrors error")
	}

	if _, ok := <-messages; ok {
		t.Fatal("messages channel remained open after read error")
	}
	if client.readRetryIn != DefaultConsumerRetryIn {
		t.Fatalf("Read retryIn = %v, want %v", client.readRetryIn, DefaultConsumerRetryIn)
	}
}

func TestConsumerStartWithErrorsReadsMultipleStreams(t *testing.T) {
	client := &fakeClient{readErr: errors.New("stop")}
	consumer, err := NewConsumer(ConsumerConfig{
		Streams: []string{"stream-a", "stream-b"},
		Group:   "group",
		Name:    "consumer",
	}, WithClient(client))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, errs := consumer.StartWithErrors(ctx, 10)
	<-errs

	if !reflect.DeepEqual(client.readStreams, []string{"stream-a", "stream-b"}) {
		t.Fatalf("ReadStreams streams = %v, want [stream-a stream-b]", client.readStreams)
	}
}

func TestMessageAckReturnsError(t *testing.T) {
	wantErr := errors.New("ack failed")
	message := Message{
		ID: "1-0",
		ackFunc: func(ctx context.Context, id string) error {
			if id != "1-0" {
				t.Fatalf("Ack id = %q, want %q", id, "1-0")
			}
			return wantErr
		},
	}

	if err := message.Ack(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Message.Ack() error = %v, want %v", err, wantErr)
	}
}

func TestMessageAckUnavailable(t *testing.T) {
	message := Message{ID: "1-0"}
	if err := message.Ack(context.Background()); !errors.Is(err, ErrAckUnavailable) {
		t.Fatalf("Message.Ack() error = %v, want %v", err, ErrAckUnavailable)
	}
}
