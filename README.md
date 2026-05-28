# Stream-go

Go library for producing and consuming Redis Streams events.

It provides a small wrapper around Redis Streams with producer and consumer helpers, consumer-group registration, acknowledgements, automatic event metadata, and JSON decoding helpers for structured payloads.

## Requirements

- Go 1.21 or newer
- Redis 8.4.x or newer

## Installation

```bash
go get github.com/supakarn-j/stream-go@latest
```

## Quick Start

```go
package main

import (
	"context"
	"log"

	stream "github.com/supakarn-j/stream-go"
)

func main() {
	ctx := context.Background()

	producer, err := stream.NewProducer(
		stream.ProducerConfig{Name: "mystream"},
		stream.WithNewRedisClient(stream.RedisConfig{
			Addr: "localhost:6379",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	if err := producer.Push(ctx, map[string]interface{}{
		"type": "ticket.created",
		"data": map[string]interface{}{
			"ticket_key": "ABC-123",
		},
	}); err != nil {
		log.Fatal(err)
	}
}
```

`Producer.Push` writes to the configured default stream. Use `Producer.PushTo` when one producer needs to write to different streams.

## Producer

```go
producer, err := stream.NewProducer(
	stream.ProducerConfig{
		Name:   "mystream",
		MaxLen: 1000,
	},
	stream.WithNewRedisClient(stream.RedisConfig{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}),
)
if err != nil {
	panic(err)
}
defer producer.Close()

err = producer.Push(ctx, map[string]interface{}{
	"type": "contact.form.submitted",
	"data": map[string]interface{}{
		"name":  "Ada",
		"email": "ada@example.com",
	},
})
if err != nil {
	panic(err)
}
```

Producer behavior:

- `Name` is the default stream for `Push`.
- `MaxLen` controls the approximate Redis stream length. The default is `1000`.
- `PushTo(ctx, streamName, message)` writes to an explicit stream.
- `timestamp` is added automatically in UTC RFC3339Nano format.
- `source` is added automatically from the host name when the message does not provide one.
- Maps, slices, structs, and other complex field values are JSON-encoded before they are written to Redis.

```go
if err := producer.PushTo(ctx, "ticket-events", map[string]interface{}{
	"type": "ticket.created",
}); err != nil {
	panic(err)
}
```

## Consumer

```go
consumer, err := stream.NewConsumer(
	stream.ConsumerConfig{
		Streams: []string{"mystream"},
		Group:   "ticket-workers",
		Name:    "consumer-1",
		RetryIn: time.Minute,
	},
	stream.WithNewRedisClient(stream.RedisConfig{
		Addr: "localhost:6379",
	}),
)
if err != nil {
	panic(err)
}
defer consumer.Close()

for event := range consumer.Start(ctx, 10) {
	log.Printf("received %s from %s: %v", event.ID, event.Stream, event.Values)

	if err := event.Ack(ctx); err != nil {
		log.Printf("failed to ack event %s: %v", event.ID, err)
	}
}
```

Consumer behavior:

- `Streams` may contain one or more stream names.
- `Group` is the Redis consumer group.
- `Name` is the Redis consumer name.
- `RetryIn` controls when pending messages can be claimed for retry. The default is `1 minute`.
- `NewConsumer` creates the consumer group and consumer for each configured stream.
- `Message.Ack` acknowledges the message against the stream that produced it.

## Multiple Streams

```go
consumer, err := stream.NewConsumer(
	stream.ConsumerConfig{
		Streams: []string{"tickets", "comments"},
		Group:   "event-workers",
		Name:    "consumer-1",
	},
	stream.WithNewRedisClient(stream.RedisConfig{
		Addr: "localhost:6379",
	}),
)
if err != nil {
	panic(err)
}
defer consumer.Close()

for event := range consumer.Start(ctx, 10) {
	log.Printf("received %s from %s", event.ID, event.Stream)
	_ = event.Ack(ctx)
}
```

`Message.Stream` identifies the Redis stream that produced the event.

## Message Decoding

Use `Message.Type` for routing, `DecodeValue` for one field, and `Decode` for the full message.

```go
type TicketCreated struct {
	TicketKey string `json:"ticket_key"`
	URL       string `json:"url"`
	Recipient string `json:"recipient"`
}

for event := range consumer.Start(ctx, 10) {
	switch event.Type {
	case "ticket.created":
		var data TicketCreated
		if err := event.DecodeValue("data", &data); err != nil {
			log.Printf("failed to decode payload %s: %v", event.ID, err)
			continue
		}

		log.Printf("ticket created: %s", data.TicketKey)
	}
}
```

Useful message fields:

- `Message.ID`: Redis stream entry ID.
- `Message.Stream`: Redis stream name.
- `Message.Type`: value from the `type` field.
- `Message.Source`: value from the `source` field.
- `Message.Timestamp`: parsed value from the `timestamp` field.
- `Message.Values`: raw Redis stream values.

## Consumer Errors

`Start` logs read errors internally. Use `StartWithErrors` when callers need explicit error handling.

```go
events, errs := consumer.StartWithErrors(ctx, 10)

for {
	select {
	case event, ok := <-events:
		if !ok {
			return
		}

		if err := event.Ack(ctx); err != nil {
			log.Printf("failed to ack event %s: %v", event.ID, err)
		}
	case err, ok := <-errs:
		if ok {
			log.Printf("consumer error: %v", err)
		}
		return
	}
}
```

## Redis Clients

Use `WithNewRedisClient` when the producer or consumer should create and own its Redis client. Use `WithClient` to share an existing client or inject a test double.

```go
client, err := stream.NewRedisClientWithContext(ctx, stream.RedisConfig{
	Addr: "localhost:6379",
})
if err != nil {
	panic(err)
}
defer client.Close()

producer, err := stream.NewProducer(
	stream.ProducerConfig{Name: "mystream"},
	stream.WithClient(client),
)
if err != nil {
	panic(err)
}

consumer, err := stream.NewConsumer(
	stream.ConsumerConfig{
		Streams: []string{"mystream"},
		Group:   "event-workers",
		Name:    "consumer-1",
	},
	stream.WithClient(client),
)
if err != nil {
	panic(err)
}
```

## CLI Examples

The `example` directory contains small producer and consumer CLIs.

Run a producer:

```bash
go run example/producer/main.go \
  -H localhost \
  -P 6379 \
  -s event:contact \
  -v id=2222 \
  -v type=contact.form.submitted \
  -v version=1 \
  -v data='{"name":"Ada","email":"ada@example.com"}' \
  -v metadata='{}'
```

Run a consumer:

```bash
go run example/consumer/main.go \
  -H localhost \
  -P 6379 \
  -s event:contact \
  -g contact-workers \
  -n consumer-1
```

## Testing

Unit tests do not require Redis:

```bash
go test ./...
```

Integration tests require Redis. Start the included Docker Compose service, then enable the integration test flag:

```bash
docker compose -f redis/docker-compose.yml up -d
STREAM_GO_INTEGRATION=1 go test ./...
```
