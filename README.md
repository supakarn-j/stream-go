# Overview

Go library for pushing/reading event to/from redis stream

## Required
```
Redis >= 8.4.x
```

## Installation
1. Set GOPRIVATE
```
go env -w GOPRIVATE=gitlab.com/hannlync/*
```
2. Create `.netrc` file in project folder with following content.
```
machine gitlab.com
login <your gitlab username>
password <your gitlab personal access token>
```
3. install package
```
go get gitlab.com/hannlync/backend/stream-go.git@v0.1.0
```

## Example
### Producer
```go
import (
    "gitlab.com/hannlync/backend/stream-go"
    "context"
)

func main() {
	producer, err := stream.NewProducer(
		stream.ProducerConfig{
			Name: "mystream", // Optional default stream for Push.
		},
		stream.WithNewRedisClient(stream.RedisConfig{
			Addr:     "localhost:6379",
			Password: "",
			DB: 0,
		}),
	)
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	
	ctx := context.Background()
	if err := producer.Push(
		ctx, 
		map[string]interface{}{
			"key1": "value1",
			...
		},
	); err != nil {
		panic(err)
	}
}

```

Top-level message fields can include nested maps, slices, and structs. Complex values are JSON-encoded before writing to Redis Streams, so consumers should unmarshal those fields when they need structured data.
`Producer.Push` automatically adds a UTC `timestamp` field in RFC3339Nano format.
Use `PushTo` when one producer should write to multiple streams:

```go
if err := producer.PushTo(ctx, "ticket-events", map[string]interface{}{"type": "ticket.created"}); err != nil {
	panic(err)
}
if err := producer.PushTo(ctx, "comment-events", map[string]interface{}{"type": "comment.created"}); err != nil {
	panic(err)
}
```

### Consumer 
```go
package main

import (
	"context"
	"log"
	"gitlab.com/hannlync/backend/stream-go"
	"time"
)

func main() {
	consumer, err := stream.NewConsumer(
		stream.ConsumerConfig{
			Streams: []string{"mystream"},
			Group:   "test-group",
			Name:    "consumer-1",
			RetryIn: 1 * time.Minute,
		},
		stream.WithNewRedisClient(stream.RedisConfig{
			Addr: "localhost:6379",
			Password: "",
			DB: 0,
		}),
	)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	ctx := context.Background()
	events := consumer.Start(ctx, 1)

	for event := range events {
		// Start processing event
		log.Printf("Received event at %s: %v", time.Now().String(), event)
		// processing done
		if err := event.Ack(ctx); err != nil {
			log.Printf("failed to ack event %s: %v", event.ID, err)
		}
	}

}
```

Use `Streams` to read from multiple streams with one consumer group. `Message.Stream` identifies which stream produced each message and `Message.Ack` acknowledges against that same stream.
`Message.Timestamp` is loaded from the producer's automatic `timestamp` field.

```go
consumer, err := stream.NewConsumer(
	stream.ConsumerConfig{
		Streams: []string{"tickets", "comments"},
		Group:   "test-group",
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
	if err := event.Ack(ctx); err != nil {
		log.Printf("failed to ack event %s from %s: %v", event.ID, event.Stream, err)
	}
}
```

Use `Message.Type` to route arbitrary payloads, then `DecodeValue` to load the payload field into a typed struct. Use `Decode` when you want to load the full message into one struct.

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
		// process data
	}
}
```

### Redis Clients

Use `WithNewRedisClient` to let a producer or consumer create and own a Redis client. Use `WithClient` to share a Redis client or inject a test double:

```go
ctx := context.Background()
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
		Group:  "test-group",
		Name:   "consumer-1",
	},
	stream.WithClient(client),
)
if err != nil {
	panic(err)
}
```

### Consumer Errors

`Start` remains available for simple usage. Use `StartWithErrors` when callers need to handle read failures explicitly:

```go
events, errs := consumer.StartWithErrors(ctx, 1)
for {
	select {
	case event, ok := <-events:
		if !ok {
			return
		}
		// process event
	case err, ok := <-errs:
		if ok {
			log.Printf("consumer error: %v", err)
		}
		return
	}
}
```

## Tests

Unit tests do not require Redis:

```bash
go test ./...
```

Integration tests use `redis/docker-compose.yml`:

```bash
docker compose -f redis/docker-compose.yml up -d
STREAM_GO_INTEGRATION=1 go test ./...
```

```
go run example/producer/main.go -H 10.250.2.102 -p h0VvsKtmhE1S -s event:contact -v id=2222 -v type=contact.form.submitted -v version=1 -v data='{"name": "test", "email":"gecad23850@iapapi.com", "company": "", "subject":"test","messages":"abvsdbjo"}' -v metadata={}
```
