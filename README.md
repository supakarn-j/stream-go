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
	producer, err := stream.NewProducer(stream.ProducerConfig{
		RedisConfig: stream.RedisConfig{
			Addr:     "localhost:6379",
			Password: "",
			DB: 0,
		},
		Name: "mystream"
	})
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
	consumer, err := stream.NewConsumer(stream.ConsumerConfig{
		RedisConfig: stream.RedisConfig{
			Addr: "localhost:6379",
			Password: "",
			DB: 0,
		},
		Streams: []string{"mystream"},
		Group:   "test-group",
		Name:    "consumer-1",
		RetryIn: 1 * time.Minute,
	})
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

```go
consumer, err := stream.NewConsumer(stream.ConsumerConfig{
	RedisConfig: stream.RedisConfig{
		Addr: "localhost:6379",
	},
	Streams: []string{"tickets", "comments"},
	Group:   "test-group",
	Name:    "consumer-1",
})
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

### Injecting a Client

Use `WithClient` to share a Redis client or inject a test double:

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
