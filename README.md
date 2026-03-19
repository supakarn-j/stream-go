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
go get gitlab.com/hannlync/backend/stream-go@v0.1.0
```

## Example
### Producer
```go
import (
    "gitlab.com/hannlync/backend/stream-go"
    "context"
)

conn := stream.NewRedisClient(
    ConnectionConfig{
        Addr:     "localhost:6379",
        Password: "",
        DB: 0,
        }
    )

producer, err := stream.NewProducer(
    conn, 
    stream.ProducerConfig{Name: "mystream"},
)

ctx := context.Backgroud()
prooducer.Push(
    ctx, 
    map[string]interface{}{
        "key1": "value1",
        ...
    },
    )
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
	conn := stream.NewRedisClient(stream.ConnectionConfig{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})

	defer conn.Close()

	consumer, err := stream.NewConsumer(conn, stream.ConsumerConfig{
		Stream:  "mystream",
		Group:   "test-group",
		Name:    "consumer-1",
		RetryIn: 1 * time.Minute,
	})


	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	events := consumer.Start(ctx, 1)

	for event := range events {
		// Start processing event
		log.Printf("Received event at %s: %v", time.Now().String(), event)
		// processing done
		consumer.Ack(ctx, event.ID)
	}

}
```