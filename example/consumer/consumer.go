package main

import (
	"context"
	"log"
	"time"

	"gitlab.com/hannlync/backend/stream-go.git"
)

func main() {
	conn := stream.NewRedisClient(stream.ConnectionConfig{
		Addr: "localhost:6379",
	})

	defer conn.Close()

	consumer, err := stream.NewConsumer(conn, stream.ConsumerConfig{
		Stream: "mystream",
		Group:  "test-group",
		Name:   "consumer-1",
	})

	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	events := consumer.Start(ctx, 1)

	for event := range events {
		log.Printf("Received event at %s: %v", time.Now().String(), event)
		consumer.Ack(ctx, event.ID)
	}

}
