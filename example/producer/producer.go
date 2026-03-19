package main

import (
	"context"
	"time"

	"gitlab.com/hannlync/backend/stream-go.git"
)

func main() {
	conn := stream.NewRedisClient(stream.ConnectionConfig{
		Addr: "localhost:6379",
	})

	defer conn.Close()

	producer, err := stream.NewProducer(conn, stream.ProducerConfig{
		Name: "mystream",
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	producer.Push(ctx, map[string]interface{}{
		"key1":    "value1",
		"sent_at": time.Now().String(),
	})
}
