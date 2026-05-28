package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	stream "github.com/supakarn-j/event-manager"
)

func main() {
	var host, port, password, streamName, group, consumerName string
	var noAckFlag *bool

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("An error occurred: %v\n", r)
			os.Exit(1)
		}
	}()

	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "consumer cli tool",
		Long:  "consumer cli tool to read from Redis stream.",
		Run: func(cmd *cobra.Command, args []string) {
			if streamName == "" {
				fmt.Println("Stream name is required. Use --stream or -s flag to specify the stream name.")
				os.Exit(1)
			}
			if consumerName == "" {
				fmt.Println("Consumer name is required. Use --name or -n flag to specify the name")
				os.Exit(1)
			}

			consumer, err := stream.NewConsumer(
				stream.ConsumerConfig{
					Streams: []string{streamName},
					Group:   group,
					Name:    consumerName,
				},
				stream.WithNewRedisClient(stream.RedisConfig{
					Addr:     fmt.Sprintf("%s:%s", host, port),
					Password: password,
				}),
			)
			if err != nil {
				panic(err)
			}
			defer consumer.Close()

			ctx := context.Background()
			events := consumer.Start(ctx, 1)

			for event := range events {
				log.Printf("Received event at %s: %v", time.Now().String(), event)
				if !*noAckFlag {
					if err := event.Ack(ctx); err != nil {
						log.Printf("Failed to acknowledge event: %v", err)
					}
				}
			}
		},
	}

	cmd.Flags().StringVarP(&host, "host", "H", "localhost", "Host of the Redis server")
	cmd.Flags().StringVarP(&port, "port", "P", "6379", "Port of the Redis server")
	cmd.Flags().StringVarP(&password, "password", "p", "", "Password for Redis server")
	cmd.Flags().StringVarP(&streamName, "stream", "s", "", "Name of Redis stream to produce message to")
	cmd.Flags().StringVarP(&group, "group", "g", "", "Name of consumer group")
	cmd.Flags().StringVarP(&consumerName, "name", "n", "", "Name of consumer")
	noAckFlag = cmd.Flags().Bool("no-ack", false, "Disable automatic acknowledgment of messages")

	cmd.Execute()
}
