package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"gitlab.com/hannlync/backend/stream-go.git"
)

func main() {
	var host, port, password, streamName string
	var values *[]string

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("An error occurred: %v\n", r)
			os.Exit(1)
		}
	}()

	cmd := &cobra.Command{
		Use:   "producer",
		Short: "producer cli tool",
		Long:  `producer cli tool to connect to Redis server and produce messages.`,
		Run: func(cmd *cobra.Command, args []string) {
			if values == nil || len(*values) == 0 {
				fmt.Println("No key-value pairs provided. Use --value or -v flag to add key-value pairs.")
				os.Exit(1)
			}

			if streamName == "" {
				fmt.Println("Stream name is required. Use --stream or -s flag to specify the stream name.")
				os.Exit(1)
			}
			fmt.Printf("Pushing message to Redis server at %s:%s stream '%s'...\n", host, port, streamName)

			producer, err := stream.NewProducer(stream.ProducerConfig{
				RedisConfig: stream.RedisConfig{
					Addr:     fmt.Sprintf("%s:%s", host, port),
					Password: password,
				},
				Name: streamName,
			})
			if err != nil {
				fmt.Printf("Error creating producer: %v\n", err)
				os.Exit(1)
			}
			defer producer.Close()

			message := make(map[string]interface{})
			for _, kv := range *values {
				parts := strings.SplitN(kv, "=", 2)
				if len(parts) != 2 {
					fmt.Printf("Invalid key-value pair: %s. Expected format: key=value\n", kv)
				}
				key := parts[0]
				value := parts[1]
				message[key] = value
			}

			ctx := context.Background()
			err = producer.Push(ctx, message)
			if err != nil {
				fmt.Printf("Error pushing message: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Message produced successfully! Message: %v\n", message)
		},
	}

	cmd.Flags().StringVarP(&host, "host", "H", "localhost", "Host of the Redis server")
	cmd.Flags().StringVarP(&port, "port", "P", "6379", "Port of the Redis server")
	cmd.Flags().StringVarP(&password, "password", "p", "", "Password for Redis server")
	cmd.Flags().StringVarP(&streamName, "stream", "s", "", "Name of Redis stream to produce message to")
	values = cmd.Flags().StringArrayP("value", "v", []string{}, "Key-value pair to include in the message (format: key=value)")

	cmd.Execute()
}
