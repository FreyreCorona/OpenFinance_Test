package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

func main() {
	kafkaBrokers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	producer, _ := sarama.NewSyncProducer([]string{kafkaBrokers}, nil)

	rc := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	defer rc.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var sent, duplicates int
	ticker := time.NewTicker(30 * time.Millisecond)
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			fmt.Printf("\n--- Load test ---\n")
			fmt.Printf("Sent: %d events in %s\n", sent, elapsed)
			fmt.Printf("Rate: %.0f events/s\n", float64(sent)/elapsed.Seconds())
			fmt.Printf("Duplicates: %d\n", duplicates)
			return
		case <-ticker.C:
			txID := fmt.Sprintf("tx-%d", rand.Intn(2000))
			msg := fmt.Sprintf(`{"transaction_id":"%s","client_id":"load-test","status":"created","amount":100.0}`, txID)

			exists, _ := rc.Exists(ctx, txID).Result()
			if exists == 1 {
				duplicates++
			} else {
				rc.SetNX(ctx, txID, true, 5*time.Minute)
			}

			producer.SendMessage(&sarama.ProducerMessage{
				Topic: "transactions",
				Key:   sarama.StringEncoder(txID),
				Value: sarama.ByteEncoder(msg),
			})
			sent++
		}
	}
}
