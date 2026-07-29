package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"github.com/freyrecorona/openfinance_test/internal/idempotency"
	"github.com/freyrecorona/openfinance_test/internal/kafka"
	"github.com/redis/go-redis/v9"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	rc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	if err := rc.Ping(ctx).Err(); err != nil {
		slog.Error("failed to connect to Redis", "error", err)
		panic(err)
	}
	defer rc.Close()

	store := idempotency.NewRedisStore(rc, 5*time.Minute)
	handler := kafka.NewHandler(store)

	client, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "antifraude", config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Start consuming messages
	for {
		if err := client.Consume(ctx, []string{"transactions"}, handler); err != nil {
			if ctx.Err() != nil {
				slog.Info("consumer stopped", "reason", ctx.Err())
				return
			}
			slog.Error("consume error", "error", err)
		}
	}
}
