package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/freyrecorona/openfinance_test/internal/transaction"
)

var status = []transaction.EventStatus{
	transaction.StatusCreated,
	transaction.StatusAuthorized,
	transaction.StatusSettled,
	transaction.StatusRejected,
}

func main() {
	c := sarama.NewConfig()
	c.Producer.Return.Successes = true
	c.Net.MaxOpenRequests = 1
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Idempotent = true

	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	producer, err := sarama.NewAsyncProducer([]string{bootstrapServers}, c)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	//goroutine to handle successes and errors without blocking the main loop
	go func() {
		for {
			select {
			case s := <-producer.Successes():
				slog.Info("send success", "topic", s.Topic, "partition", s.Partition, "offset", s.Offset)
			case err := <-producer.Errors():
				slog.Error("send error", "msg", err.Error())
			}
		}
	}()

	for {
		event := transaction.Event{
			TransactionID: fmt.Sprintf("tx-%d", rand.Intn(1000)),
			ClientID:      fmt.Sprintf("client-%d", rand.Intn(50)),
			Status:        status[rand.Intn(len(status))],
			Amount:        rand.Float64() * 1000,
			Timestamp:     time.Now(),
		}

		msg, err := json.Marshal(event)
		if err != nil {
			slog.Error("error during event marshalling", "msg", err.Error())
			continue
		}

		// send the message to the Kafka topic
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "transactions",
			Key:   sarama.StringEncoder(event.TransactionID),
			Value: sarama.ByteEncoder(msg),
		}

		time.Sleep(100 * time.Millisecond)
	}
}
