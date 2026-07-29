package kafka

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/freyrecorona/openfinance_test/internal/idempotency"
	"github.com/freyrecorona/openfinance_test/internal/transaction"
)

type Handler struct {
	store idempotency.Store
}

func NewHandler(store idempotency.Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event transaction.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal", "error", err)
			continue
		}

		if h.store.Exists(session.Context(), event.TransactionID) {
			slog.Debug("duplicate event skipped", "transaction_id", event.TransactionID)
			session.MarkMessage(msg, "")
			continue
		}

		slog.Info("processing", "transaction_id", event.TransactionID, "status", event.Status)
		time.Sleep(10 * time.Millisecond) // simulate processing

		h.store.Mark(session.Context(), event.TransactionID)
		session.MarkMessage(msg, "")
	}
	return nil
}
