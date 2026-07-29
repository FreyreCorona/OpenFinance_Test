package transaction

import "time"

type EventStatus string

const (
	StatusCreated    EventStatus = "created"
	StatusAuthorized EventStatus = "authorized"
	StatusSettled    EventStatus = "settled"
	StatusRejected   EventStatus = "rejected"
)

type Event struct {
	TransactionID string      `json:"transaction_id"`
	ClientID      string      `json:"client_id"`
	Status        EventStatus `json:"status"`
	Amount        float64     `json:"amount"`
	Timestamp     time.Time   `json:"timestamp"`
}
