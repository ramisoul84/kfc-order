package events

import (
	"context"
	"time"

	"github.com/ramisoul84/kfc-order/internal/domain"
	"github.com/ramisoul84/kfc-order/pkg/kafka"
	"github.com/ramisoul84/kfc-order/pkg/logger"
)

// Event types on the orders topic.
const (
	TypeOrderCreated       = "order.created"
	TypeOrderStatusChanged = "order.status_changed"
)

// OrderEvent is published on every order mutation. It carries the full
// order so consumers (userapi → KDS, hall display, customer tracking)
// never need a follow-up read.
type OrderEvent struct {
	Type         string            `json:"type"`
	RestaurantID string            `json:"restaurant_id"`
	Order        *domain.OrderView `json:"order"`
	OccurredAt   time.Time         `json:"occurred_at"`
}

// Publisher publishes order events to Kafka.
type Publisher struct {
	producer *kafka.Producer
	topic    string
	logger   *logger.Logger
}

func NewPublisher(producer *kafka.Producer, topic string, log *logger.Logger) *Publisher {
	return &Publisher{producer: producer, topic: topic, logger: log}
}

// Publish emits an order event keyed by restaurant, so all events for one
// restaurant land on the same partition and stay in order.
//
// Best-effort: the order is already committed. If Kafka is briefly down,
// screens catch up on their next reconnect, which always refetches the
// full active list.
func (p *Publisher) Publish(ctx context.Context, eventType string, order *domain.Order) {
	if p == nil || p.producer == nil {
		return
	}

	evt := OrderEvent{
		Type:         eventType,
		RestaurantID: order.RestaurantID.String(),
		Order:        order.ToView(),
		OccurredAt:   time.Now().UTC(),
	}

	if err := p.producer.Publish(ctx, p.topic, evt.RestaurantID, evt); err != nil {
		p.logger.Error("failed to publish order event",
			"type", eventType,
			"order_id", order.ID,
			"error", err,
		)
		return
	}

	p.logger.Debug("order event published",
		"type", eventType,
		"order_id", order.ID,
		"status", order.Status,
	)
}
