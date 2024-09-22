package events

import (
	"time"

	"github.com/ramisoul84/kfc-order/internal/domain"
)

// Event types on the orders topic.
const (
	TypeOrderCreated       = "order.created"
	TypeOrderStatusChanged = "order.status_changed"
)

// OrderEvent is emitted on every order mutation. It carries the full
// order so consumers (userapi → KDS, hall display, customer tracking;
// loyalty → points) never need a follow-up read.
//
// Events are written to the transactional outbox in the same transaction
// as the change (see repository and pkg/outbox), so a committed order
// change always produces its event, even if Kafka is down at the time.
type OrderEvent struct {
	Type         string            `json:"type"`
	RestaurantID string            `json:"restaurant_id"`
	Order        *domain.OrderView `json:"order"`
	OccurredAt   time.Time         `json:"occurred_at"`
}

// New builds the event for an order change.
func New(eventType string, order *domain.Order) OrderEvent {
	return OrderEvent{
		Type:         eventType,
		RestaurantID: order.RestaurantID.String(),
		Order:        order.ToView(),
		OccurredAt:   time.Now().UTC(),
	}
}
