package domain

import (
	"time"

	"github.com/google/uuid"
)

// ═══════════════════════════════════════════════════════════════════
// ENUMS
// ═══════════════════════════════════════════════════════════════════

// Status is where an order is in the kitchen flow.
type Status string

const (
	StatusAccepted  Status = "accepted"  // paid and sent to the kitchen
	StatusPreparing Status = "preparing" // a cook started it on the KDS
	StatusReady     Status = "ready"     // bumped; shown as "ready" in the hall
	StatusCompleted Status = "completed" // handed to the customer
	StatusCancelled Status = "cancelled"
)

// ActiveStatuses are the statuses the KDS and hall display care about.
var ActiveStatuses = []Status{StatusAccepted, StatusPreparing, StatusReady}

// transitions lists the allowed next states for each state.
//
// Besides the happy path, the KDS can "recall" a bumped order
// (ready → preparing) and skip straight to ready for items that need no
// cooking (accepted → ready). Terminal states have no exits.
var transitions = map[Status][]Status{
	StatusAccepted:  {StatusPreparing, StatusReady, StatusCancelled},
	StatusPreparing: {StatusReady, StatusAccepted, StatusCancelled},
	StatusReady:     {StatusCompleted, StatusPreparing},
}

// IsValid reports whether s is a known status.
func (s Status) IsValid() bool {
	switch s {
	case StatusAccepted, StatusPreparing, StatusReady, StatusCompleted, StatusCancelled:
		return true
	}
	return false
}

// CanTransitionTo reports whether s → next is allowed.
func (s Status) CanTransitionTo(next Status) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Channel is where the order was placed.
type Channel string

const (
	ChannelKiosk Channel = "kiosk"
	ChannelWeb   Channel = "web"
	ChannelPOS   Channel = "pos"
)

func (c Channel) IsValid() bool {
	return c == ChannelKiosk || c == ChannelWeb || c == ChannelPOS
}

// OrderType is eat-in or take-out.
type OrderType string

const (
	OrderTypeDineIn   OrderType = "dine_in"
	OrderTypeTakeaway OrderType = "takeaway"
)

func (t OrderType) IsValid() bool {
	return t == OrderTypeDineIn || t == OrderTypeTakeaway
}

// PaymentMethod and PaymentStatus describe how the order was paid.
type PaymentMethod string

const (
	PaymentCard   PaymentMethod = "card"
	PaymentCash   PaymentMethod = "cash"
	PaymentOnline PaymentMethod = "online"
)

func (p PaymentMethod) IsValid() bool {
	return p == PaymentCard || p == PaymentCash || p == PaymentOnline
}

type PaymentStatus string

const (
	PaymentPaid   PaymentStatus = "paid"
	PaymentUnpaid PaymentStatus = "unpaid"
)

func (p PaymentStatus) IsValid() bool {
	return p == PaymentPaid || p == PaymentUnpaid
}

// ═══════════════════════════════════════════════════════════════════
// ENTITIES
// ═══════════════════════════════════════════════════════════════════

// Order is a customer order. Money fields are in kopecks.
type Order struct {
	ID             uuid.UUID     `db:"id"`
	RestaurantID   uuid.UUID     `db:"restaurant_id"`
	Number         int           `db:"number"`
	BusinessDate   time.Time     `db:"business_date"`
	Channel        Channel       `db:"channel"`
	OrderType      OrderType     `db:"order_type"`
	Status         Status        `db:"status"`
	PaymentMethod  PaymentMethod `db:"payment_method"`
	PaymentStatus  PaymentStatus `db:"payment_status"`
	CustomerName   *string       `db:"customer_name"`
	CustomerPhone  *string       `db:"customer_phone"`
	Note           *string       `db:"note"`
	DeviceID       *uuid.UUID    `db:"device_id"`
	Subtotal       int64         `db:"subtotal"`
	Total          int64         `db:"total"`
	Currency       string        `db:"currency"`
	CancelReason   *string       `db:"cancel_reason"`
	IdempotencyKey *string       `db:"idempotency_key"`
	CreatedAt      time.Time     `db:"created_at"`
	UpdatedAt      time.Time     `db:"updated_at"`
	PreparingAt    *time.Time    `db:"preparing_at"`
	ReadyAt        *time.Time    `db:"ready_at"`
	CompletedAt    *time.Time    `db:"completed_at"`
	CancelledAt    *time.Time    `db:"cancelled_at"`

	Lines []OrderLine `db:"-"`
}

// OrderLine is one product on an order.
type OrderLine struct {
	ID            uuid.UUID  `db:"id"`
	OrderID       uuid.UUID  `db:"order_id"`
	Position      int        `db:"position"`
	ItemID        uuid.UUID  `db:"item_id"`
	ProductCode   string     `db:"product_code"`
	Name          string     `db:"name"`
	VariationID   *uuid.UUID `db:"variation_id"`
	VariationName *string    `db:"variation_name"`
	Quantity      int        `db:"quantity"`
	UnitPrice     int64      `db:"unit_price"`
	LineTotal     int64      `db:"line_total"`
}

// ═══════════════════════════════════════════════════════════════════
// INPUTS
// ═══════════════════════════════════════════════════════════════════

// Limits on a single order. A real kiosk caps these too, both to stop
// fat-finger orders and to keep tickets readable on the KDS.
const (
	MaxLinesPerOrder = 30
	MaxQtyPerLine    = 50
)

type CreateOrderInput struct {
	RestaurantID   uuid.UUID
	Channel        Channel
	OrderType      OrderType
	PaymentMethod  PaymentMethod
	PaymentStatus  PaymentStatus
	CustomerName   string
	CustomerPhone  string
	Note           string
	DeviceID       *uuid.UUID
	Currency       string
	IdempotencyKey string
	Lines          []CreateLineInput
}

type CreateLineInput struct {
	ItemID        uuid.UUID
	ProductCode   string
	Name          string
	VariationID   *uuid.UUID
	VariationName string
	Quantity      int
	UnitPrice     int64
}

// OrderFilter is used by CRM listings.
type OrderFilter struct {
	RestaurantIDs []uuid.UUID
	Status        Status
	Channel       Channel
	Number        int
	From          *time.Time
	To            *time.Time
	Limit         int
	Offset        int
}

// ═══════════════════════════════════════════════════════════════════
// STATS
// ═══════════════════════════════════════════════════════════════════

type OrderStats struct {
	TotalOrders     int
	Revenue         int64
	AverageTicket   int64
	AvgPrepSeconds  int
	CancelledOrders int
	ActiveOrders    int
	ByChannel       []ChannelStat
	ByHour          []HourStat
	TopItems        []ItemStat
	ByStatus        []StatusStat
}

type ChannelStat struct {
	Channel string `db:"channel"`
	Orders  int    `db:"orders"`
	Revenue int64  `db:"revenue"`
}

type HourStat struct {
	Hour    int   `db:"hour"`
	Orders  int   `db:"orders"`
	Revenue int64 `db:"revenue"`
}

type ItemStat struct {
	ItemID   uuid.UUID `db:"item_id"`
	Name     string    `db:"name"`
	Quantity int       `db:"quantity"`
	Revenue  int64     `db:"revenue"`
}

type StatusStat struct {
	Status string `db:"status"`
	Orders int    `db:"orders"`
}
