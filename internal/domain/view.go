package domain

import (
	"time"

	"github.com/google/uuid"
)

// OrderView is the JSON shape of an order on the wire: in Kafka events
// and, re-encoded by kfc-userapi, on device and customer screens.
// Amounts are in rubles here; kopecks are an internal detail.
//
// kfc-userapi keeps an identical struct. Change both together.
type OrderView struct {
	ID            uuid.UUID  `json:"id"`
	RestaurantID  uuid.UUID  `json:"restaurant_id"`
	Number        int        `json:"number"`
	BusinessDate  string     `json:"business_date"`
	Channel       string     `json:"channel"`
	OrderType     string     `json:"order_type"`
	Status        string     `json:"status"`
	PaymentMethod string     `json:"payment_method"`
	PaymentStatus string     `json:"payment_status"`
	CustomerName  string     `json:"customer_name,omitempty"`
	CustomerPhone string     `json:"customer_phone,omitempty"`
	Note          string     `json:"note,omitempty"`
	DeviceID      string     `json:"device_id,omitempty"`
	Subtotal      float64    `json:"subtotal"`
	Total         float64    `json:"total"`
	Currency      string     `json:"currency"`
	CancelReason  string     `json:"cancel_reason,omitempty"`
	Lines         []LineView `json:"lines"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	PreparingAt   *time.Time `json:"preparing_at,omitempty"`
	ReadyAt       *time.Time `json:"ready_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	CancelledAt   *time.Time `json:"cancelled_at,omitempty"`
}

type LineView struct {
	ID            uuid.UUID `json:"id"`
	ItemID        uuid.UUID `json:"item_id"`
	ProductCode   string    `json:"product_code"`
	Name          string    `json:"name"`
	VariationID   string    `json:"variation_id,omitempty"`
	VariationName string    `json:"variation_name,omitempty"`
	Quantity      int       `json:"quantity"`
	UnitPrice     float64   `json:"unit_price"`
	LineTotal     float64   `json:"line_total"`
}

// ToView converts an order to its wire shape.
func (o *Order) ToView() *OrderView {
	v := &OrderView{
		ID:            o.ID,
		RestaurantID:  o.RestaurantID,
		Number:        o.Number,
		BusinessDate:  o.BusinessDate.Format(time.DateOnly),
		Channel:       string(o.Channel),
		OrderType:     string(o.OrderType),
		Status:        string(o.Status),
		PaymentMethod: string(o.PaymentMethod),
		PaymentStatus: string(o.PaymentStatus),
		CustomerName:  deref(o.CustomerName),
		CustomerPhone: deref(o.CustomerPhone),
		Note:          deref(o.Note),
		Subtotal:      Rubles(o.Subtotal),
		Total:         Rubles(o.Total),
		Currency:      o.Currency,
		CancelReason:  deref(o.CancelReason),
		Lines:         make([]LineView, 0, len(o.Lines)),
		CreatedAt:     o.CreatedAt,
		UpdatedAt:     o.UpdatedAt,
		PreparingAt:   o.PreparingAt,
		ReadyAt:       o.ReadyAt,
		CompletedAt:   o.CompletedAt,
		CancelledAt:   o.CancelledAt,
	}
	if o.DeviceID != nil {
		v.DeviceID = o.DeviceID.String()
	}
	for _, l := range o.Lines {
		lv := LineView{
			ID:            l.ID,
			ItemID:        l.ItemID,
			ProductCode:   l.ProductCode,
			Name:          l.Name,
			VariationName: deref(l.VariationName),
			Quantity:      l.Quantity,
			UnitPrice:     Rubles(l.UnitPrice),
			LineTotal:     Rubles(l.LineTotal),
		}
		if l.VariationID != nil {
			lv.VariationID = l.VariationID.String()
		}
		v.Lines = append(v.Lines, lv)
	}
	return v
}

// Rubles converts kopecks to rubles.
func Rubles(kopecks int64) float64 {
	return float64(kopecks) / 100
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
