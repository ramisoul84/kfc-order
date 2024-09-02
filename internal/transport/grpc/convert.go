package grpc

import (
	"time"

	"github.com/google/uuid"

	orderv1 "github.com/ramisoul84/kfc-order/gen/order/v1"
	"github.com/ramisoul84/kfc-order/internal/domain"
)

func toProtoOrder(o *domain.Order) *orderv1.Order {
	p := &orderv1.Order{
		Id:            o.ID.String(),
		RestaurantId:  o.RestaurantID.String(),
		Number:        int32(o.Number),
		BusinessDate:  o.BusinessDate.Format(time.DateOnly),
		Channel:       string(o.Channel),
		OrderType:     string(o.OrderType),
		Status:        string(o.Status),
		PaymentMethod: string(o.PaymentMethod),
		PaymentStatus: string(o.PaymentStatus),
		CustomerName:  deref(o.CustomerName),
		CustomerPhone: deref(o.CustomerPhone),
		Note:          deref(o.Note),
		Subtotal:      o.Subtotal,
		Total:         o.Total,
		Currency:      o.Currency,
		CancelReason:  deref(o.CancelReason),
		CreatedAt:     o.CreatedAt.UnixMilli(),
		UpdatedAt:     o.UpdatedAt.UnixMilli(),
		PreparingAt:   millis(o.PreparingAt),
		ReadyAt:       millis(o.ReadyAt),
		CompletedAt:   millis(o.CompletedAt),
		CancelledAt:   millis(o.CancelledAt),
		Lines:         make([]*orderv1.OrderLine, 0, len(o.Lines)),
	}
	if o.DeviceID != nil {
		p.DeviceId = o.DeviceID.String()
	}
	for _, l := range o.Lines {
		pl := &orderv1.OrderLine{
			Id:            l.ID.String(),
			ItemId:        l.ItemID.String(),
			ProductCode:   l.ProductCode,
			Name:          l.Name,
			VariationName: deref(l.VariationName),
			Quantity:      int32(l.Quantity),
			UnitPrice:     l.UnitPrice,
			LineTotal:     l.LineTotal,
		}
		if l.VariationID != nil {
			pl.VariationId = l.VariationID.String()
		}
		p.Lines = append(p.Lines, pl)
	}
	return p
}

func toProtoOrders(orders []*domain.Order) []*orderv1.Order {
	out := make([]*orderv1.Order, 0, len(orders))
	for _, o := range orders {
		out = append(out, toProtoOrder(o))
	}
	return out
}

func toProtoStats(s *domain.OrderStats) *orderv1.OrderStats {
	p := &orderv1.OrderStats{
		TotalOrders:     int32(s.TotalOrders),
		Revenue:         s.Revenue,
		AverageTicket:   s.AverageTicket,
		AvgPrepSeconds:  int32(s.AvgPrepSeconds),
		CancelledOrders: int32(s.CancelledOrders),
		ActiveOrders:    int32(s.ActiveOrders),
	}
	for _, c := range s.ByChannel {
		p.ByChannel = append(p.ByChannel, &orderv1.ChannelStat{
			Channel: c.Channel, Orders: int32(c.Orders), Revenue: c.Revenue,
		})
	}
	for _, h := range s.ByHour {
		p.ByHour = append(p.ByHour, &orderv1.HourStat{
			Hour: int32(h.Hour), Orders: int32(h.Orders), Revenue: h.Revenue,
		})
	}
	for _, i := range s.TopItems {
		p.TopItems = append(p.TopItems, &orderv1.ItemStat{
			ItemId: i.ItemID.String(), Name: i.Name, Quantity: int32(i.Quantity), Revenue: i.Revenue,
		})
	}
	for _, st := range s.ByStatus {
		p.ByStatus = append(p.ByStatus, &orderv1.StatusStat{
			Status: st.Status, Orders: int32(st.Orders),
		})
	}
	return p
}

// parseUUIDs parses a list of IDs, skipping blanks.
func parseUUIDs(raw []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		if s == "" {
			continue
		}
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, domain.NewValidationError("invalid restaurant id: " + s)
		}
		out = append(out, id)
	}
	return out, nil
}

// optionalUUID parses an ID that may be blank.
func optionalUUID(s, field string) (*uuid.UUID, error) {
	if s == "" {
		return nil, nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return nil, domain.NewValidationError("invalid " + field)
	}
	return &id, nil
}

func millis(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

func fromMillis(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms)
	return &t
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
