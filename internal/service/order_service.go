package service

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/ramisoul84/kfc-order/internal/domain"
	"github.com/ramisoul84/kfc-order/internal/events"
	"github.com/ramisoul84/kfc-order/internal/repository"
	"github.com/ramisoul84/kfc-order/pkg/logger"
)

// OrderService implements the order lifecycle.
type OrderService interface {
	Create(ctx context.Context, in *domain.CreateOrderInput) (*domain.Order, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.Order, error)
	ListActive(ctx context.Context, restaurantID uuid.UUID, statuses []domain.Status) ([]*domain.Order, error)
	List(ctx context.Context, filter domain.OrderFilter) ([]*domain.Order, int, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status, reason, actor string) (*domain.Order, error)
	Stats(ctx context.Context, restaurantIDs []uuid.UUID, from, to time.Time) (*domain.OrderStats, error)
}

type orderService struct {
	repo      repository.OrderRepository
	publisher *events.Publisher
	loc       *time.Location
	logger    *logger.Logger
}

func NewOrderService(
	repo repository.OrderRepository,
	publisher *events.Publisher,
	loc *time.Location,
	log *logger.Logger,
) OrderService {
	return &orderService{repo: repo, publisher: publisher, loc: loc, logger: log}
}

// ═══════════════════════════════════════════════════════════════════
// CREATE
// ═══════════════════════════════════════════════════════════════════

func (s *orderService) Create(ctx context.Context, in *domain.CreateOrderInput) (*domain.Order, error) {
	if err := validateCreate(in); err != nil {
		return nil, err
	}

	// Idempotent retry: a kiosk that lost the response re-sends the same
	// key and gets the original order back, not a second one.
	if in.IdempotencyKey != "" {
		existing, err := s.repo.GetByIdempotencyKey(ctx, in.RestaurantID, in.IdempotencyKey)
		if err == nil {
			s.logger.Info("idempotent order replay",
				"order_id", existing.ID,
				"restaurant_id", in.RestaurantID,
			)
			return existing, nil
		}
		if !domain.IsNotFoundError(err) {
			return nil, err
		}
	}

	now := time.Now().In(s.loc)
	order := &domain.Order{
		ID:            uuid.New(),
		RestaurantID:  in.RestaurantID,
		BusinessDate:  time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC),
		Channel:       in.Channel,
		OrderType:     in.OrderType,
		Status:        domain.StatusAccepted,
		PaymentMethod: in.PaymentMethod,
		PaymentStatus: in.PaymentStatus,
		CustomerName:  optional(in.CustomerName),
		CustomerPhone: optional(in.CustomerPhone),
		Note:          optional(in.Note),
		DeviceID:      in.DeviceID,
		Currency:      in.Currency,
		Lines:         make([]domain.OrderLine, 0, len(in.Lines)),
	}
	if order.Currency == "" {
		order.Currency = "RUB"
	}
	if in.IdempotencyKey != "" {
		order.IdempotencyKey = &in.IdempotencyKey
	}
	order.MemberID = in.MemberID
	order.LoyaltyHoldID = in.LoyaltyHoldID
	order.PointsRedeemed = in.PointsRedeemed

	for _, l := range in.Lines {
		lineTotal := l.UnitPrice * int64(l.Quantity)
		order.Lines = append(order.Lines, domain.OrderLine{
			ID:            uuid.New(),
			ItemID:        l.ItemID,
			ProductCode:   l.ProductCode,
			Name:          l.Name,
			VariationID:   l.VariationID,
			VariationName: optional(l.VariationName),
			Quantity:      l.Quantity,
			UnitPrice:     l.UnitPrice,
			LineTotal:     lineTotal,
		})
		order.Subtotal += lineTotal
	}
	if in.Discount > order.Subtotal {
		return nil, domain.NewValidationError("discount exceeds the order subtotal")
	}
	order.Discount = in.Discount
	order.Total = order.Subtotal - in.Discount

	if err := s.repo.Create(ctx, order); err != nil {
		// Lost a race with a concurrent retry carrying the same key.
		if domain.IsConflictError(err) && in.IdempotencyKey != "" {
			return s.repo.GetByIdempotencyKey(ctx, in.RestaurantID, in.IdempotencyKey)
		}
		s.logger.Error("create order failed", "restaurant_id", in.RestaurantID, "error", err)
		return nil, err
	}

	s.logger.Info("order created",
		"order_id", order.ID,
		"restaurant_id", order.RestaurantID,
		"number", order.Number,
		"channel", order.Channel,
		"total", order.Total,
		"lines", len(order.Lines),
	)

	s.publisher.Publish(ctx, events.TypeOrderCreated, order)
	return order, nil
}

func validateCreate(in *domain.CreateOrderInput) error {
	if in.RestaurantID == uuid.Nil {
		return domain.NewValidationError("restaurant_id is required")
	}
	if !in.Channel.IsValid() {
		return domain.NewValidationError("invalid channel")
	}
	if !in.OrderType.IsValid() {
		return domain.NewValidationError("invalid order_type")
	}
	if !in.PaymentMethod.IsValid() {
		return domain.NewValidationError("invalid payment_method")
	}
	if !in.PaymentStatus.IsValid() {
		return domain.NewValidationError("invalid payment_status")
	}
	if len(in.Lines) == 0 {
		return domain.NewValidationError("order has no lines")
	}
	if len(in.Lines) > domain.MaxLinesPerOrder {
		return domain.NewValidationError("too many lines in one order")
	}
	if utf8.RuneCountInString(in.CustomerName) > 100 {
		return domain.NewValidationError("customer_name is too long")
	}
	if utf8.RuneCountInString(in.Note) > 500 {
		return domain.NewValidationError("note is too long")
	}
	if in.Discount < 0 || in.PointsRedeemed < 0 {
		return domain.NewValidationError("discount must not be negative")
	}
	if in.Discount > 0 && (in.MemberID == nil || in.LoyaltyHoldID == nil) {
		return domain.NewValidationError("a discount requires a loyalty member and hold")
	}
	if len(in.IdempotencyKey) > 100 {
		return domain.NewValidationError("idempotency_key is too long")
	}
	for _, l := range in.Lines {
		if l.ItemID == uuid.Nil || strings.TrimSpace(l.Name) == "" {
			return domain.NewValidationError("every line needs item_id and name")
		}
		if l.Quantity < 1 || l.Quantity > domain.MaxQtyPerLine {
			return domain.NewValidationError("line quantity out of range")
		}
		if l.UnitPrice < 0 {
			return domain.NewValidationError("line unit_price must not be negative")
		}
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// READ
// ═══════════════════════════════════════════════════════════════════

func (s *orderService) Get(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *orderService) ListActive(
	ctx context.Context,
	restaurantID uuid.UUID,
	statuses []domain.Status,
) ([]*domain.Order, error) {
	if restaurantID == uuid.Nil {
		return nil, domain.NewValidationError("restaurant_id is required")
	}
	if len(statuses) == 0 {
		statuses = domain.ActiveStatuses
	}
	for _, st := range statuses {
		if !st.IsValid() {
			return nil, domain.NewValidationError("invalid status filter")
		}
	}
	return s.repo.ListByStatus(ctx, restaurantID, statuses)
}

func (s *orderService) List(ctx context.Context, filter domain.OrderFilter) ([]*domain.Order, int, error) {
	if filter.Status != "" && !filter.Status.IsValid() {
		return nil, 0, domain.NewValidationError("invalid status filter")
	}
	if filter.Channel != "" && !filter.Channel.IsValid() {
		return nil, 0, domain.NewValidationError("invalid channel filter")
	}
	return s.repo.List(ctx, filter)
}

func (s *orderService) Stats(
	ctx context.Context,
	restaurantIDs []uuid.UUID,
	from, to time.Time,
) (*domain.OrderStats, error) {
	if !to.After(from) {
		return nil, domain.NewValidationError("'to' must be after 'from'")
	}
	if to.Sub(from) > 366*24*time.Hour {
		return nil, domain.NewValidationError("stats range is limited to one year")
	}
	return s.repo.Stats(ctx, restaurantIDs, from, to, s.loc)
}

// ═══════════════════════════════════════════════════════════════════
// STATUS
// ═══════════════════════════════════════════════════════════════════

func (s *orderService) UpdateStatus(
	ctx context.Context,
	id uuid.UUID,
	to domain.Status,
	reason, actor string,
) (*domain.Order, error) {
	if !to.IsValid() {
		return nil, domain.NewValidationError("invalid status")
	}
	if to == domain.StatusCancelled && strings.TrimSpace(reason) == "" {
		return nil, domain.NewValidationError("a reason is required to cancel an order")
	}

	current, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if current.Status == to {
		// Double-tap on the KDS. Not an error; nothing to publish.
		return current, nil
	}
	if !current.Status.CanTransitionTo(to) {
		return nil, domain.NewConflictError(
			"cannot move order from " + string(current.Status) + " to " + string(to))
	}

	updated, err := s.repo.UpdateStatus(ctx, id, current.Status, to, reason)
	if err != nil {
		return nil, err
	}

	s.logger.Info("order status changed",
		"order_id", id,
		"restaurant_id", updated.RestaurantID,
		"number", updated.Number,
		"from", current.Status,
		"to", to,
		"actor", actor,
	)

	s.publisher.Publish(ctx, events.TypeOrderStatusChanged, updated)
	return updated, nil
}

func optional(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}
