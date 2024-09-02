package grpc

import (
	"context"
	"time"

	"github.com/google/uuid"

	orderv1 "github.com/ramisoul84/kfc-order/gen/order/v1"
	"github.com/ramisoul84/kfc-order/internal/domain"
	"github.com/ramisoul84/kfc-order/internal/service"
	"github.com/ramisoul84/kfc-order/pkg/logger"
)

// OrderServer implements orderv1.OrderServiceServer.
type OrderServer struct {
	orderv1.UnimplementedOrderServiceServer
	svc    service.OrderService
	logger *logger.Logger
}

func NewOrderServer(svc service.OrderService, log *logger.Logger) *OrderServer {
	return &OrderServer{svc: svc, logger: log}
}

func (s *OrderServer) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.Order, error) {
	restaurantID, err := uuid.Parse(req.GetRestaurantId())
	if err != nil {
		return nil, mapErrorToGRPC(domain.NewValidationError("invalid restaurant_id"))
	}
	deviceID, err := optionalUUID(req.GetDeviceId(), "device_id")
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}

	in := &domain.CreateOrderInput{
		RestaurantID:   restaurantID,
		Channel:        domain.Channel(req.GetChannel()),
		OrderType:      domain.OrderType(req.GetOrderType()),
		PaymentMethod:  domain.PaymentMethod(req.GetPaymentMethod()),
		PaymentStatus:  domain.PaymentStatus(req.GetPaymentStatus()),
		CustomerName:   req.GetCustomerName(),
		CustomerPhone:  req.GetCustomerPhone(),
		Note:           req.GetNote(),
		DeviceID:       deviceID,
		Currency:       req.GetCurrency(),
		IdempotencyKey: req.GetIdempotencyKey(),
		Lines:          make([]domain.CreateLineInput, 0, len(req.GetLines())),
	}
	for _, l := range req.GetLines() {
		itemID, err := uuid.Parse(l.GetItemId())
		if err != nil {
			return nil, mapErrorToGRPC(domain.NewValidationError("invalid line item_id"))
		}
		variationID, err := optionalUUID(l.GetVariationId(), "line variation_id")
		if err != nil {
			return nil, mapErrorToGRPC(err)
		}
		in.Lines = append(in.Lines, domain.CreateLineInput{
			ItemID:        itemID,
			ProductCode:   l.GetProductCode(),
			Name:          l.GetName(),
			VariationID:   variationID,
			VariationName: l.GetVariationName(),
			Quantity:      int(l.GetQuantity()),
			UnitPrice:     l.GetUnitPrice(),
		})
	}

	order, err := s.svc.Create(ctx, in)
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	return toProtoOrder(order), nil
}

func (s *OrderServer) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.Order, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, mapErrorToGRPC(domain.NewValidationError("invalid order id"))
	}
	order, err := s.svc.Get(ctx, id)
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	return toProtoOrder(order), nil
}

func (s *OrderServer) ListActiveOrders(ctx context.Context, req *orderv1.ListActiveOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	restaurantID, err := uuid.Parse(req.GetRestaurantId())
	if err != nil {
		return nil, mapErrorToGRPC(domain.NewValidationError("invalid restaurant_id"))
	}
	statuses := make([]domain.Status, 0, len(req.GetStatuses()))
	for _, st := range req.GetStatuses() {
		statuses = append(statuses, domain.Status(st))
	}

	orders, err := s.svc.ListActive(ctx, restaurantID, statuses)
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	return &orderv1.ListOrdersResponse{Orders: toProtoOrders(orders), Total: int32(len(orders))}, nil
}

func (s *OrderServer) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	restaurantIDs, err := parseUUIDs(req.GetRestaurantIds())
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}

	orders, total, err := s.svc.List(ctx, domain.OrderFilter{
		RestaurantIDs: restaurantIDs,
		Status:        domain.Status(req.GetStatus()),
		Channel:       domain.Channel(req.GetChannel()),
		Number:        int(req.GetNumber()),
		From:          fromMillis(req.GetFrom()),
		To:            fromMillis(req.GetTo()),
		Limit:         int(req.GetLimit()),
		Offset:        int(req.GetOffset()),
	})
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	return &orderv1.ListOrdersResponse{Orders: toProtoOrders(orders), Total: int32(total)}, nil
}

func (s *OrderServer) UpdateOrderStatus(ctx context.Context, req *orderv1.UpdateOrderStatusRequest) (*orderv1.Order, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, mapErrorToGRPC(domain.NewValidationError("invalid order id"))
	}
	order, err := s.svc.UpdateStatus(ctx, id, domain.Status(req.GetStatus()), req.GetReason(), req.GetActor())
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	return toProtoOrder(order), nil
}

func (s *OrderServer) GetOrderStats(ctx context.Context, req *orderv1.GetOrderStatsRequest) (*orderv1.OrderStats, error) {
	restaurantIDs, err := parseUUIDs(req.GetRestaurantIds())
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	if req.GetFrom() == 0 || req.GetTo() == 0 {
		return nil, mapErrorToGRPC(domain.NewValidationError("from and to are required"))
	}

	stats, err := s.svc.Stats(ctx, restaurantIDs, time.UnixMilli(req.GetFrom()), time.UnixMilli(req.GetTo()))
	if err != nil {
		return nil, mapErrorToGRPC(err)
	}
	return toProtoStats(stats), nil
}
