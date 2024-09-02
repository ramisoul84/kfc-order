package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/ramisoul84/kfc-order/internal/domain"
)

// OrderRepository persists orders and their lines.
type OrderRepository interface {
	// Create assigns the next daily number and inserts the order with its
	// lines in one transaction. order.ID, Number, BusinessDate, CreatedAt
	// and UpdatedAt are filled in on success.
	Create(ctx context.Context, order *domain.Order) error

	GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error)
	GetByIdempotencyKey(ctx context.Context, restaurantID uuid.UUID, key string) (*domain.Order, error)
	ListByStatus(ctx context.Context, restaurantID uuid.UUID, statuses []domain.Status) ([]*domain.Order, error)
	List(ctx context.Context, filter domain.OrderFilter) ([]*domain.Order, int, error)

	// UpdateStatus moves an order from `from` to `to`. It returns a
	// conflict error if the order is no longer in `from`, so two KDS
	// screens bumping the same ticket can't both win.
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status, reason string) (*domain.Order, error)

	Stats(ctx context.Context, restaurantIDs []uuid.UUID, from, to time.Time, loc *time.Location) (*domain.OrderStats, error)
}

type orderRepo struct {
	db *sqlx.DB
}

func NewOrderRepository(db *sqlx.DB) OrderRepository {
	return &orderRepo{db: db}
}

const orderColumns = `
	id, restaurant_id, number, business_date, channel, order_type, status,
	payment_method, payment_status, customer_name, customer_phone, note,
	device_id, subtotal, total, currency, cancel_reason, idempotency_key,
	created_at, updated_at, preparing_at, ready_at, completed_at, cancelled_at`

// ═══════════════════════════════════════════════════════════════════
// CREATE
// ═══════════════════════════════════════════════════════════════════

func (r *orderRepo) Create(ctx context.Context, o *domain.Order) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return domain.NewInternalError(fmt.Errorf("begin: %w", err))
	}
	defer tx.Rollback() //nolint:errcheck

	// Upsert-and-increment in one statement: the row lock taken by
	// ON CONFLICT DO UPDATE serialises concurrent orders per restaurant.
	err = tx.GetContext(ctx, &o.Number, `
		INSERT INTO order_counters (restaurant_id, business_date, last_number)
		VALUES ($1, $2, 1)
		ON CONFLICT (restaurant_id, business_date)
		DO UPDATE SET last_number = order_counters.last_number + 1
		RETURNING last_number`,
		o.RestaurantID, o.BusinessDate.Format(time.DateOnly),
	)
	if err != nil {
		return domain.NewInternalError(fmt.Errorf("next order number: %w", err))
	}

	err = tx.QueryRowxContext(ctx, `
		INSERT INTO orders (
			id, restaurant_id, number, business_date, channel, order_type, status,
			payment_method, payment_status, customer_name, customer_phone, note,
			device_id, subtotal, total, currency, idempotency_key
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		RETURNING created_at, updated_at`,
		o.ID, o.RestaurantID, o.Number, o.BusinessDate.Format(time.DateOnly),
		o.Channel, o.OrderType, o.Status, o.PaymentMethod, o.PaymentStatus,
		o.CustomerName, o.CustomerPhone, o.Note, o.DeviceID,
		o.Subtotal, o.Total, o.Currency, o.IdempotencyKey,
	).Scan(&o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" &&
			strings.Contains(pqErr.Constraint, "idempotency") {
			return domain.NewConflictError("duplicate idempotency key")
		}
		return domain.NewInternalError(fmt.Errorf("insert order: %w", err))
	}

	for i := range o.Lines {
		l := &o.Lines[i]
		l.OrderID = o.ID
		l.Position = i + 1
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO order_lines (
				id, order_id, position, item_id, product_code, name,
				variation_id, variation_name, quantity, unit_price, line_total
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			l.ID, l.OrderID, l.Position, l.ItemID, l.ProductCode, l.Name,
			l.VariationID, l.VariationName, l.Quantity, l.UnitPrice, l.LineTotal,
		); err != nil {
			return domain.NewInternalError(fmt.Errorf("insert order line: %w", err))
		}
	}

	if err := tx.Commit(); err != nil {
		return domain.NewInternalError(fmt.Errorf("commit: %w", err))
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// READ
// ═══════════════════════════════════════════════════════════════════

func (r *orderRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	var o domain.Order
	err := r.db.GetContext(ctx, &o, `SELECT `+orderColumns+` FROM orders WHERE id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewNotFoundError("order not found")
	}
	if err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("get order: %w", err))
	}
	if err := r.attachLines(ctx, []*domain.Order{&o}); err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *orderRepo) GetByIdempotencyKey(ctx context.Context, restaurantID uuid.UUID, key string) (*domain.Order, error) {
	var o domain.Order
	err := r.db.GetContext(ctx, &o,
		`SELECT `+orderColumns+` FROM orders WHERE restaurant_id = $1 AND idempotency_key = $2`,
		restaurantID, key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewNotFoundError("order not found")
	}
	if err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("get order by key: %w", err))
	}
	if err := r.attachLines(ctx, []*domain.Order{&o}); err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *orderRepo) ListByStatus(ctx context.Context, restaurantID uuid.UUID, statuses []domain.Status) ([]*domain.Order, error) {
	var orders []*domain.Order
	err := r.db.SelectContext(ctx, &orders, `
		SELECT `+orderColumns+`
		FROM orders
		WHERE restaurant_id = $1 AND status = ANY($2)
		ORDER BY created_at ASC`,
		restaurantID, pq.Array(statusStrings(statuses)),
	)
	if err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("list orders by status: %w", err))
	}
	if err := r.attachLines(ctx, orders); err != nil {
		return nil, err
	}
	return orders, nil
}

func (r *orderRepo) List(ctx context.Context, f domain.OrderFilter) ([]*domain.Order, int, error) {
	where, args := buildWhere(f)

	var total int
	if err := r.db.GetContext(ctx, &total, `SELECT COUNT(*) FROM orders`+where, args...); err != nil {
		return nil, 0, domain.NewInternalError(fmt.Errorf("count orders: %w", err))
	}

	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	args = append(args, limit, max(f.Offset, 0))

	var orders []*domain.Order
	query := fmt.Sprintf(`SELECT %s FROM orders%s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
		orderColumns, where, len(args)-1, len(args))
	if err := r.db.SelectContext(ctx, &orders, query, args...); err != nil {
		return nil, 0, domain.NewInternalError(fmt.Errorf("list orders: %w", err))
	}
	if err := r.attachLines(ctx, orders); err != nil {
		return nil, 0, err
	}
	return orders, total, nil
}

// buildWhere turns a filter into a WHERE clause with positional args.
func buildWhere(f domain.OrderFilter) (string, []any) {
	var conds []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}

	if len(f.RestaurantIDs) > 0 {
		add("restaurant_id = ANY($%d)", pq.Array(uuidStrings(f.RestaurantIDs)))
	}
	if f.Status != "" {
		add("status = $%d", f.Status)
	}
	if f.Channel != "" {
		add("channel = $%d", f.Channel)
	}
	if f.Number > 0 {
		add("number = $%d", f.Number)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}

	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// attachLines loads lines for all orders in a single query.
func (r *orderRepo) attachLines(ctx context.Context, orders []*domain.Order) error {
	if len(orders) == 0 {
		return nil
	}
	ids := make([]string, len(orders))
	byID := make(map[uuid.UUID]*domain.Order, len(orders))
	for i, o := range orders {
		ids[i] = o.ID.String()
		byID[o.ID] = o
		o.Lines = []domain.OrderLine{}
	}

	var lines []domain.OrderLine
	err := r.db.SelectContext(ctx, &lines, `
		SELECT id, order_id, position, item_id, product_code, name,
		       variation_id, variation_name, quantity, unit_price, line_total
		FROM order_lines
		WHERE order_id = ANY($1)
		ORDER BY order_id, position`,
		pq.Array(ids),
	)
	if err != nil {
		return domain.NewInternalError(fmt.Errorf("load order lines: %w", err))
	}
	for _, l := range lines {
		if o, ok := byID[l.OrderID]; ok {
			o.Lines = append(o.Lines, l)
		}
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// UPDATE
// ═══════════════════════════════════════════════════════════════════

func (r *orderRepo) UpdateStatus(
	ctx context.Context,
	id uuid.UUID,
	from, to domain.Status,
	reason string,
) (*domain.Order, error) {
	// Each status stamps its own timestamp. Moving backwards (recall)
	// clears the later timestamps so prep-time stats stay honest.
	var set string
	switch to {
	case domain.StatusAccepted:
		set = "preparing_at = NULL, ready_at = NULL"
	case domain.StatusPreparing:
		set = "preparing_at = COALESCE(preparing_at, NOW()), ready_at = NULL"
	case domain.StatusReady:
		set = "ready_at = NOW(), preparing_at = COALESCE(preparing_at, NOW())"
	case domain.StatusCompleted:
		set = "completed_at = NOW()"
	case domain.StatusCancelled:
		set = "cancelled_at = NOW(), cancel_reason = NULLIF($4, '')"
	default:
		return nil, domain.NewValidationError("invalid status")
	}

	args := []any{id, from, to}
	if to == domain.StatusCancelled {
		args = append(args, reason)
	}

	res, err := r.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE orders
		SET status = $3, updated_at = NOW(), %s
		WHERE id = $1 AND status = $2`, set), args...)
	if err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("update order status: %w", err))
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, domain.NewConflictError("order status changed by someone else; refresh and try again")
	}
	return r.GetByID(ctx, id)
}

// ═══════════════════════════════════════════════════════════════════
// STATS
// ═══════════════════════════════════════════════════════════════════

func (r *orderRepo) Stats(
	ctx context.Context,
	restaurantIDs []uuid.UUID,
	from, to time.Time,
	loc *time.Location,
) (*domain.OrderStats, error) {
	where, args := buildWhere(domain.OrderFilter{RestaurantIDs: restaurantIDs, From: &from, To: &to})
	stats := &domain.OrderStats{}

	var totals struct {
		Orders    int   `db:"orders"`
		Revenue   int64 `db:"revenue"`
		Cancelled int   `db:"cancelled"`
		Active    int   `db:"active"`
		AvgPrep   int   `db:"avg_prep"`
	}
	err := r.db.GetContext(ctx, &totals, `
		SELECT
			COUNT(*) FILTER (WHERE status <> 'cancelled')                         AS orders,
			COALESCE(SUM(total) FILTER (WHERE status <> 'cancelled'), 0)::BIGINT  AS revenue,
			COUNT(*) FILTER (WHERE status = 'cancelled')                          AS cancelled,
			COUNT(*) FILTER (WHERE status IN ('accepted','preparing','ready'))    AS active,
			COALESCE(AVG(EXTRACT(EPOCH FROM ready_at - created_at))
				FILTER (WHERE ready_at IS NOT NULL), 0)::INT                       AS avg_prep
		FROM orders`+where, args...)
	if err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("stats totals: %w", err))
	}
	stats.TotalOrders = totals.Orders
	stats.Revenue = totals.Revenue
	stats.CancelledOrders = totals.Cancelled
	stats.ActiveOrders = totals.Active
	stats.AvgPrepSeconds = totals.AvgPrep
	if totals.Orders > 0 {
		stats.AverageTicket = totals.Revenue / int64(totals.Orders)
	}

	notCancelled := where + " AND status <> 'cancelled'"

	if err := r.db.SelectContext(ctx, &stats.ByChannel, `
		SELECT channel, COUNT(*) AS orders, COALESCE(SUM(total), 0)::BIGINT AS revenue
		FROM orders`+notCancelled+`
		GROUP BY channel ORDER BY orders DESC`, args...); err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("stats by channel: %w", err))
	}

	hourArgs := append(append([]any{}, args...), loc.String())
	if err := r.db.SelectContext(ctx, &stats.ByHour, fmt.Sprintf(`
		SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE $%d)::INT AS hour,
		       COUNT(*) AS orders, COALESCE(SUM(total), 0)::BIGINT AS revenue
		FROM orders%s
		GROUP BY hour ORDER BY hour`, len(hourArgs), notCancelled), hourArgs...); err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("stats by hour: %w", err))
	}

	if err := r.db.SelectContext(ctx, &stats.TopItems, `
		SELECT l.item_id, MIN(l.name) AS name,
		       SUM(l.quantity)::INT AS quantity, SUM(l.line_total)::BIGINT AS revenue
		FROM order_lines l
		JOIN (SELECT id FROM orders`+notCancelled+`) o ON o.id = l.order_id
		GROUP BY l.item_id
		ORDER BY quantity DESC
		LIMIT 10`, args...); err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("stats top items: %w", err))
	}

	if err := r.db.SelectContext(ctx, &stats.ByStatus, `
		SELECT status, COUNT(*) AS orders
		FROM orders`+where+`
		GROUP BY status ORDER BY orders DESC`, args...); err != nil {
		return nil, domain.NewInternalError(fmt.Errorf("stats by status: %w", err))
	}

	return stats, nil
}

// ═══════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════

func statusStrings(ss []domain.Status) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}
