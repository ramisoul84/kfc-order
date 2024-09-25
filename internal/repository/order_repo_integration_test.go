package repository

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/ramisoul84/kfc-order/internal/domain"
	"github.com/ramisoul84/kfc-order/migrations"
	"github.com/ramisoul84/kfc-order/pkg/migrate"
)

// Integration tests against a real Postgres. Run with:
//
//	TEST_DATABASE_URL=postgres://user:pass@localhost:5432/kfc_orders_test?sslmode=disable go test ./...
func newTestRepo(t *testing.T) (OrderRepository, *sqlx.DB) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := migrate.Up(context.Background(), db, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.Exec(`TRUNCATE outbox, order_lines, orders, order_counters CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return NewOrderRepository(db, "orders.events", nil), db
}

func newOrder(restaurantID uuid.UUID) *domain.Order {
	return &domain.Order{
		ID:            uuid.New(),
		RestaurantID:  restaurantID,
		BusinessDate:  time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC),
		Channel:       domain.ChannelKiosk,
		OrderType:     domain.OrderTypeDineIn,
		Status:        domain.StatusAccepted,
		PaymentMethod: domain.PaymentCard,
		PaymentStatus: domain.PaymentPaid,
		Currency:      "RUB",
		Subtotal:      29900,
		Total:         29900,
		Lines: []domain.OrderLine{{
			ID: uuid.New(), ItemID: uuid.New(), ProductCode: "Z", Name: "Zinger",
			Quantity: 1, UnitPrice: 29900, LineTotal: 29900,
		}},
	}
}

// Twenty kiosks submitting at once must get numbers 1..20, each once.
func TestConcurrentOrdersGetUniqueSequentialNumbers(t *testing.T) {
	repo, _ := newTestRepo(t)
	restaurant := uuid.New()

	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[int]bool{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := newOrder(restaurant)
			if err := repo.Create(context.Background(), o); err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[o.Number] {
				t.Errorf("number %d issued twice", o.Number)
			}
			seen[o.Number] = true
		}()
	}
	wg.Wait()
	for n := 1; n <= 20; n++ {
		if !seen[n] {
			t.Fatalf("number %d missing: %v", n, seen)
		}
	}
}

// Every committed change has exactly one event in the outbox, written in
// the same transaction, carrying the order as it was committed.
func TestEventsAreWrittenWithTheChange(t *testing.T) {
	repo, db := newTestRepo(t)
	ctx := context.Background()
	o := newOrder(uuid.New())
	if err := repo.Create(ctx, o); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.UpdateStatus(ctx, o.ID, domain.StatusAccepted, domain.StatusPreparing, ""); err != nil {
		t.Fatal(err)
	}

	var payloads []json.RawMessage
	if err := db.Select(&payloads, `SELECT payload FROM outbox ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 {
		t.Fatalf("want 2 outbox events, got %d", len(payloads))
	}
	var last struct {
		Type  string `json:"type"`
		Order struct {
			Status string `json:"status"`
			Lines  []any  `json:"lines"`
		} `json:"order"`
	}
	_ = json.Unmarshal(payloads[1], &last)
	if last.Type != "order.status_changed" || last.Order.Status != "preparing" || len(last.Order.Lines) != 1 {
		t.Fatalf("unexpected event: %s", payloads[1])
	}
}

// Two screens bumping the same ticket: only one wins, one event is emitted.
func TestStatusChangeIsCompareAndSet(t *testing.T) {
	repo, db := newTestRepo(t)
	ctx := context.Background()
	o := newOrder(uuid.New())
	_ = repo.Create(ctx, o)

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, conflicts := 0, 0
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.UpdateStatus(ctx, o.ID, domain.StatusAccepted, domain.StatusPreparing, "")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case domain.IsConflictError(err):
				conflicts++
			default:
				t.Errorf("unexpected: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok != 1 || conflicts != 4 {
		t.Fatalf("ok=%d conflicts=%d, want 1 and 4", ok, conflicts)
	}
	var events int
	_ = db.Get(&events, `SELECT COUNT(*) FROM outbox WHERE payload->>'type' = 'order.status_changed'`)
	if events != 1 {
		t.Fatalf("want 1 status event, got %d", events)
	}
}

func TestDiscountMustMatchTotal(t *testing.T) {
	repo, _ := newTestRepo(t)
	o := newOrder(uuid.New())
	o.Discount = 5000 // total not reduced: the check constraint must reject it
	if err := repo.Create(context.Background(), o); err == nil {
		t.Fatal("inconsistent discount/total was stored")
	}
}
