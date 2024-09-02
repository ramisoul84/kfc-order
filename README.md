# kfc-order

Order service for the KFC platform. Owns every order from the moment it
is paid until it is handed to the customer.

## What it does

- **Creates orders** from kiosks and the website (via `kfc-userapi`).
  Prices arrive already resolved; this service stores them, it never
  re-prices.
- **Numbers orders** per restaurant per business day (1, 2, 3, …),
  restarting at local midnight (`APP_TIMEZONE`). This is the number the
  customer sees on the receipt and on the hall display.
- **Runs the kitchen state machine** driven by the KDS.
- **Publishes every change** to Kafka (`orders.events`) with the full
  order, so screens update without polling.
- **Serves CRM** with filtered order history and dashboard stats.

## Order lifecycle

```
              ┌──────────── cancel ─────────────┐
              │                                 ▼
 [kiosk/web]  accepted ──▶ preparing ──▶ ready ──▶ completed
                  │  ◀── undo ──┘  ◀─ recall ─┘
                  └────────── ready (no-cook items) ──▶
```

| From        | Allowed next                              |
|-------------|-------------------------------------------|
| `accepted`  | `preparing`, `ready`, `cancelled`         |
| `preparing` | `ready`, `accepted`, `cancelled`          |
| `ready`     | `completed`, `preparing` (recall)         |
| `completed` | —                                         |
| `cancelled` | — (reason required)                       |

Transitions use compare-and-set (`WHERE status = $from`), so two cooks
bumping the same ticket can't both win; the loser gets
`FailedPrecondition` and the KDS refreshes.

## Architecture

```
kfc-userapi ──gRPC──▶┌──────────────┐──▶ Postgres (kfc_orders)
kfc-crm     ──gRPC──▶│  kfc-order   │
                     └──────┬───────┘
                            │ Kafka: orders.events (key = restaurant_id)
                            ▼
                     kfc-userapi → SSE → KDS, hall display, customer tracking
```

## Key design decisions

- **Money in kopecks** (`BIGINT`) everywhere internally; rubles only in
  the JSON wire view.
- **Idempotency keys** — kiosks send a UUID per checkout. A retry after a
  network blip returns the original order instead of charging twice.
- **Events carry the full order** — consumers never need a read-back.
  Events are keyed by restaurant and hash-partitioned, so per-restaurant
  order is preserved.
- **Embedded migrations** — `migrations/*.sql` are compiled in and applied
  at startup under an advisory lock (`DB_AUTO_MIGRATE=true`).

## gRPC API (`proto/order/v1/order.proto`)

| RPC                 | Used by            |
|---------------------|--------------------|
| `CreateOrder`       | userapi (kiosk, web) |
| `GetOrder`          | userapi (tracking), CRM |
| `ListActiveOrders`  | userapi (KDS, hall display) |
| `UpdateOrderStatus` | userapi (KDS), CRM (cancel) |
| `ListOrders`        | CRM                |
| `GetOrderStats`     | CRM dashboard      |

## Running

```bash
cp .env.example .env        # point DB_* at your Postgres
make run                    # gRPC :9103, health :8003
make test
```

With the rest of the platform: see `kfc-infra`.

## Ports

| Port | Purpose        |
|------|----------------|
| 9103 | gRPC           |
| 8003 | `GET /health`  |
