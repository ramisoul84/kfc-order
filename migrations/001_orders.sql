CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Per-restaurant daily order number. The row for (restaurant, day) is
-- upserted and incremented in the same statement, so concurrent kiosks
-- never get the same number.
CREATE TABLE IF NOT EXISTS order_counters (
    restaurant_id UUID NOT NULL,
    business_date DATE NOT NULL,
    last_number   INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (restaurant_id, business_date)
);

CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    restaurant_id   UUID         NOT NULL,
    number          INT          NOT NULL,
    business_date   DATE         NOT NULL,
    channel         VARCHAR(20)  NOT NULL,
    order_type      VARCHAR(20)  NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    payment_method  VARCHAR(20)  NOT NULL,
    payment_status  VARCHAR(20)  NOT NULL,
    customer_name   VARCHAR(100),
    customer_phone  VARCHAR(30),
    note            VARCHAR(500),
    device_id       UUID,
    subtotal        BIGINT       NOT NULL,
    total           BIGINT       NOT NULL,
    currency        VARCHAR(3)   NOT NULL DEFAULT 'RUB',
    cancel_reason   VARCHAR(500),
    idempotency_key VARCHAR(100),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    preparing_at    TIMESTAMPTZ,
    ready_at        TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,

    CONSTRAINT chk_orders_channel CHECK (channel IN ('kiosk', 'web', 'pos')),
    CONSTRAINT chk_orders_type    CHECK (order_type IN ('dine_in', 'takeaway')),
    CONSTRAINT chk_orders_status  CHECK (
        status IN ('accepted', 'preparing', 'ready', 'completed', 'cancelled')
    ),
    CONSTRAINT chk_orders_payment_method CHECK (payment_method IN ('card', 'cash', 'online')),
    CONSTRAINT chk_orders_payment_status CHECK (payment_status IN ('paid', 'unpaid')),
    CONSTRAINT uq_orders_number UNIQUE (restaurant_id, business_date, number)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_idempotency
    ON orders (restaurant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- KDS / display: "everything still in flight at this restaurant".
CREATE INDEX IF NOT EXISTS idx_orders_active
    ON orders (restaurant_id, created_at)
    WHERE status IN ('accepted', 'preparing', 'ready');

-- CRM listing and stats.
CREATE INDEX IF NOT EXISTS idx_orders_restaurant_created
    ON orders (restaurant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_created
    ON orders (created_at DESC);

CREATE TABLE IF NOT EXISTS order_lines (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       UUID         NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    position       INT          NOT NULL,
    item_id        UUID         NOT NULL,
    product_code   VARCHAR(50)  NOT NULL,
    name           VARCHAR(200) NOT NULL,
    variation_id   UUID,
    variation_name VARCHAR(100),
    quantity       INT          NOT NULL CHECK (quantity > 0),
    unit_price     BIGINT       NOT NULL CHECK (unit_price >= 0),
    line_total     BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_lines_order
    ON order_lines (order_id, position);

CREATE INDEX IF NOT EXISTS idx_order_lines_item
    ON order_lines (item_id);
