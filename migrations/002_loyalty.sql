-- Loyalty: who ordered (if signed in) and points used as a discount.
-- total = subtotal - discount.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS member_id       UUID;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount        BIGINT NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS points_redeemed INT    NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS loyalty_hold_id UUID;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS chk_orders_discount;
ALTER TABLE orders ADD CONSTRAINT chk_orders_discount
    CHECK (discount >= 0 AND discount <= subtotal AND total = subtotal - discount);

-- "My orders" on the website.
CREATE INDEX IF NOT EXISTS idx_orders_member
    ON orders (member_id, created_at DESC)
    WHERE member_id IS NOT NULL;
