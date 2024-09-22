// Package outbox implements the transactional outbox pattern.
//
// Instead of publishing to Kafka after a database commit (and losing the
// event if Kafka is down or the process dies in between), a service writes
// the event into the `outbox` table in the same transaction as the change
// itself. A Relay then publishes pending rows to Kafka in insertion order
// and marks them sent. Delivery is at-least-once: consumers must be
// idempotent (ours are).
//
// Table (create it in a migration):
//
//	CREATE TABLE outbox (
//	    id           BIGSERIAL PRIMARY KEY,
//	    topic        TEXT        NOT NULL,
//	    key          TEXT        NOT NULL,
//	    payload      JSONB       NOT NULL,
//	    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	    published_at TIMESTAMPTZ,
//	    attempts     INT         NOT NULL DEFAULT 0,
//	    last_error   TEXT
//	);
//	CREATE INDEX idx_outbox_pending ON outbox (id) WHERE published_at IS NULL;
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	"github.com/ramisoul84/kfc-order/pkg/logger"
)

// Insert queues an event inside the caller's transaction.
func Insert(ctx context.Context, tx sqlx.ExecerContext, topic, key string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("outbox: marshal: %w", err)
	}
	if key == "" {
		key = "all"
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`, topic, key, raw); err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

// Writer is the Kafka side; *kafka.Writer satisfies it.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Relay publishes pending outbox rows.
type Relay struct {
	db       *sqlx.DB
	writer   Writer
	logger   *logger.Logger
	lockID   int64
	interval time.Duration
	batch    int
	notify   chan struct{}
}

// NewRelay builds a relay. lockID must be unique per service database:
// it elects a single active relay across replicas, which keeps events in
// insertion order.
func NewRelay(db *sqlx.DB, writer Writer, lockID int64, log *logger.Logger) *Relay {
	return &Relay{
		db:       db,
		writer:   writer,
		logger:   log,
		lockID:   lockID,
		interval: time.Second,
		batch:    100,
		notify:   make(chan struct{}, 1),
	}
}

// Notify wakes the relay right after a commit instead of waiting for the
// next poll, so events go out within milliseconds.
func (r *Relay) Notify() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	cleanup := time.NewTicker(time.Hour)
	defer cleanup.Stop()
	backoff := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanup.C:
			r.purge(ctx)
			continue
		case <-ticker.C:
		case <-r.notify:
		}
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}

		// Drain: keep going while full batches come back.
		for {
			n, err := r.publishBatch(ctx)
			if err != nil {
				backoff = min(max(backoff*2, time.Second), 30*time.Second)
				r.logger.Warn("outbox relay: publish failed, will retry", "error", err, "backoff", backoff)
				break
			}
			backoff = 0
			if n < r.batch {
				break
			}
		}
	}
}

type row struct {
	ID      int64           `db:"id"`
	Topic   string          `db:"topic"`
	Key     string          `db:"key"`
	Payload json.RawMessage `db:"payload"`
}

// publishBatch sends up to r.batch pending rows in id order. Returns how
// many were published.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	// Only one replica relays at a time (released at commit/rollback).
	var leader bool
	if err := tx.GetContext(ctx, &leader, `SELECT pg_try_advisory_xact_lock($1)`, r.lockID); err != nil {
		return 0, err
	}
	if !leader {
		return 0, nil
	}

	var rows []row
	if err := tx.SelectContext(ctx, &rows, `
		SELECT id, topic, key, payload FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1`, r.batch); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	msgs := make([]kafka.Message, len(rows))
	ids := make([]int64, len(rows))
	for i, rw := range rows {
		msgs[i] = kafka.Message{Topic: rw.Topic, Key: []byte(rw.Key), Value: rw.Payload}
		ids[i] = rw.ID
	}

	if err := r.writer.WriteMessages(ctx, msgs...); err != nil {
		// Record the failure on the oldest row for visibility, outside
		// this transaction so the note survives the rollback.
		_, _ = r.db.ExecContext(ctx,
			`UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`, ids[0], err.Error())
		return 0, err
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE outbox SET published_at = NOW() WHERE id = ANY($1)`, pq.Array(ids)); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		// Kafka has the messages but we couldn't mark them: they will be
		// sent again. At-least-once; consumers are idempotent.
		return 0, err
	}
	r.logger.Debug("outbox relay: published", "count", len(rows))
	return len(rows), nil
}

// purge drops published rows after a few days; they're only kept for
// troubleshooting.
func (r *Relay) purge(ctx context.Context) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM outbox WHERE published_at < NOW() - INTERVAL '3 days'`)
	if err != nil {
		r.logger.Warn("outbox relay: purge failed", "error", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		r.logger.Info("outbox relay: purged published events", "count", n)
	}
}

// Pending reports how many events are waiting, for health and metrics.
func Pending(ctx context.Context, db *sqlx.DB) (int, error) {
	var n int
	err := db.GetContext(ctx, &n, `SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`)
	return n, err
}
