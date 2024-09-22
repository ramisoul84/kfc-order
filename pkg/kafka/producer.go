package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ramisoul84/kfc-order/pkg/logger"
)

// Producer publishes JSON messages to Kafka topics.
type Producer struct {
	writer *kafka.Writer
	logger *logger.Logger
}

// NewProducer creates a Kafka producer pointed at the given brokers.
func NewProducer(brokers []string, log *logger.Logger) *Producer {
	w := &kafka.Writer{
		Addr: kafka.TCP(brokers...),
		// Hash on the message key so every event for one restaurant lands
		// on the same partition and consumers see them in order.
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		WriteBackoffMin:        100 * time.Millisecond,
		WriteBackoffMax:        1 * time.Second,
		MaxAttempts:            5,
	}

	return &Producer{
		writer: w,
		logger: log,
	}
}

// Publish sends a payload as JSON to the given topic with the given key.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal kafka payload: %w", err)
	}

	// Global events (empty key) would otherwise round-robin across
	// partitions. Using "all" keeps them on one partition so ordering
	// is preserved, which matters for invalidation semantics.
	if key == "" {
		key = "all"
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
		Time:  time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}

	p.logger.Debug("kafka published",
		"topic", topic,
		"key", key,
		"bytes", len(data),
	)
	return nil
}

// WriteMessages sends pre-encoded messages in one batch, in order. Used
// by the outbox relay.
func (p *Producer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, msgs...)
}

// Close flushes pending writes and closes the writer.
// Safe to call more than once.
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
