// Package kafka provides leasework's Kafka producer and consumer, built on
// franz-go (pure Go, no cgo, which matters because the alternative,
// confluent-kafka-go, needs librdkafka).
package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes records to Kafka on behalf of the scheduler's outbox
// relay.
type Producer struct {
	client *kgo.Client
	logger *slog.Logger
}

// NewProducer creates a Producer that will publish to the Kafka cluster
// reachable at brokers. Client construction does not dial: connections are
// established lazily on first use, so this only fails on invalid
// configuration (for example, an empty broker list).
func NewProducer(brokers []string, logger *slog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Acks from all in-sync replicas, not just the partition leader.
		// This is what makes the cluster's RF=3, min-ISR=2 configuration
		// actually mean something: with leader-only acks a message could be
		// acknowledged, the leader could then crash before replicating it,
		// and the message would be gone despite the "successful" produce.
		// AllISRAcks is the only setting under which a successful Produce
		// implies the record survived a broker failure.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent production is franz-go's default and is left enabled
		// (no kgo.DisableIdempotentWrite call here). Paired with
		// RequiredAcks(AllISRAcks()), it stops the producer's own retries
		// on a transient error from writing the same record twice into a
		// partition.
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer client: %w", err)
	}

	return &Producer{client: client, logger: logger}, nil
}

// Produce writes value to topic under key and blocks until Kafka has
// acknowledged the record, returning any error from that acknowledgement.
//
// This must stay synchronous. The outbox relay calls MarkPublished only
// after Produce returns nil: if Produce instead fired the send and returned
// immediately, a record that ultimately failed to reach Kafka (or never
// reached the required in-sync replicas) could still get marked published,
// and the relay would never retry it. That is a silently lost job, which is
// precisely the failure the transactional outbox exists to prevent. Every
// caller of Produce depends on its error return being trustworthy.
func (p *Producer) Produce(ctx context.Context, topic, key string, value []byte) error {
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}

	result := p.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("producing record to topic %s: %w", topic, err)
	}
	return nil
}

// Ping reports whether the Kafka cluster is currently reachable.
func (p *Producer) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("pinging kafka: %w", err)
	}
	return nil
}

// Close releases the underlying Kafka client and its connections.
func (p *Producer) Close() {
	p.client.Close()
}
