package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// maxPollRecords bounds how many records a single PollRecords call can hand
// back, so one poll can never return an unbounded batch that ties up the
// run loop or blows up memory.
const maxPollRecords = 100

// Consumer reads records from a Kafka consumer group on behalf of the
// worker.
type Consumer struct {
	client *kgo.Client
	logger *slog.Logger
}

// NewConsumer creates a Consumer in the given group, subscribed to topics
// on the Kafka cluster reachable at brokers. Client construction does not
// dial: connections are established lazily on first use, so this only
// fails on invalid configuration (for example, an empty broker list).
func NewConsumer(brokers []string, group string, topics []string, logger *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		// This is the single most important line in this file. franz-go's
		// default is to commit offsets on a timer, independent of whether
		// the record at that offset has actually been handled. A crash
		// between a timer commit and the handler finishing its work would
		// then resume, on restart, past the very record that was never
		// processed: a silent loss. Disabling auto-commit makes offset
		// advancement wait on Run explicitly calling CommitRecords after a
		// successful handle, so the committed offset never lies about what
		// has actually been done.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer client: %w", err)
	}

	return &Consumer{client: client, logger: logger}, nil
}

// Ping reports whether the Kafka cluster is currently reachable.
func (c *Consumer) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx); err != nil {
		return fmt.Errorf("pinging kafka: %w", err)
	}
	return nil
}

// Close releases the underlying Kafka client and its connections.
func (c *Consumer) Close() {
	c.client.Close()
}

// Run polls for records and invokes handle for each one, in order,
// committing a record's offset only after handle returns nil for it. Work
// happens before the commit, never after, so a crash between the two
// causes redelivery on restart rather than loss: at-least-once delivery
// depends entirely on that ordering.
//
// If handle returns an error, Run returns it rather than continuing. That
// is deliberate and blunt. Skipping the record and polling on would leave
// its offset uncommitted but also leave it unprocessed for the life of the
// process: correct on restart, yet an invisible stall until then, which is
// the one failure class this system is built to avoid. Returning lets the
// worker exit non-zero and be restarted, resuming from the last committed
// offset and replaying the record. Phase 6 replaces this with retry and a
// dead-letter topic, which is what makes a genuinely poison record stop
// crash-looping the worker.
func (c *Consumer) Run(ctx context.Context, handle func(ctx context.Context, key, value []byte) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches := c.client.PollRecords(ctx, maxPollRecords)
		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.logger.Error("kafka fetch error",
					"topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			if err := handle(ctx, record.Key, record.Value); err != nil {
				return fmt.Errorf("handling record on topic %s partition %d offset %d: %w",
					record.Topic, record.Partition, record.Offset, err)
			}

			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("committing offset for topic %s partition %d: %w",
					record.Topic, record.Partition, err)
			}
		}
	}
}
