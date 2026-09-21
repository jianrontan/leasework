package kafka

import (
	"io"
	"log/slog"
	"testing"
)

// testLogger returns a logger that discards output, so tests don't print
// noise but still exercise the same logger.Error/Info call sites the real
// binaries use.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestNewProducer_RequiresSeedBrokers checks that construction fails fast
// on an empty broker list rather than silently defaulting somewhere. This
// is the one piece of NewProducer that is checkable without a broker:
// kgo.NewClient validates its options synchronously and does not dial until
// first use, so a nil/empty seed list is a pure configuration error.
func TestNewProducer_RequiresSeedBrokers(t *testing.T) {
	p, err := NewProducer(nil, testLogger())
	if err == nil {
		if p != nil {
			p.Close()
		}
		t.Fatal("expected an error constructing a producer with no seed brokers, got nil")
	}
}

// TestNewProducer_Succeeds checks that a valid broker list is accepted at
// construction time. franz-go clients connect lazily, so this exercises
// option assembly (SeedBrokers, RequiredAcks) without needing a live
// broker.
func TestNewProducer_Succeeds(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"}, testLogger())
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	if p.client == nil {
		t.Fatal("expected a non-nil underlying client")
	}
}

// TestNewConsumer_RequiresSeedBrokers mirrors TestNewProducer_RequiresSeedBrokers
// for the consumer's client construction.
func TestNewConsumer_RequiresSeedBrokers(t *testing.T) {
	c, err := NewConsumer(nil, "leasework-workers", []string{"jobs.ready"}, testLogger())
	if err == nil {
		if c != nil {
			c.Close()
		}
		t.Fatal("expected an error constructing a consumer with no seed brokers, got nil")
	}
}

// TestNewConsumer_Succeeds checks that a valid group and topic list are
// accepted at construction time, exercising option assembly (SeedBrokers,
// ConsumerGroup, ConsumeTopics, DisableAutoCommit) without a live broker.
func TestNewConsumer_Succeeds(t *testing.T) {
	c, err := NewConsumer([]string{"localhost:9092"}, "leasework-workers", []string{"jobs.ready"}, testLogger())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close()

	if c.client == nil {
		t.Fatal("expected a non-nil underlying client")
	}
}
