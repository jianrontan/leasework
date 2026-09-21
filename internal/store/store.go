// Package store provides leasework's Postgres persistence layer: the jobs
// table, the transactional outbox that hands accepted work to the relay,
// and the embedded schema migrations that create both.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/jianrontan/leasework/internal/store/migrations"
)

// Job status values. Phase 1 only ever writes StatusPending on submission;
// the remaining values exist so callers have a stable vocabulary to compare
// against as later phases start setting them.
const (
	StatusPending   = "pending"
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

// ErrJobNotFound is returned by GetJob when no job with the given id
// exists.
var ErrJobNotFound = errors.New("job not found")

// Job is a row of the jobs table.
type Job struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// OutboxMessage is a row of the outbox table: a job-submission event
// waiting to be relayed onto Kafka.
type OutboxMessage struct {
	ID      int64           `json:"id"`
	JobID   string          `json:"job_id"`
	Topic   string          `json:"topic"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

// Store is leasework's Postgres persistence layer, backed by a connection
// pool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a Store connected to the Postgres instance addressed by dsn.
// It pings the database before returning so callers learn about a bad DSN
// or an unreachable database immediately rather than on first use.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating postgres connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close releases the underlying connection pool. It does not wait for
// in-flight queries beyond what pgxpool itself does.
func (s *Store) Close() {
	s.pool.Close()
}

// Ping reports whether the database is currently reachable.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}
	return nil
}

// Migrate applies all pending goose migrations embedded in the migrations
// package, bringing the schema up to date.
func (s *Store) Migrate(ctx context.Context) error {
	// goose only accepts a database/sql handle. stdlib.OpenDBFromPool wraps
	// the existing pgxpool so migrations run over the same pool instead of
	// opening a second connection; closing the wrapper does not close the
	// pool underneath it.
	db := stdlib.OpenDBFromPool(s.pool)
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("setting goose dialect: %w", err)
	}
	goose.SetBaseFS(migrations.FS)
	// Route goose's own progress output through nothing rather than letting
	// it print directly to stdout, which would break the JSON-only log
	// stream every other part of this service produces.
	goose.SetLogger(goose.NopLogger())

	if err := goose.UpContext(ctx, db, "."); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}
	return nil
}

// EnqueueJob records a new job and its outbox message in a single
// transaction: either both are durable or neither is, so the relay can
// never observe an outbox row whose job does not exist. The generated job
// id also becomes the outbox message key, keeping all events for a job on
// one Kafka partition.
func (s *Store) EnqueueJob(ctx context.Context, jobType string, payload json.RawMessage, topic string) (Job, error) {
	if payload == nil {
		payload = json.RawMessage("{}")
	}

	id := uuid.NewString()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("beginning enqueue transaction: %w", err)
	}
	// A no-op after a successful Commit, so this is safe alongside every
	// early return below; it exists to catch the ones that aren't obviously
	// errors, such as a panic unwinding the stack.
	defer tx.Rollback(ctx)

	var job Job
	err = tx.QueryRow(ctx,
		`INSERT INTO jobs (id, type, payload, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, type, payload, status, created_at, updated_at`,
		id, jobType, payload, StatusPending,
	).Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return Job{}, fmt.Errorf("inserting job: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (job_id, topic, key, payload) VALUES ($1, $2, $3, $4)`,
		id, topic, id, payload,
	); err != nil {
		return Job{}, fmt.Errorf("inserting outbox message: %w", err)
	}

	// This notification is an optimisation only: it is delivered at most
	// once and is not durable, so a relay that is down or mid-restart will
	// simply miss it. The relay's poll loop is what guarantees delivery;
	// this just lets it wake immediately instead of waiting for the next
	// poll tick, so correctness never depends on the notification arriving.
	if _, err := tx.Exec(ctx, `SELECT pg_notify('outbox_new', '')`); err != nil {
		return Job{}, fmt.Errorf("notifying outbox_new: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("committing enqueue transaction: %w", err)
	}

	return job, nil
}

// GetJob returns the job with the given id, or ErrJobNotFound if no such
// job exists.
func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	var job Job
	err := s.pool.QueryRow(ctx,
		`SELECT id, type, payload, status, created_at, updated_at FROM jobs WHERE id = $1`,
		id,
	).Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, fmt.Errorf("getting job %s: %w", id, ErrJobNotFound)
		}
		return Job{}, fmt.Errorf("getting job %s: %w", id, err)
	}
	return job, nil
}

// FetchUnpublished returns up to limit outbox messages that have not yet
// been published, oldest first, for the relay to hand to Kafka.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, job_id, topic, key, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetching unpublished outbox messages: %w", err)
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.JobID, &m.Topic, &m.Key, &m.Payload); err != nil {
			return nil, fmt.Errorf("scanning outbox message: %w", err)
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating unpublished outbox messages: %w", err)
	}

	return messages, nil
}

// MarkPublished records that the outbox message with the given id has been
// handed to Kafka, so the relay does not fetch it again.
func (s *Store) MarkPublished(ctx context.Context, outboxID int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = $1`,
		outboxID,
	); err != nil {
		return fmt.Errorf("marking outbox message %d published: %w", outboxID, err)
	}
	return nil
}

// SetJobStatus updates the status of the job with the given id. It returns
// ErrJobNotFound if no job with that id exists.
func (s *Store) SetJobStatus(ctx context.Context, jobID string, status string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = $1, updated_at = now() WHERE id = $2`,
		status, jobID,
	)
	if err != nil {
		return fmt.Errorf("setting job %s status to %s: %w", jobID, status, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("setting job %s status to %s: %w", jobID, status, ErrJobNotFound)
	}
	return nil
}

// WaitForOutboxNotification blocks until an outbox_new notification arrives
// on a dedicated LISTEN connection, or until ctx is done. It exists only to
// let the relay loop wake up promptly instead of waiting out a full poll
// tick; the notification itself is not durable (delivered at most once, to
// whichever listener happens to be connected when it fires), so the relay's
// poll loop remains the sole source of correctness. Any error here,
// including ctx cancellation, is meant to be treated by the caller as
// nothing more than "fall back to the next poll tick": this method must
// never be the thing that makes the relay give up.
func (s *Store) WaitForOutboxNotification(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection to listen for outbox_new: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN outbox_new"); err != nil {
		return fmt.Errorf("listening for outbox_new: %w", err)
	}

	if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
		return fmt.Errorf("waiting for outbox_new notification: %w", err)
	}
	return nil
}
