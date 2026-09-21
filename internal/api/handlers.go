// Package api implements leasework's job submission HTTP API: POST /jobs to
// accept new work and GET /jobs/{id} to look it up.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jianrontan/leasework/internal/metrics"
	"github.com/jianrontan/leasework/internal/store"
)

// maxSubmitBodyBytes bounds the size of a POST /jobs request body. It
// exists to stop an oversized payload from tying up a handler goroutine or
// landing an arbitrarily large row in Postgres.
const maxSubmitBodyBytes = 64 * 1024

// maxJobTypeLen bounds the length of the "type" field on a submitted job.
const maxJobTypeLen = 128

// jobsReadyTopic is the outbox topic job-submission messages are written
// with. The scheduler's relay loop republishes outbox rows onto the Kafka
// topic of the same name.
const jobsReadyTopic = "jobs.ready"

// jobStore is the subset of *store.Store the handlers need. Depending on
// this interface, rather than the concrete *store.Store type, lets tests
// substitute a fake without a live Postgres connection.
type jobStore interface {
	EnqueueJob(ctx context.Context, jobType string, payload json.RawMessage, topic string) (store.Job, error)
	GetJob(ctx context.Context, id string) (store.Job, error)
}

// Handler serves leasework's job submission HTTP API.
type Handler struct {
	store  jobStore
	logger *slog.Logger
}

// NewHandler returns a Handler that reads and writes jobs through store,
// logging through logger.
func NewHandler(store jobStore, logger *slog.Logger) *Handler {
	return &Handler{store: store, logger: logger}
}

// Register mounts the job API routes on mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /jobs", h.handleSubmit)
	mux.HandleFunc("GET /jobs/{id}", h.handleGet)
}

// submitRequest is the POST /jobs request body.
type submitRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// submitResponse is the POST /jobs response body.
type submitResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// handleSubmit accepts a new job, writing the job row and its outbox
// message in one transaction, and returns 202 Accepted: the work has been
// durably recorded for later execution, not performed yet.
func (h *Handler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSubmitBodyBytes)

	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if len(req.Type) > maxJobTypeLen {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("type must be at most %d characters", maxJobTypeLen))
		return
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	job, err := h.store.EnqueueJob(r.Context(), req.Type, payload, jobsReadyTopic)
	if err != nil {
		h.logger.Error("enqueueing job", "error", err, "job_type", req.Type)
		writeError(w, http.StatusInternalServerError, "failed to submit job")
		return
	}

	metrics.JobsSubmitted.Inc()
	writeJSON(w, http.StatusAccepted, submitResponse{JobID: job.ID, Status: job.Status})
}

// handleGet returns the job identified by the {id} path value, or 404 if
// it does not exist.
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, err := h.store.GetJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		h.logger.Error("getting job", "error", err, "job_id", id)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// writeJSON writes v as a JSON response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a JSON error body of the form {"error": message} with
// the given status code. It never includes raw underlying error text: only
// the caller-supplied, deliberately safe message.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
