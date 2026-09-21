package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jianrontan/leasework/internal/store"
)

// fakeStore is an in-memory jobStore double, letting handler tests run
// without a live Postgres connection.
type fakeStore struct {
	// enqueueErr, when set, makes EnqueueJob fail with this error.
	enqueueErr error
	// jobs is keyed by id, for GetJob to look up.
	jobs map[string]store.Job
	// enqueueCalls records every EnqueueJob invocation for assertions.
	enqueueCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: make(map[string]store.Job)}
}

func (f *fakeStore) EnqueueJob(_ context.Context, jobType string, payload json.RawMessage, _ string) (store.Job, error) {
	f.enqueueCalls++
	if f.enqueueErr != nil {
		return store.Job{}, f.enqueueErr
	}
	job := store.Job{
		ID:        "fake-job-id",
		Type:      jobType,
		Payload:   payload,
		Status:    store.StatusPending,
		CreatedAt: time.Unix(0, 0).UTC(),
		UpdatedAt: time.Unix(0, 0).UTC(),
	}
	f.jobs[job.ID] = job
	return job, nil
}

func (f *fakeStore) GetJob(_ context.Context, id string) (store.Job, error) {
	job, ok := f.jobs[id]
	if !ok {
		return store.Job{}, store.ErrJobNotFound
	}
	return job, nil
}

// newTestHandler returns a Handler backed by fs and a discard logger.
func newTestHandler(fs *fakeStore) *Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(fs, logger)
}

// doRequest drives an HTTP request through h's mux and returns the
// recorded response.
func doRequest(t *testing.T, h *Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	h.Register(mux)

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// decodeError decodes a {"error": "..."} response body.
func decodeError(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding error body: %v (body: %q)", err, rec.Body.String())
	}
	return body["error"]
}

// TestSubmitValid verifies a well-formed submission returns 202 with a
// non-empty job id and the pending status.
func TestSubmitValid(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type":"send-email","payload":{"to":"a@example.com"}}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp submitResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.JobID == "" {
		t.Error("job_id is empty, want a generated id")
	}
	if resp.Status != store.StatusPending {
		t.Errorf("status = %q, want %q", resp.Status, store.StatusPending)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
	if fs.enqueueCalls != 1 {
		t.Errorf("EnqueueJob called %d times, want 1", fs.enqueueCalls)
	}
}

// TestSubmitDefaultsEmptyPayload verifies that omitting payload entirely
// still succeeds, defaulting it to an empty object.
func TestSubmitDefaultsEmptyPayload(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type":"send-email"}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

// TestSubmitMissingType verifies a request with no "type" field is
// rejected with 400.
func TestSubmitMissingType(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"payload":{}}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if msg := decodeError(t, rec); msg == "" {
		t.Error("error body is empty, want an explanatory message")
	}
	if fs.enqueueCalls != 0 {
		t.Errorf("EnqueueJob called %d times, want 0", fs.enqueueCalls)
	}
}

// TestSubmitEmptyType verifies a request with an empty "type" string is
// rejected with 400, distinct from an absent field but handled the same
// way.
func TestSubmitEmptyType(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type":""}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if fs.enqueueCalls != 0 {
		t.Errorf("EnqueueJob called %d times, want 0", fs.enqueueCalls)
	}
}

// TestSubmitMalformedJSON verifies unparseable JSON is rejected with 400.
func TestSubmitMalformedJSON(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type":`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// TestSubmitOversizedBody verifies a body larger than maxSubmitBodyBytes is
// rejected. http.MaxBytesReader surfaces this as an *http.MaxBytesError,
// which the handler maps to 413; this test asserts that actual behaviour.
func TestSubmitOversizedBody(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	oversized := `{"type":"x","payload":{"pad":"` + strings.Repeat("a", maxSubmitBodyBytes+1) + `"}}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", oversized)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
	if fs.enqueueCalls != 0 {
		t.Errorf("EnqueueJob called %d times, want 0", fs.enqueueCalls)
	}
}

// TestSubmitStoreError verifies a store failure on submission returns 500
// and never leaks the underlying error text into the response body.
func TestSubmitStoreError(t *testing.T) {
	fs := newFakeStore()
	fs.enqueueErr = errors.New("pq: connection refused to internal-db-host:5432 user=leasework")
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type":"send-email"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	msg := decodeError(t, rec)
	if strings.Contains(msg, "internal-db-host") || strings.Contains(msg, "pq:") {
		t.Errorf("error body leaked underlying store error: %q", msg)
	}
	if msg == "" {
		t.Error("error body is empty, want a generic message")
	}
}

// TestGetJobFound verifies GET /jobs/{id} returns 200 with the job as
// JSON.
func TestGetJobFound(t *testing.T) {
	fs := newFakeStore()
	fs.jobs["abc-123"] = store.Job{
		ID:        "abc-123",
		Type:      "send-email",
		Payload:   json.RawMessage(`{}`),
		Status:    store.StatusPending,
		CreatedAt: time.Unix(0, 0).UTC(),
		UpdatedAt: time.Unix(0, 0).UTC(),
	}
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodGet, "/jobs/abc-123", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got store.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if got.ID != "abc-123" {
		t.Errorf("ID = %q, want %q", got.ID, "abc-123")
	}
	if got.Type != "send-email" {
		t.Errorf("Type = %q, want %q", got.Type, "send-email")
	}
}

// TestGetJobNotFound verifies GET /jobs/{id} returns 404 for an unknown
// id.
func TestGetJobNotFound(t *testing.T) {
	fs := newFakeStore()
	h := newTestHandler(fs)

	rec := doRequest(t, h, http.MethodGet, "/jobs/does-not-exist", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if msg := decodeError(t, rec); msg == "" {
		t.Error("error body is empty, want an explanatory message")
	}
}
