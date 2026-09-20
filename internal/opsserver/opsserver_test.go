package opsserver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestServer returns a Server backed by a discard logger, suitable for
// exercising the mux without binding a real network listener.
func newTestServer() *Server {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(":0", logger)
}

// doRequest drives method and path through srv's mux and returns the
// recorded response.
func doRequest(t *testing.T, srv *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)
	return rec
}

// assertResponse checks the recorded status code and exact trimmed body.
func assertResponse(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantBody string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Errorf("status = %d, want %d (body: %q)", rec.Code, wantStatus, rec.Body.String())
	}
	if got := rec.Body.String(); got != wantBody {
		t.Errorf("body = %q, want %q", got, wantBody)
	}
}

// TestReadyzTransitions verifies the readiness endpoint reflects
// SetReady's state across the full not-ready -> ready -> not-ready cycle,
// since graceful shutdown relies on the last transition.
func TestReadyzTransitions(t *testing.T) {
	srv := newTestServer()

	rec := doRequest(t, srv, http.MethodGet, "/readyz")
	assertResponse(t, rec, http.StatusServiceUnavailable, "not ready")

	srv.SetReady(true)
	rec = doRequest(t, srv, http.MethodGet, "/readyz")
	assertResponse(t, rec, http.StatusOK, "ready")

	srv.SetReady(false)
	rec = doRequest(t, srv, http.MethodGet, "/readyz")
	assertResponse(t, rec, http.StatusServiceUnavailable, "not ready")
}

// TestHealthz verifies the liveness endpoint always reports ok.
func TestHealthz(t *testing.T) {
	srv := newTestServer()

	rec := doRequest(t, srv, http.MethodGet, "/healthz")
	assertResponse(t, rec, http.StatusOK, "ok")
}

// TestMetrics verifies the Prometheus handler is wired up at /metrics.
func TestMetrics(t *testing.T) {
	srv := newTestServer()

	rec := doRequest(t, srv, http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); !strings.Contains(body, "go_goroutines") {
		t.Errorf("body does not contain %q, indicating the Prometheus handler is not wired up correctly; body length = %d", "go_goroutines", len(body))
	}
}

// TestMethodRouting verifies that method-scoped patterns reject the wrong
// HTTP method with 405 rather than serving the handler anyway.
func TestMethodRouting(t *testing.T) {
	srv := newTestServer()

	rec := doRequest(t, srv, http.MethodPost, "/healthz")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// TestMuxExtraRoute verifies that Mux() returns a mux callers can register
// additional routes on, as the api binary does to mount the job API.
func TestMuxExtraRoute(t *testing.T) {
	srv := newTestServer()

	srv.Mux().HandleFunc("GET /jobs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("jobs"))
	})

	rec := doRequest(t, srv, http.MethodGet, "/jobs")
	assertResponse(t, rec, http.StatusOK, "jobs")
}
