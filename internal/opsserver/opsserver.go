// Package opsserver provides the operational HTTP server every leasework
// binary runs, exposing liveness, readiness and Prometheus metrics
// endpoints.
package opsserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the operational HTTP server every leasework binary runs.
// It serves GET /healthz, GET /readyz and GET /metrics.
type Server struct {
	httpServer *http.Server
	mux        *http.ServeMux
	logger     *slog.Logger
	ready      atomic.Bool
}

// New creates a Server that will listen on addr. Call Start to begin
// serving.
func New(addr string, logger *slog.Logger) *Server {
	s := &Server{
		mux:    http.NewServeMux(),
		logger: logger,
	}

	// Method-scoped patterns (Go 1.22+): a POST to /healthz gets 405 rather
	// than a misleading 200.
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /metrics", promhttp.Handler())

	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

// Mux returns the server's *http.ServeMux so callers, such as the api
// binary, can register additional routes (the job API) on it.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// SetReady marks the server ready, or not ready, for the /readyz endpoint.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// Start begins serving in a background goroutine and returns a channel that
// receives exactly one error if the listener fails for any reason other than
// a deliberate Shutdown. Callers must select on it: a process whose ops
// server never bound its port would otherwise stay up reporting nothing,
// invisible to both health checks and Prometheus.
func (s *Server) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("ops server listening on %s: %w", s.httpServer.Addr, err)
			return
		}
		close(errCh)
	}()
	return errCh
}

// Shutdown gracefully stops the server, waiting for in-flight requests to
// complete or ctx to expire.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutting down ops server: %w", err)
	}
	return nil
}

// handleHealthz reports liveness: always 200 OK once the process is up.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writePlain(w, http.StatusOK, "ok")
}

// handleReadyz reports readiness: 503 until SetReady(true) has been called,
// then 200.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writePlain(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	writePlain(w, http.StatusOK, "ready")
}

// writePlain writes a plain-text response with the given status code.
func writePlain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
