// Package httpapi holds the HTTP surface shared by the three services: JSON
// replies, health, graceful shutdown, and the server-sent-events broker the
// dashboard reads from.
//
// Routing is stdlib. Go 1.22's ServeMux understands method and path patterns,
// which is the whole of what this needs — and a portfolio project earns more
// from having no router dependency than from having a familiar one.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// JSON writes a value with the right content type.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent, so there is nothing to report to
		// the client. Logging is the only honest option.
		slog.Debug("response encode failed", "err", err)
	}
}

// Error writes a JSON error body.
func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"error": msg})
}

// CORS allows the dashboard to be served from somewhere other than the
// gateway — the portfolio is on Vercel and the pipeline is not.
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Serve runs an HTTP server until ctx is cancelled, then drains it.
//
// The drain matters more than it looks: the gateway holds long-lived SSE
// connections, and without a bounded shutdown a redeploy would either hang on
// them or cut every viewer off mid-event.
func Serve(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: an SSE response is meant to stay open indefinitely,
		// and a write deadline would sever it on a schedule.
		IdleTimeout: 2 * time.Minute,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", addr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Warn("http shutdown was not clean", "err", err)
			return srv.Close()
		}
		return nil
	}
}
