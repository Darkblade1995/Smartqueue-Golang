package api

import (
	"log"
	"net/http"
	"strings"
	"time"

	"smartqueue/internal/auth"
)

func NewRouter(h *Handler, authMiddleware *auth.Middleware) http.Handler {
	mux := http.NewServeMux()

	// rutas de autenticación (públicas)
	mux.HandleFunc("/auth/keys", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			h.CreateAPIKey(w, r)
		case http.MethodGet:
			h.ListAPIKeys(w, r)
		default:
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/auth/keys/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			h.RevokeAPIKey(w, r)
		} else {
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	// rutas protegidas
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			h.EnqueueJob(w, r)
		case http.MethodGet:
			h.ListJobs(w, r)
		default:
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			h.GetJob(w, r)
		} else {
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			h.GetDLQ(w, r)
		} else {
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/dlq/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/retry") {
			h.RetryJob(w, r)
		} else {
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			h.GetMetrics(w, r)
		} else {
			http.Error(w, "método no permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		h.respond(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	return recoveryMiddleware(loggerMiddleware(corsMiddleware(authMiddleware.Protect(mux))))
}

func loggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		log.Printf("[http] %s %s → %d (%s)",
			r.Method,
			r.URL.Path,
			wrapped.status,
			time.Since(start),
		)
	})
}

func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[recovery] panic atrapado: %v", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}