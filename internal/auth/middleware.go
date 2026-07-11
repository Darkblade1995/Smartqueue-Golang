package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type contextKey string

const APIKeyContextKey contextKey = "api_key"

type Middleware struct {
	store *Store
}

func NewMiddleware(store *Store) *Middleware {
	return &Middleware{store: store}
}


func (m *Middleware) Protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	
		if isPublicRoute(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}


		key, err := extractKey(r)
		if err != nil {
			respondUnauthorized(w, err.Error())
			return
		}


		apiKey, err := m.store.Validate(r.Context(), key)
		if err != nil {
			respondUnauthorized(w, "API key inválida o revocada")
			return
		}

		ctx := context.WithValue(r.Context(), APIKeyContextKey, apiKey)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}


func extractKey(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", fmt.Errorf("header Authorization requerido")
	}


	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return "", fmt.Errorf("formato inválido, usa: Bearer <key>")
	}

	return parts[1], nil
}


func isPublicRoute(path string) bool {
	publicRoutes := []string{
		"/health",
		"/auth/keys",
	}

	for _, route := range publicRoutes {
		if path == route {
			return true
		}
	}
	return false
}


func respondUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}