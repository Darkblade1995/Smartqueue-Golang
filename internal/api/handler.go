package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"smartqueue/internal/auth"
	"smartqueue/internal/metrics"
	"smartqueue/internal/queue"
	"smartqueue/internal/storage"
)

type Handler struct {
	producer *queue.Producer
	consumer *queue.Consumer
	store    *storage.PostgresStore
	metrics  *metrics.Metrics
	authStore *auth.Store
}

func NewHandler(
	producer *queue.Producer,
	consumer *queue.Consumer,
	store *storage.PostgresStore,
	metrics *metrics.Metrics,
	authStore *auth.Store,
) *Handler {
	return &Handler{
		producer:  producer,
		consumer:  consumer,
		store:     store,
		metrics:   metrics,
		authStore: authStore,
	}
}

func (h *Handler) respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	h.respond(w, status, map[string]string{"error": msg})
}

// ── Auth endpoints ────────────────────────────────────────────

func (h *Handler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.respondError(w, http.StatusBadRequest, "body JSON inválido")
		return
	}

	if body.Name == "" {
		h.respondError(w, http.StatusBadRequest, "field 'name' es requerido")
		return
	}

	key, err := h.authStore.Create(r.Context(), body.Name)
	if err != nil {
		log.Printf("[handler] error creando API key: %v", err)
		h.respondError(w, http.StatusInternalServerError, "error creando key")
		return
	}

	
	h.respond(w, http.StatusCreated, key)
}

func (h *Handler) ListAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := h.authStore.List(r.Context())
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "error listando keys")
		return
	}
	h.respond(w, http.StatusOK, keys)
}

func (h *Handler) RevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/auth/keys/")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "id requerido")
		return
	}

	if err := h.authStore.Revoke(r.Context(), id); err != nil {
		h.respondError(w, http.StatusNotFound, "key no encontrada")
		return
	}

	h.respond(w, http.StatusOK, map[string]string{"status": "revocada"})
}

// ── Job endpoints ─────────────────────────────────────────────

func (h *Handler) EnqueueJob(w http.ResponseWriter, r *http.Request) {
	var job queue.Job

	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		h.respondError(w, http.StatusBadRequest, "body JSON inválido")
		return
	}

	if job.Type == "" {
		h.respondError(w, http.StatusBadRequest, "field 'type' es requerido")
		return
	}
	if job.Priority < 1 || job.Priority > 3 {
		job.Priority = 2
	}

	if err := h.producer.Enqueue(r.Context(), &job); err != nil {
		log.Printf("[handler] error encolando job: %v", err)
		h.respondError(w, http.StatusInternalServerError, "error encolando job")
		return
	}

	if err := h.store.Save(r.Context(), &job); err != nil {
		log.Printf("[handler] error guardando job en postgres: %v", err)
	}

	h.respond(w, http.StatusCreated, job)
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/jobs/")
	if id == "" {
		h.respondError(w, http.StatusBadRequest, "id requerido")
		return
	}

	job, err := h.store.GetByID(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "job no encontrado")
		return
	}

	h.respond(w, http.StatusOK, job)
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	statusParam := r.URL.Query().Get("status")

	var status queue.Status
	switch statusParam {
	case "pending":
		status = queue.StatusPending
	case "processing":
		status = queue.StatusProcessing
	case "done":
		status = queue.StatusDone
	case "failed":
		status = queue.StatusFailed
	default:
		status = queue.StatusPending
	}

	jobs, err := h.store.GetByStatus(r.Context(), status)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "error listando jobs")
		return
	}

	if jobs == nil {
		jobs = []*queue.Job{}
	}

	h.respond(w, http.StatusOK, jobs)
}

func (h *Handler) GetDLQ(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.GetByStatus(r.Context(), queue.StatusFailed)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "error obteniendo DLQ")
		return
	}

	if jobs == nil {
		jobs = []*queue.Job{}
	}

	h.respond(w, http.StatusOK, jobs)
}

func (h *Handler) RetryJob(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/dlq/")
	id = strings.TrimSuffix(id, "/retry")

	if id == "" {
		h.respondError(w, http.StatusBadRequest, "id requerido")
		return
	}

	job, err := h.store.GetByID(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "job no encontrado")
		return
	}

	if job.Status != queue.StatusFailed {
		h.respondError(w, http.StatusBadRequest, "solo se pueden reintentar jobs fallidos")
		return
	}

	job.Status = queue.StatusPending
	job.Retries = 0
	job.Error = ""

	if err := h.producer.Enqueue(r.Context(), job); err != nil {
		h.respondError(w, http.StatusInternalServerError, "error re-encolando job")
		return
	}

	h.respond(w, http.StatusOK, job)
}

func (h *Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := h.metrics.Snapshot()
	h.respond(w, http.StatusOK, snapshot)
}

func extractID(path, prefix string) string {
	id := strings.TrimPrefix(path, prefix)
	return strings.Split(id, "/")[0]
}