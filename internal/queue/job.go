package queue

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID          string         `json:"id"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload"`
	Priority    int            `json:"priority"`
	Status      Status         `json:"status"`
	Retries     int            `json:"retries"`
	MaxRetries  int            `json:"max_retries"`
	CreatedAt   time.Time      `json:"created_at"`
	ProcessedAt *time.Time     `json:"processed_at,omitempty"`
	Error       string         `json:"error,omitempty"`
}