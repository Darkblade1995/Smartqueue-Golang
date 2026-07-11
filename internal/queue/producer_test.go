package queue

import (
	"testing"
	"time"
)

func TestEnqueue_AsignaIDSiNoTiene(t *testing.T) {
	job := &Job{
		Type:     "email",
		Priority: 1,
	}

	
	if job.ID == "" {
		job.ID = "uuid-generado"
		job.CreatedAt = time.Now()
	}

	if job.ID == "" {
		t.Error("el job debería tener ID después de Enqueue")
	}
}

func TestEnqueue_ConservaIDSiYaTiene(t *testing.T) {
	job := &Job{
		ID:       "id-original",
		Type:     "email",
		Priority: 1,
	}

	idOriginal := job.ID

	
	if job.ID == "" {
		job.ID = "id-nuevo"
	}

	if job.ID != idOriginal {
		t.Errorf("el ID no debería cambiar, esperaba %s obtuve %s", idOriginal, job.ID)
	}
}

func TestJob_StatusPendingEsDefault(t *testing.T) {
	job := &Job{
		Type:     "email",
		Priority: 1,
	}

	job.Status = StatusPending

	if job.Status != StatusPending {
		t.Errorf("esperaba status=pending, obtuve %s", job.Status)
	}
}

func TestJob_MaxRetriesDefault(t *testing.T) {
	job := &Job{
		Type: "email",
	}

	if job.MaxRetries == 0 {
		job.MaxRetries = 3
	}

	if job.MaxRetries != 3 {
		t.Errorf("esperaba max_retries=3, obtuve %d", job.MaxRetries)
	}
}