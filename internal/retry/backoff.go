package retry

import (
	"context"
	"log"
	"math"
	"math/rand"
	"time"

	"smartqueue/internal/queue"
	"smartqueue/internal/storage"
)

const (
	baseDelay = 2 * time.Second
	maxDelay  = 30 * time.Second
)

type Retryer struct {
	producer *queue.Producer
	consumer *queue.Consumer
	store    *storage.PostgresStore
}

func NewRetryer(producer *queue.Producer, consumer *queue.Consumer, store *storage.PostgresStore) *Retryer {
	return &Retryer{
		producer: producer,
		consumer: consumer,
		store:    store,
	}
}

func (r *Retryer) Handle(ctx context.Context, job *queue.Job, jobErr error) {
	job.Retries++
	job.Error = jobErr.Error()

	log.Printf("[retry] job %s falló (intento %d/%d): %v",
		job.ID, job.Retries, job.MaxRetries, jobErr)

	if job.Retries >= job.MaxRetries {
		r.sendToDLQ(ctx, job)
		return
	}

	job.Status = queue.StatusPending
	if err := r.store.Update(ctx, job); err != nil {
		log.Printf("[retry] error actualizando postgres job %s: %v", job.ID, err)
	}

	delay := r.calculateDelay(job.Retries)
	log.Printf("[retry] reintentando job %s en %s", job.ID, delay)

	select {
	case <-time.After(delay):
		job.Status = queue.StatusPending
		job.Error = ""
		if err := r.producer.Enqueue(ctx, job); err != nil {
			log.Printf("[retry] error re-encolando job %s: %v", job.ID, err)
		}
	case <-ctx.Done():
		log.Printf("[retry] contexto cancelado, job %s no re-encolado", job.ID)
	}
}

func (r *Retryer) calculateDelay(retries int) time.Duration {
	exp := math.Pow(2, float64(retries))
	delay := time.Duration(float64(baseDelay) * exp)

	jitter := time.Duration(rand.Int63n(int64(time.Second)))
	delay += jitter

	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

func (r *Retryer) sendToDLQ(ctx context.Context, job *queue.Job) {
	job.Status = queue.StatusFailed

	if err := r.consumer.UpdateStatus(ctx, job); err != nil {
		log.Printf("[retry] error actualizando status DLQ job %s en redis: %v", job.ID, err)
	}

	if err := r.store.Update(ctx, job); err != nil {
		log.Printf("[retry] error actualizando status DLQ job %s en postgres: %v", job.ID, err)
		return
	}

	log.Printf("[retry] job %s enviado a DLQ después de %d intentos",
		job.ID, job.Retries)
}
