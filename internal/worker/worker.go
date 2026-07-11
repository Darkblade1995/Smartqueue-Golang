package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"smartqueue/internal/metrics"
	"smartqueue/internal/queue"
	"smartqueue/internal/retry"
	"smartqueue/internal/storage"
)

type HandlerFunc func(ctx context.Context, job *queue.Job) error

type Worker struct {
	id       int
	consumer *queue.Consumer
	handlers map[string]HandlerFunc
	retryer  *retry.Retryer
	store    *storage.PostgresStore
	metrics  *metrics.Metrics
}

func NewWorker(id int, consumer *queue.Consumer, retryer *retry.Retryer, store *storage.PostgresStore, met *metrics.Metrics) *Worker {
	return &Worker{
		id:       id,
		consumer: consumer,
		handlers: make(map[string]HandlerFunc),
		retryer:  retryer,
		store:    store,
		metrics:  met,
	}
}

func (w *Worker) Register(jobType string, fn HandlerFunc) {
	w.handlers[jobType] = fn
}

func (w *Worker) Run(ctx context.Context) {
	log.Printf("[worker-%d] iniciado", w.id)
	w.metrics.WorkerStarted()
	defer w.metrics.WorkerStopped()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] detenido", w.id)
			return
		default:
		}

		job, err := w.consumer.Dequeue(ctx)
		if err != nil {
			log.Printf("[worker-%d] error en dequeue: %v", w.id, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if job == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		w.process(ctx, job)
	}
}

func (w *Worker) process(ctx context.Context, job *queue.Job) {
	start := time.Now()
	log.Printf("[worker-%d] procesando job %s tipo=%s", w.id, job.ID, job.Type)

	job.Status = queue.StatusProcessing
	w.consumer.UpdateStatus(ctx, job)
	w.store.Update(ctx, job)

	handler, exists := w.handlers[job.Type]
	if !exists {
		w.handleFailure(ctx, job, fmt.Errorf("no hay handler para tipo: %s", job.Type))
		return
	}

	err := handler(ctx, job)
	if err != nil {
		w.handleFailure(ctx, job, err)
		return
	}

	now := time.Now()
	job.Status = queue.StatusDone
	job.ProcessedAt = &now

	
	w.consumer.UpdateStatus(ctx, job)
	w.store.Update(ctx, job)

	
	w.metrics.JobProcessed(time.Since(start))

	log.Printf("[worker-%d] job %s completado en %s", w.id, job.ID, time.Since(start))
}

func (w *Worker) handleFailure(ctx context.Context, job *queue.Job, err error) {
	w.metrics.JobFailed()
	w.store.Update(ctx, job)
	w.retryer.Handle(ctx, job, err)
}