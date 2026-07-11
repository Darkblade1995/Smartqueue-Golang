package worker

import (
	"context"
	"log"
	"sync"

	"smartqueue/internal/metrics"
	"smartqueue/internal/queue"
	"smartqueue/internal/retry"
	"smartqueue/internal/storage"
)

type Pool struct {
	size     int
	consumer *queue.Consumer
	handlers map[string]HandlerFunc
	retryer  *retry.Retryer
	store    *storage.PostgresStore
	metrics  *metrics.Metrics
	workers  []*Worker
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

func NewPool(size int, consumer *queue.Consumer, retryer *retry.Retryer, store *storage.PostgresStore, met *metrics.Metrics) *Pool {
	return &Pool{
		size:     size,
		consumer: consumer,
		handlers: make(map[string]HandlerFunc),
		retryer:  retryer,
		store:    store,
		metrics:  met,
	}
}

func (p *Pool) Register(jobType string, fn HandlerFunc) {
	p.handlers[jobType] = fn
}

func (p *Pool) Start(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	log.Printf("[pool] iniciando %d workers", p.size)

	for i := 0; i < p.size; i++ {
		w := NewWorker(i, p.consumer, p.retryer, p.store, p.metrics)

		for jobType, fn := range p.handlers {
			w.Register(jobType, fn)
		}

		p.workers = append(p.workers, w)

		p.wg.Add(1)
		go func(worker *Worker) {
			defer p.wg.Done()
			worker.Run(childCtx)
		}(w)
	}
}

func (p *Pool) Stop() {
	log.Printf("[pool] deteniendo workers...")
	p.cancel()
	p.wg.Wait()
	log.Printf("[pool] todos los workers detenidos")
}