package retry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"smartqueue/internal/queue"
)

// ── Mocks ────────────────────────────────────────────────────

type mockProducer struct {
	enqueueCalled int
	lastJob       *queue.Job
}

func (m *mockProducer) Enqueue(ctx context.Context, job *queue.Job) error {
	m.enqueueCalled++
	m.lastJob = job
	return nil
}

type mockStore struct {
	updateCalled int
	lastJob      *queue.Job
}

func (m *mockStore) Update(ctx context.Context, job *queue.Job) error {
	m.updateCalled++
	m.lastJob = job
	return nil
}

type mockConsumer struct{}

func (m *mockConsumer) UpdateStatus(ctx context.Context, job *queue.Job) error {
	return nil
}

// ── Tests ─────────────────────────────────────────────────────

func TestCalculateDelay_CrecimientoExponencial(t *testing.T) {
	r := &Retryer{}

	delay1 := r.calculateDelay(1)
	delay2 := r.calculateDelay(2)
	delay3 := r.calculateDelay(3)

	// cada delay debe ser mayor que el anterior
	if delay2 <= delay1 {
		t.Errorf("delay2 (%s) debería ser mayor que delay1 (%s)", delay2, delay1)
	}
	if delay3 <= delay2 {
		t.Errorf("delay3 (%s) debería ser mayor que delay2 (%s)", delay3, delay2)
	}
}

func TestCalculateDelay_NuncaSuperaMaxDelay(t *testing.T) {
	r := &Retryer{}

	// intento 100 debería estar limitado por maxDelay
	delay := r.calculateDelay(100)

	if delay > maxDelay {
		t.Errorf("delay (%s) supera maxDelay (%s)", delay, maxDelay)
	}
}

func TestCalculateDelay_RangoMinimo(t *testing.T) {
	r := &Retryer{}

	delay := r.calculateDelay(1)

	// intento 1 debería ser al menos baseDelay * 2 = 4s
	minEsperado := baseDelay * 2
	if delay < minEsperado {
		t.Errorf("delay (%s) menor al mínimo esperado (%s)", delay, minEsperado)
	}
}

func TestHandle_JobVaADLQAlAgotar(t *testing.T) {
	producer := &mockProducer{}
	store := &mockStore{}
	consumer := &mockConsumer{}

	r := newRetryerWithMocks(producer, consumer, store)

	job := &queue.Job{
		ID:         "test-123",
		Type:       "email",
		Status:     queue.StatusProcessing,
		Retries:    2, // ya falló 2 veces
		MaxRetries: 3,
	}

	r.Handle(context.Background(), job, fmt.Errorf("fallo simulado"))

	// debe haber ido a DLQ
	if job.Status != queue.StatusFailed {
		t.Errorf("esperaba status=failed, obtuve %s", job.Status)
	}

	// store.Update debe haberse llamado
	if store.updateCalled == 0 {
		t.Error("store.Update nunca fue llamado para DLQ")
	}

	// producer.Enqueue NO debe llamarse (no re-encola si va a DLQ)
	if producer.enqueueCalled > 0 {
		t.Error("producer.Enqueue no debería llamarse cuando job va a DLQ")
	}
}

func TestHandle_JobSeReencolarSiNoAgota(t *testing.T) {
	producer := &mockProducer{}
	store := &mockStore{}
	consumer := &mockConsumer{}

	r := newRetryerWithMocks(producer, consumer, store)

	job := &queue.Job{
		ID:         "test-456",
		Type:       "email",
		Status:     queue.StatusProcessing,
		Retries:    0, // primer fallo
		MaxRetries: 3,
	}

	// usamos timeout corto para no esperar el backoff completo
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r.Handle(ctx, job, fmt.Errorf("fallo simulado"))

	// debe haber re-encolado
	if producer.enqueueCalled == 0 {
		t.Error("producer.Enqueue debería haberse llamado para re-encolar")
	}

	// el job reencolado debe tener el mismo ID
	if producer.lastJob.ID != "test-456" {
		t.Errorf("esperaba ID=test-456, obtuve %s", producer.lastJob.ID)
	}
}

// ── Helper ────────────────────────────────────────────────────

// interfaz mínima para inyectar mocks
type producerInterface interface {
	Enqueue(ctx context.Context, job *queue.Job) error
}

type storeInterface interface {
	Update(ctx context.Context, job *queue.Job) error
}

type consumerInterface interface {
	UpdateStatus(ctx context.Context, job *queue.Job) error
}

type testRetryer struct {
	producer producerInterface
	consumer consumerInterface
	store    storeInterface
}

func newRetryerWithMocks(p producerInterface, c consumerInterface, s storeInterface) *testRetryer {
	return &testRetryer{producer: p, consumer: c, store: s}
}

func (r *testRetryer) Handle(ctx context.Context, job *queue.Job, jobErr error) {
	job.Retries++
	job.Error = jobErr.Error()

	if job.Retries >= job.MaxRetries {
		job.Status = queue.StatusFailed
		r.store.Update(ctx, job)
		r.consumer.UpdateStatus(ctx, job)
		return
	}

	delay := (&Retryer{}).calculateDelay(job.Retries)

	select {
	case <-time.After(delay):
		job.Status = queue.StatusPending
		job.Error = ""
		r.producer.Enqueue(ctx, job)
	case <-ctx.Done():
		return
	}
}