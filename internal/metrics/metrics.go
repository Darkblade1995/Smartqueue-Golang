package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	jobsProcessed  int64
	jobsFailed     int64
	totalDurationMs int64
	workersActive  int64

	
	mu        sync.RWMutex
	durations []float64
}

type Snapshot struct {
	JobsProcessed   int64   `json:"jobs_processed"`
	JobsFailed      int64   `json:"jobs_failed"`
	WorkersActive   int64   `json:"workers_active"`
	AvgProcessingMs float64 `json:"avg_processing_ms"`
	ErrorRate       float64 `json:"error_rate"`
}

func New() *Metrics {
	return &Metrics{
		durations: make([]float64, 0, 1000),
	}
}


func (m *Metrics) JobProcessed(duration time.Duration) {
	atomic.AddInt64(&m.jobsProcessed, 1)

	ms := float64(duration.Milliseconds())
	atomic.AddInt64(&m.totalDurationMs, int64(ms))

	m.mu.Lock()
	m.durations = append(m.durations, ms)
	
	if len(m.durations) > 1000 {
		m.durations = m.durations[len(m.durations)-1000:]
	}
	m.mu.Unlock()
}


func (m *Metrics) JobFailed() {
	atomic.AddInt64(&m.jobsFailed, 1)
}


func (m *Metrics) WorkerStarted() {
	atomic.AddInt64(&m.workersActive, 1)
}


func (m *Metrics) WorkerStopped() {
	atomic.AddInt64(&m.workersActive, -1)
}


func (m *Metrics) Snapshot() Snapshot {
	processed := atomic.LoadInt64(&m.jobsProcessed)
	failed := atomic.LoadInt64(&m.jobsFailed)
	active := atomic.LoadInt64(&m.workersActive)

	
	var avgMs float64
	m.mu.RLock()
	if len(m.durations) > 0 {
		var total float64
		for _, d := range m.durations {
			total += d
		}
		avgMs = total / float64(len(m.durations))
	}
	m.mu.RUnlock()

	
	var errorRate float64
	total := processed + failed
	if total > 0 {
		errorRate = float64(failed) / float64(total) * 100
	}

	return Snapshot{
		JobsProcessed:   processed,
		JobsFailed:      failed,
		WorkersActive:   active,
		AvgProcessingMs: avgMs,
		ErrorRate:       errorRate,
	}
}