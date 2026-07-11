package metrics

import (
	"testing"
	"time"
)

func TestJobProcessed_IncrementaContador(t *testing.T) {
	m := New()

	m.JobProcessed(100 * time.Millisecond)
	m.JobProcessed(200 * time.Millisecond)

	snap := m.Snapshot()

	if snap.JobsProcessed != 2 {
		t.Errorf("esperaba 2 jobs procesados, obtuve %d", snap.JobsProcessed)
	}
}

func TestJobFailed_IncrementaContador(t *testing.T) {
	m := New()

	m.JobFailed()
	m.JobFailed()
	m.JobFailed()

	snap := m.Snapshot()

	if snap.JobsFailed != 3 {
		t.Errorf("esperaba 3 jobs fallidos, obtuve %d", snap.JobsFailed)
	}
}

func TestSnapshot_CalculaPromedioCorrectamente(t *testing.T) {
	m := New()

	m.JobProcessed(100 * time.Millisecond)
	m.JobProcessed(300 * time.Millisecond)

	snap := m.Snapshot()

	
	if snap.AvgProcessingMs != 200.0 {
		t.Errorf("esperaba promedio=200ms, obtuve %f", snap.AvgProcessingMs)
	}
}

func TestSnapshot_ErrorRateCorrect(t *testing.T) {
	m := New()

	m.JobProcessed(100 * time.Millisecond)
	m.JobProcessed(100 * time.Millisecond)
	m.JobProcessed(100 * time.Millisecond)
	m.JobFailed()

	snap := m.Snapshot()

	
	if snap.ErrorRate != 25.0 {
		t.Errorf("esperaba error_rate=25%%, obtuve %f", snap.ErrorRate)
	}
}

func TestWorkersActive_SubeYBaja(t *testing.T) {
	m := New()

	m.WorkerStarted()
	m.WorkerStarted()
	m.WorkerStarted()

	snap := m.Snapshot()
	if snap.WorkersActive != 3 {
		t.Errorf("esperaba 3 workers activos, obtuve %d", snap.WorkersActive)
	}

	m.WorkerStopped()
	snap = m.Snapshot()
	if snap.WorkersActive != 2 {
		t.Errorf("esperaba 2 workers activos después de Stop, obtuve %d", snap.WorkersActive)
	}
}