package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" 
	"smartqueue/internal/queue"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error abriendo conexión: %w", err)
	}

	
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error conectando a postgres: %w", err)
	}

	
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &PostgresStore{db: db}, nil
}


func (s *PostgresStore) Migrate() error {
	query := `
	CREATE TABLE IF NOT EXISTS jobs (
		id           VARCHAR(36) PRIMARY KEY,
		type         VARCHAR(50) NOT NULL,
		payload      JSONB NOT NULL DEFAULT '{}',
		priority     INT NOT NULL DEFAULT 2,
		status       VARCHAR(20) NOT NULL DEFAULT 'pending',
		retries      INT NOT NULL DEFAULT 0,
		max_retries  INT NOT NULL DEFAULT 3,
		error        TEXT,
		created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		processed_at TIMESTAMPTZ
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_status   ON jobs(status);
	CREATE INDEX IF NOT EXISTS idx_jobs_type     ON jobs(type);
	CREATE INDEX IF NOT EXISTS idx_jobs_created  ON jobs(created_at DESC);
	`

	_, err := s.db.Exec(query)
	return err
}


func (s *PostgresStore) Save(ctx context.Context, job *queue.Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("error serializando payload: %w", err)
	}

	query := `
	INSERT INTO jobs (id, type, payload, priority, status, retries, max_retries, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err = s.db.ExecContext(ctx, query,
		job.ID,
		job.Type,
		payload,
		job.Priority,
		job.Status,
		job.Retries,
		job.MaxRetries,
		job.CreatedAt,
	)

	return err
}

// Update actualiza el estado de un job existente
func (s *PostgresStore) Update(ctx context.Context, job *queue.Job) error {
	query := `
	UPDATE jobs
	SET status       = $1,
	    retries      = $2,
	    error        = $3,
	    processed_at = $4
	WHERE id = $5
	`

	_, err := s.db.ExecContext(ctx, query,
		job.Status,
		job.Retries,
		job.Error,
		job.ProcessedAt,
		job.ID,
	)

	return err
}


func (s *PostgresStore) GetByID(ctx context.Context, id string) (*queue.Job, error) {
	query := `
	SELECT id, type, payload, priority, status, retries, max_retries,
	       error, created_at, processed_at
	FROM jobs
	WHERE id = $1
	`

	row := s.db.QueryRowContext(ctx, query, id)
	return scanJob(row)
}


func (s *PostgresStore) GetByStatus(ctx context.Context, status queue.Status) ([]*queue.Job, error) {
	query := `
	SELECT id, type, payload, priority, status, retries, max_retries,
	       error, created_at, processed_at
	FROM jobs
	WHERE status = $1
	ORDER BY created_at DESC
	LIMIT 100
	`

	rows, err := s.db.QueryContext(ctx, query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*queue.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}


func scanJob(scanner interface {
	Scan(...any) error
}) (*queue.Job, error) {
	var job queue.Job
	var payloadRaw []byte
	var errStr sql.NullString
	var processedAt sql.NullTime

	err := scanner.Scan(
		&job.ID,
		&job.Type,
		&payloadRaw,
		&job.Priority,
		&job.Status,
		&job.Retries,
		&job.MaxRetries,
		&errStr,
		&job.CreatedAt,
		&processedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error escaneando job: %w", err)
	}

	
	if err := json.Unmarshal(payloadRaw, &job.Payload); err != nil {
		return nil, fmt.Errorf("error deserializando payload: %w", err)
	}

	
	if errStr.Valid {
		job.Error = errStr.String
	}
	if processedAt.Valid {
		job.ProcessedAt = &processedAt.Time
	}

	return &job, nil
}