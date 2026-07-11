package auth

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Migrate() error {
	query := `
	CREATE TABLE IF NOT EXISTS api_keys (
		id         VARCHAR(36)  PRIMARY KEY,
		name       VARCHAR(100) NOT NULL,
		key_hash   VARCHAR(64)  NOT NULL UNIQUE,
		key_prefix VARCHAR(20)  NOT NULL,
		active     BOOLEAN      NOT NULL DEFAULT true,
		created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
	`
	_, err := s.db.Exec(query)
	return err
}


func (s *Store) Create(ctx context.Context, name string) (*APIKeyResponse, error) {
	key, hash, prefix, err := GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("error generando key: %w", err)
	}

	apiKey := &APIKey{
		ID:        uuid.NewString(),
		Name:      name,
		KeyHash:   hash,
		KeyPrefix: prefix,
		Active:    true,
		CreatedAt: time.Now(),
	}

	query := `
	INSERT INTO api_keys (id, name, key_hash, key_prefix, active, created_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = s.db.ExecContext(ctx, query,
		apiKey.ID,
		apiKey.Name,
		apiKey.KeyHash,
		apiKey.KeyPrefix,
		apiKey.Active,
		apiKey.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error guardando key: %w", err)
	}

	return &APIKeyResponse{
		APIKey: *apiKey,
		Key:    key, 
	}, nil
}


func (s *Store) Validate(ctx context.Context, key string) (*APIKey, error) {
	hash := hashKey(key)

	query := `
	SELECT id, name, key_hash, key_prefix, active, created_at
	FROM api_keys
	WHERE key_hash = $1
	`

	row := s.db.QueryRowContext(ctx, query, hash)

	var apiKey APIKey
	err := row.Scan(
		&apiKey.ID,
		&apiKey.Name,
		&apiKey.KeyHash,
		&apiKey.KeyPrefix,
		&apiKey.Active,
		&apiKey.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("key no encontrada")
	}
	if err != nil {
		return nil, fmt.Errorf("error validando key: %w", err)
	}

	if !apiKey.Active {
		return nil, fmt.Errorf("key revocada")
	}

	return &apiKey, nil
}


func (s *Store) List(ctx context.Context) ([]*APIKey, error) {
	query := `
	SELECT id, name, key_prefix, active, created_at
	FROM api_keys
	ORDER BY created_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []*APIKey
	for rows.Next() {
		var k APIKey
		err := rows.Scan(
			&k.ID,
			&k.Name,
			&k.KeyPrefix,
			&k.Active,
			&k.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		keys = append(keys, &k)
	}

	if keys == nil {
		keys = []*APIKey{}
	}

	return keys, rows.Err()
}


func (s *Store) Revoke(ctx context.Context, id string) error {
	query := `UPDATE api_keys SET active = false WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("error revocando key: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("key no encontrada")
	}

	return nil
}