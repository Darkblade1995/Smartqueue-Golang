package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Producer struct {
	client *redis.Client
}

func NewProducer(client *redis.Client) *Producer {
	return &Producer{client: client}
}

func (p *Producer) Enqueue(ctx context.Context, job *Job) error {

	if job.ID == "" {
		job.ID = uuid.NewString()
		job.CreatedAt = time.Now()
	}
	job.Status = StatusPending

	if job.MaxRetries == 0 {
		job.MaxRetries = 3
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error serializando job: %w", err)
	}

	jobKey := fmt.Sprintf("job:%s", job.ID)
	err = p.client.Set(ctx, jobKey, data, 0).Err()
	if err != nil {
		return fmt.Errorf("error guardando job en redis: %w", err)
	}

	err = p.client.ZAdd(ctx, "queue:jobs", redis.Z{
		Score:  float64(job.Priority),
		Member: job.ID,
	}).Err()
	if err != nil {
		return fmt.Errorf("error encolando job: %w", err)
	}

	return nil
}