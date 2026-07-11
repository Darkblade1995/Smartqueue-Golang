package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Consumer struct {
	client *redis.Client
}

func NewConsumer(client *redis.Client) *Consumer {
	return &Consumer{client: client}
}


func (c *Consumer) Dequeue(ctx context.Context) (*Job, error) {
	
	result, err := c.client.ZPopMin(ctx, "queue:jobs", 1).Result()
	if err != nil {
		return nil, fmt.Errorf("error leyendo cola: %w", err)
	}

	
	if len(result) == 0 {
		return nil, nil
	}

	jobID := result[0].Member.(string)

	
	jobKey := fmt.Sprintf("job:%s", jobID)
	data, err := c.client.Get(ctx, jobKey).Bytes()
	if err != nil {
		return nil, fmt.Errorf("error obteniendo job %s: %w", jobID, err)
	}

	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("error deserializando job: %w", err)
	}

	return &job, nil
}


func (c *Consumer) UpdateStatus(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("error serializando job: %w", err)
	}

	jobKey := fmt.Sprintf("job:%s", job.ID)
	return c.client.Set(ctx, jobKey, data, 0).Err()
}