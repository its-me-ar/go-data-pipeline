package model

import (
	"sync"
	"time"
)

// RetryConfig defines retry behavior for operations
type RetryConfig struct {
	MaxRetries      int           `json:"max_retries"`
	InitialDelay    time.Duration `json:"initial_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	BackoffFactor   float64       `json:"backoff_factor"`
	RetryableErrors []string      `json:"retryable_errors"`
}

// RetryableOperation represents an operation that can be retried
type RetryableOperation struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"` // "ingestion", "validation", "transformation", "aggregation", "export"
	SourceURL   string                 `json:"source_url"`
	RecordData  map[string]interface{} `json:"record_data"`
	Error       error                  `json:"error"`
	RetryCount  int                    `json:"retry_count"`
	NextRetryAt time.Time              `json:"next_retry_at"`
	MaxRetries  int                    `json:"max_retries"`
	CreatedAt   time.Time              `json:"created_at"`
	LastRetryAt time.Time              `json:"last_retry_at"`
}

// RetryManager manages retry operations for failed pipeline tasks
type RetryManager struct {
	JobID                string                  `json:"job_id"`
	RetryableOps         []RetryableOperation    `json:"retryable_ops"`
	RetryConfig          RetryConfig             `json:"retry_config"`
	MaxConcurrentRetries int                     `json:"max_concurrent_retries"`
	RetryWorkers         int                     `json:"retry_workers"`
	Mutex                sync.RWMutex            `json:"-"`
	StopChan             chan struct{}           `json:"-"`
	RetryQueue           chan RetryableOperation `json:"-"`
	WorkerDone           chan int                `json:"-"`
	Stats                map[string]interface{}  `json:"stats"`
}
