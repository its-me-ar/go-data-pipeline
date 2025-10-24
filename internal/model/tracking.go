package model

import (
	"sync"
	"time"
)

// PipelineMetrics represents overall pipeline performance metrics
type PipelineMetrics struct {
	TotalRecords   int64                    `json:"total_records"`
	ValidRecords   int64                    `json:"valid_records"`
	InvalidRecords int64                    `json:"invalid_records"`
	ErrorCount     int64                    `json:"error_count"`
	ProcessingTime time.Duration            `json:"processing_time"`
	ThroughputRPS  float64                  `json:"throughput_rps"`
	StageMetrics   map[string]StageMetrics  `json:"stage_metrics"`
	SourceMetrics  map[string]SourceMetrics `json:"source_metrics"`
}

// StageMetrics represents metrics for a specific pipeline stage
type StageMetrics struct {
	StageName        string        `json:"stage_name"`
	StartTime        time.Time     `json:"start_time"`
	EndTime          time.Time     `json:"end_time"`
	Duration         time.Duration `json:"duration"`
	RecordsProcessed int64         `json:"records_processed"`
	WorkerCount      int           `json:"worker_count"`
	ErrorCount       int64         `json:"error_count"`
	ThroughputRPS    float64       `json:"throughput_rps"`
}

// SourceMetrics represents metrics for a specific data source
type SourceMetrics struct {
	SourceURL       string        `json:"source_url"`
	RecordsIngested int64         `json:"records_ingested"`
	RecordsValid    int64         `json:"records_valid"`
	RecordsInvalid  int64         `json:"records_invalid"`
	IngestionTime   time.Duration `json:"ingestion_time"`
	AverageLatency  time.Duration `json:"average_latency"`
	ErrorCount      int64         `json:"error_count"`
}

// ErrorDetail represents a detailed error with context
type ErrorDetail struct {
	ID         string                 `json:"id"`
	Stage      string                 `json:"stage"`
	ErrorType  string                 `json:"error_type"`
	Message    string                 `json:"message"`
	SourceURL  string                 `json:"source_url,omitempty"`
	RecordData map[string]interface{} `json:"record_data,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
	Severity   string                 `json:"severity"`
	Retryable  bool                   `json:"retryable"`
	RetryCount int                    `json:"retry_count"`
}

// PipelineTracker manages pipeline execution tracking and metrics
type PipelineTracker struct {
	JobID          string                   `json:"job_id"`
	Job            PipelineJobSpec          `json:"job"`
	StartTime      time.Time                `json:"start_time"`
	EndTime        time.Time                `json:"end_time"`
	Status         string                   `json:"status"`
	Metrics        PipelineMetrics          `json:"metrics"`
	StageMetrics   map[string]StageMetrics  `json:"stage_metrics"`
	SourceMetrics  map[string]SourceMetrics `json:"source_metrics"`
	Errors         []ErrorDetail            `json:"errors"`
	RetryableOps   []RetryableOperation     `json:"retryable_ops"`
	Mutex          sync.RWMutex             `json:"-"`
	StopChan       chan struct{}            `json:"-"`
	BackgroundDone chan struct{}            `json:"-"`
}
