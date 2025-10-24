package model

import "sync"

// AggregationWorker handles aggregation for a subset of records
type AggregationWorker struct {
	ID          int                          `json:"id"`
	JobID       string                       `json:"job_id"`
	GroupBy     string                       `json:"group_by"`
	Metrics     []string                     `json:"metrics"`
	Results     map[string]*AggregatedResult `json:"results"`
	Mutex       sync.RWMutex                 `json:"-"`
	RecordCount int64                        `json:"record_count"`
	ErrorCount  int64                        `json:"error_count"`
}

// ExportManager manages export operations
type ExportManager struct {
	JobID      string       `json:"job_id"`
	ExportSpec *Export      `json:"export_spec"`
	BatchSize  int          `json:"batch_size"`
	Mutex      sync.RWMutex `json:"-"`
}
