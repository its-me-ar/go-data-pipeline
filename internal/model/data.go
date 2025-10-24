package model

import "time"

// AggregatedResult represents the result of data aggregation
type AggregatedResult struct {
	GroupKey    string                 `json:"group_key"`
	GroupValue  interface{}            `json:"group_value"`
	Metrics     map[string]interface{} `json:"metrics"`
	RecordCount int                    `json:"record_count"`
	SourceURL   string                 `json:"source_url,omitempty"`
}

// ExportResult represents the result of an export operation
type ExportResult struct {
	Type        string    `json:"type"` // "database", "csv", "json"
	Path        string    `json:"path"` // file path or table name
	RecordCount int       `json:"record_count"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}
