package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/store"
	"sync"
	"time"
)

// PipelineMetrics tracks comprehensive pipeline metrics
type PipelineMetrics struct {
	JobID              string                 `json:"job_id"`
	StartTime          time.Time              `json:"start_time"`
	EndTime            *time.Time             `json:"end_time,omitempty"`
	Duration           time.Duration          `json:"duration,omitempty"`
	Status             string                 `json:"status"`
	
	// Stage Metrics
	IngestionMetrics   StageMetrics           `json:"ingestion_metrics"`
	ValidationMetrics  StageMetrics           `json:"validation_metrics"`
	TransformMetrics   StageMetrics           `json:"transform_metrics"`
	AggregationMetrics StageMetrics           `json:"aggregation_metrics"`
	ExportMetrics      StageMetrics           `json:"export_metrics"`
	
	// Overall Metrics
	TotalRecords       int64                  `json:"total_records"`
	ValidRecords       int64                  `json:"valid_records"`
	InvalidRecords     int64                  `json:"invalid_records"`
	TransformedRecords int64                  `json:"transformed_records"`
	AggregatedGroups   int64                  `json:"aggregated_groups"`
	ExportedRecords    int64                  `json:"exported_records"`
	ErrorCount         int64                  `json:"error_count"`
	
	// Performance Metrics
	RecordsPerSecond   float64                `json:"records_per_second"`
	AverageLatency     time.Duration          `json:"average_latency"`
	PeakMemoryUsage    int64                  `json:"peak_memory_usage"`
	
	// Error Details
	Errors             []ErrorDetail          `json:"errors"`
	RetryAttempts      int                    `json:"retry_attempts"`
	LastRetryTime      *time.Time             `json:"last_retry_time,omitempty"`
	
	// Source-specific metrics
	SourceMetrics      map[string]SourceMetrics `json:"source_metrics"`
}

// StageMetrics tracks metrics for individual pipeline stages
type StageMetrics struct {
	StartTime          time.Time              `json:"start_time"`
	EndTime            *time.Time             `json:"end_time,omitempty"`
	Duration           time.Duration          `json:"duration,omitempty"`
	RecordsProcessed   int64                  `json:"records_processed"`
	RecordsPerSecond   float64                `json:"records_per_second"`
	ErrorCount         int64                  `json:"error_count"`
	WorkerCount        int                    `json:"worker_count"`
	Status             string                 `json:"status"` // "running", "completed", "failed"
}

// SourceMetrics tracks metrics for individual data sources
type SourceMetrics struct {
	SourceURL          string                 `json:"source_url"`
	SourceType         string                 `json:"source_type"`
	RecordsIngested    int64                  `json:"records_ingested"`
	RecordsValid       int64                  `json:"records_valid"`
	RecordsInvalid     int64                  `json:"records_invalid"`
	IngestionTime      time.Duration          `json:"ingestion_time"`
	AverageRecordSize  int64                  `json:"average_record_size"`
	ErrorCount         int64                  `json:"error_count"`
	LastError          string                 `json:"last_error,omitempty"`
}

// ErrorDetail represents a detailed error with context
type ErrorDetail struct {
	Timestamp          time.Time              `json:"timestamp"`
	Stage              string                 `json:"stage"`
	SourceURL          string                 `json:"source_url,omitempty"`
	ErrorType          string                 `json:"error_type"`
	ErrorMessage       string                 `json:"error_message"`
	RecordData         map[string]interface{} `json:"record_data,omitempty"`
	Retryable          bool                   `json:"retryable"`
	RetryCount         int                    `json:"retry_count"`
	Severity           string                 `json:"severity"` // "low", "medium", "high", "critical"
}

// PipelineTracker manages comprehensive pipeline tracking and metrics
type PipelineTracker struct {
	JobID              string
	Metrics            *PipelineMetrics
	Mutex              sync.RWMutex
	ErrorChannel       chan ErrorDetail
	UpdateChannel      chan PipelineMetrics
	Context            context.Context
	Cancel             context.CancelFunc
}

// NewPipelineTracker creates a new pipeline tracker
func NewPipelineTracker(jobID string, job model.PipelineJobSpec) *PipelineTracker {
	ctx, cancel := context.WithCancel(context.Background())
	
	tracker := &PipelineTracker{
		JobID:        jobID,
		Context:      ctx,
		Cancel:       cancel,
		ErrorChannel: make(chan ErrorDetail, 1000),
		UpdateChannel: make(chan PipelineMetrics, 100),
		Metrics: &PipelineMetrics{
			JobID:         jobID,
			StartTime:     time.Now(),
			Status:        "initializing",
			SourceMetrics: make(map[string]SourceMetrics),
			Errors:        make([]ErrorDetail, 0),
		},
	}
	
	// Initialize source metrics
	for _, source := range job.Sources {
		tracker.Metrics.SourceMetrics[source.URL] = SourceMetrics{
			SourceURL: source.URL,
			SourceType: source.Type,
		}
	}
	
	// Start background tracking
	go tracker.startBackgroundTracking()
	
	return tracker
}

// startBackgroundTracking runs background tasks for metrics collection
func (pt *PipelineTracker) startBackgroundTracking() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-pt.Context.Done():
			return
		case errorDetail := <-pt.ErrorChannel:
			pt.handleError(errorDetail)
		case <-ticker.C:
			pt.updateMetrics()
		}
	}
}

// handleError processes and stores error details
func (pt *PipelineTracker) handleError(errorDetail ErrorDetail) {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	// Add error to metrics
	pt.Metrics.Errors = append(pt.Metrics.Errors, errorDetail)
	pt.Metrics.ErrorCount++
	
	// Update stage-specific error count
	switch errorDetail.Stage {
	case "ingestion":
		pt.Metrics.IngestionMetrics.ErrorCount++
	case "validation":
		pt.Metrics.ValidationMetrics.ErrorCount++
	case "transformation":
		pt.Metrics.TransformMetrics.ErrorCount++
	case "aggregation":
		pt.Metrics.AggregationMetrics.ErrorCount++
	case "export":
		pt.Metrics.ExportMetrics.ErrorCount++
	}
	
	// Update source-specific error count
	if errorDetail.SourceURL != "" {
		if sourceMetrics, exists := pt.Metrics.SourceMetrics[errorDetail.SourceURL]; exists {
			sourceMetrics.ErrorCount++
			sourceMetrics.LastError = errorDetail.ErrorMessage
			pt.Metrics.SourceMetrics[errorDetail.SourceURL] = sourceMetrics
		}
	}
	
	// Store error in database
	store.SaveJobError(pt.JobID, fmt.Errorf("[%s] %s: %s", 
		errorDetail.Stage, errorDetail.ErrorType, errorDetail.ErrorMessage))
	
	// Log critical errors
	if errorDetail.Severity == "critical" {
		fmt.Printf("🚨 CRITICAL ERROR [%s]: %s\n", errorDetail.Stage, errorDetail.ErrorMessage)
	}
}

// updateMetrics updates current metrics and saves to database
func (pt *PipelineTracker) updateMetrics() {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	// Calculate duration
	if pt.Metrics.EndTime == nil {
		pt.Metrics.Duration = time.Since(pt.Metrics.StartTime)
	} else {
		pt.Metrics.Duration = pt.Metrics.EndTime.Sub(pt.Metrics.StartTime)
	}
	
	// Calculate records per second
	if pt.Metrics.Duration > 0 {
		pt.Metrics.RecordsPerSecond = float64(pt.Metrics.TotalRecords) / pt.Metrics.Duration.Seconds()
	}
	
	// Update stage durations
	pt.updateStageDurations()
	
	// Save metrics to database (you might want to implement this)
	// store.SavePipelineMetrics(pt.Metrics)
}

// updateStageDurations updates the duration for each stage
func (pt *PipelineTracker) updateStageDurations() {
	stages := []*StageMetrics{
		&pt.Metrics.IngestionMetrics,
		&pt.Metrics.ValidationMetrics,
		&pt.Metrics.TransformMetrics,
		&pt.Metrics.AggregationMetrics,
		&pt.Metrics.ExportMetrics,
	}
	
	for _, stage := range stages {
		if stage.EndTime == nil && !stage.StartTime.IsZero() {
			stage.Duration = time.Since(stage.StartTime)
		} else if stage.EndTime != nil {
			stage.Duration = stage.EndTime.Sub(stage.StartTime)
		}
		
		// Calculate stage records per second
		if stage.Duration > 0 && stage.RecordsProcessed > 0 {
			stage.RecordsPerSecond = float64(stage.RecordsProcessed) / stage.Duration.Seconds()
		}
	}
}

// StartStage marks the start of a pipeline stage
func (pt *PipelineTracker) StartStage(stage string, workerCount int) {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	now := time.Now()
	
	switch stage {
	case "ingestion":
		pt.Metrics.IngestionMetrics.StartTime = now
		pt.Metrics.IngestionMetrics.WorkerCount = workerCount
		pt.Metrics.IngestionMetrics.Status = "running"
	case "validation":
		pt.Metrics.ValidationMetrics.StartTime = now
		pt.Metrics.ValidationMetrics.WorkerCount = workerCount
		pt.Metrics.ValidationMetrics.Status = "running"
	case "transformation":
		pt.Metrics.TransformMetrics.StartTime = now
		pt.Metrics.TransformMetrics.WorkerCount = workerCount
		pt.Metrics.TransformMetrics.Status = "running"
	case "aggregation":
		pt.Metrics.AggregationMetrics.StartTime = now
		pt.Metrics.AggregationMetrics.WorkerCount = workerCount
		pt.Metrics.AggregationMetrics.Status = "running"
	case "export":
		pt.Metrics.ExportMetrics.StartTime = now
		pt.Metrics.ExportMetrics.WorkerCount = workerCount
		pt.Metrics.ExportMetrics.Status = "running"
	}
	
	fmt.Printf("📊 Stage '%s' started with %d workers\n", stage, workerCount)
}

// EndStage marks the end of a pipeline stage
func (pt *PipelineTracker) EndStage(stage string, recordsProcessed int64) {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	now := time.Now()
	
	switch stage {
	case "ingestion":
		pt.Metrics.IngestionMetrics.EndTime = &now
		pt.Metrics.IngestionMetrics.RecordsProcessed = recordsProcessed
		pt.Metrics.IngestionMetrics.Status = "completed"
		pt.Metrics.TotalRecords += recordsProcessed
	case "validation":
		pt.Metrics.ValidationMetrics.EndTime = &now
		pt.Metrics.ValidationMetrics.RecordsProcessed = recordsProcessed
		pt.Metrics.ValidationMetrics.Status = "completed"
		pt.Metrics.ValidRecords += recordsProcessed
	case "transformation":
		pt.Metrics.TransformMetrics.EndTime = &now
		pt.Metrics.TransformMetrics.RecordsProcessed = recordsProcessed
		pt.Metrics.TransformMetrics.Status = "completed"
		pt.Metrics.TransformedRecords += recordsProcessed
	case "aggregation":
		pt.Metrics.AggregationMetrics.EndTime = &now
		pt.Metrics.AggregationMetrics.RecordsProcessed = recordsProcessed
		pt.Metrics.AggregationMetrics.Status = "completed"
		pt.Metrics.AggregatedGroups += recordsProcessed
	case "export":
		pt.Metrics.ExportMetrics.EndTime = &now
		pt.Metrics.ExportMetrics.RecordsProcessed = recordsProcessed
		pt.Metrics.ExportMetrics.Status = "completed"
		pt.Metrics.ExportedRecords += recordsProcessed
	}
	
	fmt.Printf("📊 Stage '%s' completed: %d records processed\n", stage, recordsProcessed)
}

// RecordError records an error with detailed context
func (pt *PipelineTracker) RecordError(stage, errorType, message, sourceURL string, recordData map[string]interface{}, retryable bool) {
	errorDetail := ErrorDetail{
		Timestamp:    time.Now(),
		Stage:        stage,
		SourceURL:    sourceURL,
		ErrorType:    errorType,
		ErrorMessage: message,
		RecordData:   recordData,
		Retryable:    retryable,
		Severity:     determineSeverity(errorType, stage),
	}
	
	select {
	case pt.ErrorChannel <- errorDetail:
	default:
		// Channel is full, log the error
		fmt.Printf("⚠️ Error channel full, dropping error: %s\n", message)
	}
}

// UpdateSourceMetrics updates metrics for a specific source
func (pt *PipelineTracker) UpdateSourceMetrics(sourceURL string, recordsIngested, recordsValid, recordsInvalid int64, ingestionTime time.Duration) {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	if sourceMetrics, exists := pt.Metrics.SourceMetrics[sourceURL]; exists {
		sourceMetrics.RecordsIngested += recordsIngested
		sourceMetrics.RecordsValid += recordsValid
		sourceMetrics.RecordsInvalid += recordsInvalid
		sourceMetrics.IngestionTime += ingestionTime
		
		// Calculate average record size (simplified)
		if sourceMetrics.RecordsIngested > 0 {
			sourceMetrics.AverageRecordSize = 1024 // Placeholder
		}
		
		pt.Metrics.SourceMetrics[sourceURL] = sourceMetrics
	}
}

// Complete marks the pipeline as completed
func (pt *PipelineTracker) Complete() {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	now := time.Now()
	pt.Metrics.EndTime = &now
	pt.Metrics.Status = "completed"
	pt.Metrics.Duration = now.Sub(pt.Metrics.StartTime)
	
	// Final metrics update
	pt.updateStageDurations()
	
	fmt.Printf("📊 Pipeline completed in %v\n", pt.Metrics.Duration)
	fmt.Printf("📊 Total records: %d, Valid: %d, Invalid: %d, Errors: %d\n", 
		pt.Metrics.TotalRecords, pt.Metrics.ValidRecords, pt.Metrics.InvalidRecords, pt.Metrics.ErrorCount)
}

// Fail marks the pipeline as failed
func (pt *PipelineTracker) Fail() {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	now := time.Now()
	pt.Metrics.EndTime = &now
	pt.Metrics.Status = "failed"
	pt.Metrics.Duration = now.Sub(pt.Metrics.StartTime)
	
	fmt.Printf("❌ Pipeline failed after %v\n", pt.Metrics.Duration)
}

// GetMetrics returns current pipeline metrics
func (pt *PipelineTracker) GetMetrics() PipelineMetrics {
	pt.Mutex.RLock()
	defer pt.Mutex.RUnlock()
	
	// Create a copy to avoid race conditions
	metrics := *pt.Metrics
	return metrics
}

// ShouldRetry determines if the pipeline should be retried based on error analysis
func (pt *PipelineTracker) ShouldRetry() bool {
	pt.Mutex.RLock()
	defer pt.Mutex.RUnlock()
	
	// Don't retry if already retried too many times
	if pt.Metrics.RetryAttempts >= 3 {
		return false
	}
	
	// Don't retry if there are critical errors
	for _, error := range pt.Metrics.Errors {
		if error.Severity == "critical" {
			return false
		}
	}
	
	// Retry if there are retryable errors
	for _, error := range pt.Metrics.Errors {
		if error.Retryable {
			return true
		}
	}
	
	return false
}

// Retry increments retry count and updates retry time
func (pt *PipelineTracker) Retry() {
	pt.Mutex.Lock()
	defer pt.Mutex.Unlock()
	
	pt.Metrics.RetryAttempts++
	now := time.Now()
	pt.Metrics.LastRetryTime = &now
	
	fmt.Printf("🔄 Pipeline retry attempt %d\n", pt.Metrics.RetryAttempts)
}

// Stop stops the tracker and cleans up resources
func (pt *PipelineTracker) Stop() {
	pt.Cancel()
	close(pt.ErrorChannel)
	close(pt.UpdateChannel)
}

// Helper function to determine error severity
func determineSeverity(errorType, stage string) string {
	// Critical errors that should stop the pipeline
	criticalErrors := []string{"database_connection", "file_system", "memory_allocation", "network_timeout"}
	for _, critical := range criticalErrors {
		if errorType == critical {
			return "critical"
		}
	}
	
	// High severity errors that might cause data loss
	highErrors := []string{"data_corruption", "validation_failure", "transformation_error"}
	for _, high := range highErrors {
		if errorType == high {
			return "high"
		}
	}
	
	// Medium severity errors that might affect performance
	mediumErrors := []string{"rate_limit", "temporary_failure", "retryable_error"}
	for _, medium := range mediumErrors {
		if errorType == medium {
			return "medium"
		}
	}
	
	// Default to low severity
	return "low"
}
