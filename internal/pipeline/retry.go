package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/store"
	"math"
	"time"
)

// RetryConfig defines retry behavior for different types of operations
type RetryConfig struct {
	MaxAttempts        int           `json:"max_attempts"`
	InitialDelay       time.Duration `json:"initial_delay"`
	MaxDelay           time.Duration `json:"max_delay"`
	BackoffMultiplier  float64       `json:"backoff_multiplier"`
	Jitter             bool          `json:"jitter"`
	RetryableErrors    []string      `json:"retryable_errors"`
	NonRetryableErrors []string      `json:"non_retryable_errors"`
}

// RetryableOperation represents an operation that can be retried
type RetryableOperation struct {
	ID          string
	Type        string // "ingestion", "validation", "transformation", "aggregation", "export"
	SourceURL   string
	RecordData  map[string]interface{}
	Attempts    int
	LastError   error
	LastAttempt time.Time
	NextRetry   time.Time
	Config      RetryConfig
}

// RetryManager manages retry operations for the pipeline
type RetryManager struct {
	JobID              string
	Operations         map[string]*RetryableOperation
	RetryQueue         chan *RetryableOperation
	Context            context.Context
	Cancel             context.CancelFunc
	MaxConcurrentRetries int
	RetryConfigs       map[string]RetryConfig
}

// Default retry configurations for different operation types
var DefaultRetryConfigs = map[string]RetryConfig{
	"ingestion": {
		MaxAttempts:        3,
		InitialDelay:       1 * time.Second,
		MaxDelay:           30 * time.Second,
		BackoffMultiplier:  2.0,
		Jitter:             true,
		RetryableErrors:    []string{"network_timeout", "rate_limit", "temporary_failure", "connection_reset"},
		NonRetryableErrors: []string{"invalid_url", "authentication_failed", "permission_denied"},
	},
	"validation": {
		MaxAttempts:        2,
		InitialDelay:       500 * time.Millisecond,
		MaxDelay:           5 * time.Second,
		BackoffMultiplier:  1.5,
		Jitter:             false,
		RetryableErrors:    []string{"temporary_validation_error", "schema_mismatch"},
		NonRetryableErrors: []string{"invalid_data_format", "missing_required_field"},
	},
	"transformation": {
		MaxAttempts:        2,
		InitialDelay:       200 * time.Millisecond,
		MaxDelay:           2 * time.Second,
		BackoffMultiplier:  1.5,
		Jitter:             false,
		RetryableErrors:    []string{"transformation_timeout", "temporary_error"},
		NonRetryableErrors: []string{"invalid_transformation_rule", "data_type_error"},
	},
	"aggregation": {
		MaxAttempts:        2,
		InitialDelay:       1 * time.Second,
		MaxDelay:           10 * time.Second,
		BackoffMultiplier:  2.0,
		Jitter:             true,
		RetryableErrors:    []string{"aggregation_timeout", "memory_pressure"},
		NonRetryableErrors: []string{"invalid_group_by_field", "unsupported_metric"},
	},
	"export": {
		MaxAttempts:        3,
		InitialDelay:       2 * time.Second,
		MaxDelay:           60 * time.Second,
		BackoffMultiplier:  2.0,
		Jitter:             true,
		RetryableErrors:    []string{"file_write_error", "database_connection", "disk_full"},
		NonRetryableErrors: []string{"invalid_export_format", "permission_denied"},
	},
}

// NewRetryManager creates a new retry manager
func NewRetryManager(jobID string, maxConcurrentRetries int) *RetryManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	rm := &RetryManager{
		JobID:               jobID,
		Operations:          make(map[string]*RetryableOperation),
		RetryQueue:          make(chan *RetryableOperation, 1000),
		Context:             ctx,
		Cancel:              cancel,
		MaxConcurrentRetries: maxConcurrentRetries,
		RetryConfigs:        make(map[string]RetryConfig),
	}
	
	// Initialize with default configs
	for opType, config := range DefaultRetryConfigs {
		rm.RetryConfigs[opType] = config
	}
	
	// Start retry workers
	go rm.startRetryWorkers()
	
	return rm
}

// startRetryWorkers starts background workers to process retry operations
func (rm *RetryManager) startRetryWorkers() {
	// Start multiple retry workers
	for i := 0; i < rm.MaxConcurrentRetries; i++ {
		go rm.retryWorker(i)
	}
	
	// Start scheduler to check for operations ready to retry
	go rm.retryScheduler()
}

// retryWorker processes retry operations
func (rm *RetryManager) retryWorker(workerID int) {
	for {
		select {
		case <-rm.Context.Done():
			return
		case operation := <-rm.RetryQueue:
			rm.processRetryOperation(workerID, operation)
		}
	}
}

// retryScheduler periodically checks for operations ready to retry
func (rm *RetryManager) retryScheduler() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-rm.Context.Done():
			return
		case <-ticker.C:
			rm.checkRetryableOperations()
		}
	}
}

// checkRetryableOperations checks if any operations are ready to retry
func (rm *RetryManager) checkRetryableOperations() {
	now := time.Now()
	
	for _, operation := range rm.Operations {
		if operation.Attempts < operation.Config.MaxAttempts && 
		   now.After(operation.NextRetry) {
			select {
			case rm.RetryQueue <- operation:
			default:
				// Queue is full, skip this operation for now
			}
		}
	}
}

// processRetryOperation attempts to retry a failed operation
func (rm *RetryManager) processRetryOperation(workerID int, operation *RetryableOperation) {
	fmt.Printf("🔄 Retry Worker %d: Attempting to retry operation %s (attempt %d/%d)\n", 
		workerID, operation.ID, operation.Attempts+1, operation.Config.MaxAttempts)
	
	// Increment attempt count
	operation.Attempts++
	operation.LastAttempt = time.Now()
	
	// Attempt the operation based on type
	var err error
	switch operation.Type {
	case "ingestion":
		err = rm.retryIngestion(operation)
	case "validation":
		err = rm.retryValidation(operation)
	case "transformation":
		err = rm.retryTransformation(operation)
	case "aggregation":
		err = rm.retryAggregation(operation)
	case "export":
		err = rm.retryExport(operation)
	default:
		err = fmt.Errorf("unknown operation type: %s", operation.Type)
	}
	
	if err != nil {
		operation.LastError = err
		
		// Check if we should retry again
		if operation.Attempts < operation.Config.MaxAttempts && rm.isRetryableError(err, operation.Config) {
			// Calculate next retry time
			operation.NextRetry = rm.calculateNextRetryTime(operation)
			fmt.Printf("❌ Retry failed for operation %s: %v. Next retry at %v\n", 
				operation.ID, err, operation.NextRetry)
		} else {
			// Max attempts reached or non-retryable error
			fmt.Printf("❌ Operation %s failed permanently after %d attempts: %v\n", 
				operation.ID, operation.Attempts, err)
			
			// Remove from active operations
			delete(rm.Operations, operation.ID)
			
			// Save final error
			store.SaveJobError(rm.JobID, fmt.Errorf("retry failed for %s: %w", operation.ID, err))
		}
	} else {
		// Success!
		fmt.Printf("✅ Retry successful for operation %s after %d attempts\n", 
			operation.ID, operation.Attempts)
		
		// Remove from active operations
		delete(rm.Operations, operation.ID)
	}
}

// retryIngestion retries an ingestion operation
func (rm *RetryManager) retryIngestion(operation *RetryableOperation) error {
	// This would contain the actual retry logic for ingestion
	// For now, we'll simulate a retry
	fmt.Printf("🔄 Retrying ingestion from %s\n", operation.SourceURL)
	
	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)
	
	// Simulate success/failure (in real implementation, this would be actual retry logic)
	if operation.Attempts == 1 {
		return fmt.Errorf("simulated retry failure")
	}
	
	return nil // Success on second attempt
}

// retryValidation retries a validation operation
func (rm *RetryManager) retryValidation(operation *RetryableOperation) error {
	fmt.Printf("🔄 Retrying validation for record from %s\n", operation.SourceURL)
	
	// Simulate retry logic
	time.Sleep(50 * time.Millisecond)
	
	return nil // Assume success
}

// retryTransformation retries a transformation operation
func (rm *RetryManager) retryTransformation(operation *RetryableOperation) error {
	fmt.Printf("🔄 Retrying transformation for record from %s\n", operation.SourceURL)
	
	// Simulate retry logic
	time.Sleep(50 * time.Millisecond)
	
	return nil // Assume success
}

// retryAggregation retries an aggregation operation
func (rm *RetryManager) retryAggregation(operation *RetryableOperation) error {
	fmt.Printf("🔄 Retrying aggregation for group from %s\n", operation.SourceURL)
	
	// Simulate retry logic
	time.Sleep(100 * time.Millisecond)
	
	return nil // Assume success
}

// retryExport retries an export operation
func (rm *RetryManager) retryExport(operation *RetryableOperation) error {
	fmt.Printf("🔄 Retrying export operation %s\n", operation.ID)
	
	// Simulate retry logic
	time.Sleep(200 * time.Millisecond)
	
	return nil // Assume success
}

// ScheduleRetry schedules an operation for retry
func (rm *RetryManager) ScheduleRetry(operationType, sourceURL string, recordData map[string]interface{}, err error) string {
	operationID := fmt.Sprintf("%s_%s_%d", operationType, sourceURL, time.Now().UnixNano())
	
	config, exists := rm.RetryConfigs[operationType]
	if !exists {
		config = DefaultRetryConfigs["ingestion"] // Default config
	}
	
	operation := &RetryableOperation{
		ID:         operationID,
		Type:       operationType,
		SourceURL:  sourceURL,
		RecordData: recordData,
		Attempts:   0,
		LastError:  err,
		Config:     config,
		NextRetry:  time.Now().Add(config.InitialDelay),
	}
	
	rm.Operations[operationID] = operation
	
	fmt.Printf("📋 Scheduled retry for operation %s (type: %s, source: %s)\n", 
		operationID, operationType, sourceURL)
	
	return operationID
}

// calculateNextRetryTime calculates when the next retry should occur
func (rm *RetryManager) calculateNextRetryTime(operation *RetryableOperation) time.Time {
	config := operation.Config
	
	// Calculate delay with exponential backoff
	delay := time.Duration(float64(config.InitialDelay) * math.Pow(config.BackoffMultiplier, float64(operation.Attempts-1)))
	
	// Cap at max delay
	if delay > config.MaxDelay {
		delay = config.MaxDelay
	}
	
	// Add jitter if enabled
	if config.Jitter {
		jitter := time.Duration(float64(delay) * 0.1 * (0.5 - math.Mod(float64(time.Now().UnixNano()), 1.0)))
		delay += jitter
	}
	
	return time.Now().Add(delay)
}

// isRetryableError checks if an error is retryable based on the retry config
func (rm *RetryManager) isRetryableError(err error, config RetryConfig) bool {
	errorMsg := err.Error()
	
	// Check non-retryable errors first
	for _, nonRetryable := range config.NonRetryableErrors {
		if contains(errorMsg, nonRetryable) {
			return false
		}
	}
	
	// Check retryable errors
	for _, retryable := range config.RetryableErrors {
		if contains(errorMsg, retryable) {
			return true
		}
	}
	
	// Default to retryable for unknown errors
	return true
}

// GetRetryStats returns statistics about retry operations
func (rm *RetryManager) GetRetryStats() map[string]interface{} {
	stats := map[string]interface{}{
		"total_operations":     len(rm.Operations),
		"queued_operations":    len(rm.RetryQueue),
		"max_concurrent":       rm.MaxConcurrentRetries,
	}
	
	// Count operations by type
	typeCounts := make(map[string]int)
	for _, operation := range rm.Operations {
		typeCounts[operation.Type]++
	}
	stats["operations_by_type"] = typeCounts
	
	// Count operations by attempt count
	attemptCounts := make(map[int]int)
	for _, operation := range rm.Operations {
		attemptCounts[operation.Attempts]++
	}
	stats["operations_by_attempts"] = attemptCounts
	
	return stats
}

// Stop stops the retry manager
func (rm *RetryManager) Stop() {
	rm.Cancel()
	close(rm.RetryQueue)
}

// Helper function to check if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		   (s == substr || 
		    len(s) > len(substr) && 
		    (s[:len(substr)] == substr || 
		     s[len(s)-len(substr):] == substr || 
		     containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// RetryJob retries an entire failed job
func RetryJob(jobID string, job model.PipelineJobSpec) error {
	fmt.Printf("🔄 Retrying job %s\n", jobID)
	
	// Update job status to indicate retry
	store.UpdateJobStatus(jobID, "retrying")
	
	// Create new context for retry
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	
	// Run the pipeline again
	err := Run(ctx, jobID, job)
	
	if err != nil {
		store.UpdateJobStatus(jobID, "failed")
		store.SaveJobError(jobID, fmt.Errorf("retry failed: %w", err))
		return err
	}
	
	store.UpdateJobStatus(jobID, "completed")
	return nil
}
