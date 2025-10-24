package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/store"
	"log"
	"sync"
	"time"
)

// GenericRecord is a schema-agnostic map for any data source
type GenericRecord map[string]interface{}

// PipelineChannels holds all the channels used for data flow between stages
type PipelineChannels struct {
	records     chan GenericRecord
	validated   chan GenericRecord
	transformed chan GenericRecord
	aggregated  chan AggregatedResult
	errors      chan error
}

// ------------------- Pipeline Orchestrator -------------------

// Run orchestrates the entire data processing pipeline
func Run(ctx context.Context, jobID string, job model.PipelineJobSpec) (err error) {
	start := time.Now()
	fmt.Printf("🚀 Starting pipeline for job: %s\n", jobID)

	// Initialize pipeline tracker for monitoring
	tracker := NewPipelineTracker(jobID, job)
	defer tracker.Stop()

	// Update status to running
	store.UpdateJobStatus(jobID, "running")
	tracker.StartStage("pipeline", 1)

	// Defer function to handle status updates on completion/error
	defer func() {
		if err != nil {
			store.UpdateJobStatus(jobID, "failed")
			store.SaveJobError(jobID, err)
			tracker.RecordError("pipeline", "system", err.Error(), "", nil, false)
			tracker.Fail()
		} else {
			tracker.Complete()
		}
	}()

	// Create pipeline orchestrator (pass nil for tracker for now)
	orchestrator := NewPipelineOrchestrator(jobID, job, nil)

	// Execute the pipeline
	if err := orchestrator.Execute(ctx); err != nil {
		return err
	}

	duration := time.Since(start)
	fmt.Printf("🏁 Pipeline completed successfully for job: %s in %v\n", jobID, duration)

	// Update final status
	store.UpdateJobStatus(jobID, "completed")
	store.SavePipelineLog(jobID, "pipeline", "info", "Pipeline completed successfully", map[string]interface{}{
		"duration_ms": duration.Milliseconds(),
		"status":      "completed",
	})

	return nil
}

// PipelineOrchestrator manages the execution of all pipeline stages
type PipelineOrchestrator struct {
	jobID   string
	job     model.PipelineJobSpec
	tracker interface{} // Can be nil or *model.PipelineTracker
}

// NewPipelineOrchestrator creates a new pipeline orchestrator
func NewPipelineOrchestrator(jobID string, job model.PipelineJobSpec, tracker interface{}) *PipelineOrchestrator {
	return &PipelineOrchestrator{
		jobID:   jobID,
		job:     job,
		tracker: tracker,
	}
}

// Execute runs the entire pipeline by coordinating all stages
func (po *PipelineOrchestrator) Execute(ctx context.Context) error {
	// Create channels for data flow between stages
	channels := po.createChannels()

	// Start error logger
	errorLogger := po.startErrorLogger(ctx, channels.errors)
	defer errorLogger.Wait()

	// Execute all stages in sequence
	var wg sync.WaitGroup

	// Stage 1: Ingestion
	if err := po.executeIngestionStage(ctx, channels, &wg); err != nil {
		return err
	}

	// Stage 2: Validation
	if err := po.executeValidationStage(ctx, channels, &wg); err != nil {
		return err
	}

	// Stage 3: Transformation
	if err := po.executeTransformationStage(ctx, channels, &wg); err != nil {
		return err
	}

	// Stage 4: Aggregation
	if err := po.executeAggregationStage(ctx, channels, &wg); err != nil {
		return err
	}

	// Stage 5: Export
	if err := po.executeExportStage(ctx, channels, &wg); err != nil {
		return err
	}

	// Wait for all stages to complete
	wg.Wait()

	// Close error channel
	close(channels.errors)

	return nil
}

// createChannels creates all necessary channels for the pipeline
func (po *PipelineOrchestrator) createChannels() *PipelineChannels {
	bufferSize := po.job.Concurrency.ChannelBufferSize
	if bufferSize == 0 {
		bufferSize = 100 // Default buffer size
	}

	return &PipelineChannels{
		records:     make(chan GenericRecord, bufferSize),
		validated:   make(chan GenericRecord, bufferSize),
		transformed: make(chan GenericRecord, bufferSize),
		aggregated:  make(chan AggregatedResult, 100),
		errors:      make(chan error, bufferSize),
	}
}

// startErrorLogger starts a goroutine to log errors from the error channel
func (po *PipelineOrchestrator) startErrorLogger(ctx context.Context, errorCh <-chan error) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errorCh {
			log.Printf("❌ Error in job %s: %v\n", po.jobID, err)
		}
	}()
	return &wg
}

// executeIngestionStage runs the ingestion stage using the ingest.go file
func (po *PipelineOrchestrator) executeIngestionStage(ctx context.Context, channels *PipelineChannels, wg *sync.WaitGroup) error {
	ingestionComplete := make(chan struct{})
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(ingestionComplete)

		// Use the ingestion stage from ingest.go
		ExecuteIngestionStage(ctx, po.jobID, po.job, channels.records, channels.errors, nil)
		close(channels.records)
	}()

	// Wait for ingestion to complete before proceeding
	<-ingestionComplete
	return nil
}

// executeValidationStage runs the validation stage using the validate.go file
func (po *PipelineOrchestrator) executeValidationStage(ctx context.Context, channels *PipelineChannels, wg *sync.WaitGroup) error {
	wg.Add(1)

	go func() {
		defer wg.Done()

		// Use the validation stage from validate.go
		ExecuteValidationStage(ctx, po.jobID, po.job, channels.records, channels.validated, channels.errors, nil)
	}()

	return nil
}

// executeTransformationStage runs the transformation stage using the transform.go file
func (po *PipelineOrchestrator) executeTransformationStage(ctx context.Context, channels *PipelineChannels, wg *sync.WaitGroup) error {
	wg.Add(1)

	go func() {
		defer wg.Done()

		// Use the transformation stage from transform.go
		ExecuteTransformationStage(ctx, po.jobID, po.job, channels.validated, channels.transformed, channels.errors, nil)
	}()

	return nil
}

// executeAggregationStage runs the aggregation stage using the aggregate.go file
func (po *PipelineOrchestrator) executeAggregationStage(ctx context.Context, channels *PipelineChannels, wg *sync.WaitGroup) error {
	wg.Add(1)

	go func() {
		defer wg.Done()

		// Use the aggregation stage from aggregate.go
		ExecuteAggregationStage(ctx, po.jobID, po.job, channels.transformed, channels.aggregated, channels.errors, nil)
		close(channels.aggregated)
	}()

	return nil
}

// executeExportStage runs the export stage using the export.go file
func (po *PipelineOrchestrator) executeExportStage(ctx context.Context, channels *PipelineChannels, wg *sync.WaitGroup) error {
	wg.Add(1)

	go func() {
		defer wg.Done()

		// Use the export stage from export.go
		ExecuteExportStage(ctx, po.jobID, po.job, channels.aggregated, channels.errors, nil)
	}()

	return nil
}
