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

// ------------------- Pipeline Runner -------------------
func Run(ctx context.Context, jobID string, job model.PipelineJobSpec) (err error) {
	start := time.Now()
	fmt.Printf("🚀 Starting pipeline for job: %s\n", jobID)

	// Update status to running
	store.UpdateJobStatus(jobID, "running")

	// Defer function to handle status updates on completion/error
	defer func() {
		if err != nil {
			store.UpdateJobStatus(jobID, "failed")
			store.SaveJobError(jobID, err)
		}
	}()

	// Parse job timeout
	timeout, err := time.ParseDuration(job.Concurrency.JobTimeout)
	if err != nil {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	recordsCh := make(chan GenericRecord, job.Concurrency.ChannelBufferSize)
	validatedCh := make(chan GenericRecord, job.Concurrency.ChannelBufferSize)
	errorCh := make(chan error, job.Concurrency.ChannelBufferSize)
	transformedCh := make(chan GenericRecord, job.Concurrency.ChannelBufferSize)

	var wg sync.WaitGroup

	// --- ERROR LOGGER ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errorCh {
			log.Printf("❌ Error in job %s: %v\n", jobID, err)
		}
	}()

	// --- INGESTION STAGE ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		store.UpdateJobStatus(jobID, "ingesting")
		store.SaveStageProgress(jobID, "ingestion", "started", &startTime, nil, 0, 0)
		store.SavePipelineLog(jobID, "ingestion", "info", "Starting ingestion stage", map[string]interface{}{
			"sources_count": len(job.Sources),
		})

		StartIngestion(ctx, job.Sources, recordsCh, errorCh)
		close(recordsCh) // safe: only this goroutine closes recordsCh

		endTime := time.Now()
		store.SaveStageProgress(jobID, "ingestion", "completed", &startTime, &endTime, 0, 0)
		store.SavePipelineLog(jobID, "ingestion", "info", "Ingestion stage completed", map[string]interface{}{
			"duration_ms": endTime.Sub(startTime).Milliseconds(),
		})
	}()

	// --- VALIDATION STAGE ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("🔍 Starting validation stage...")
		store.UpdateJobStatus(jobID, "validating")

		numWorkers := job.Concurrency.Workers.Validation
		if numWorkers == 0 {
			numWorkers = 3 // default
		}

		ValidateRecords(
			ctx,
			job.Sources,
			recordsCh,
			validatedCh,
			errorCh,
			numWorkers,
		)

		fmt.Println("✅ Validation stage setup complete.")
	}()

	// --- TRANSFORMATION STAGE ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("🔄 Starting transformation stage...")
		store.UpdateJobStatus(jobID, "transforming")

		numWorkers := job.Concurrency.Workers.Transform
		if numWorkers == 0 {
			numWorkers = 2 // default
		}

		TransformRecords(
			ctx,
			job.Transformations,
			validatedCh,
			transformedCh,
			errorCh,
			numWorkers,
		)

		fmt.Println("✅ Transformation stage setup complete.")
	}()

	// --- AGGREGATION STAGE ---
	aggregatedCh := make(chan AggregatedResult, 100)
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		fmt.Println("📊 Starting aggregation stage...")
		store.UpdateJobStatus(jobID, "aggregating")
		store.SaveStageProgress(jobID, "aggregation", "started", &startTime, nil, 0, 0)

		numWorkers := job.Concurrency.Workers.Aggregation
		if numWorkers == 0 {
			numWorkers = 2 // default
		}

		store.SavePipelineLog(jobID, "aggregation", "info", "Starting aggregation stage", map[string]interface{}{
			"group_by": job.Aggregation.GroupBy,
			"metrics":  job.Aggregation.Metrics,
			"workers":  numWorkers,
		})

		aggregatedResults := AggregateRecords(ctx, transformedCh, job, numWorkers)

		// Forward aggregated results to the channel
		resultCount := 0
		for result := range aggregatedResults {
			select {
			case <-ctx.Done():
				store.SavePipelineLog(jobID, "aggregation", "warning", "Aggregation cancelled", map[string]interface{}{
					"results_processed": resultCount,
				})
				return
			case aggregatedCh <- result:
				resultCount++
			}
		}

		endTime := time.Now()
		store.SaveStageProgress(jobID, "aggregation", "completed", &startTime, &endTime, resultCount, 0)
		store.SavePipelineLog(jobID, "aggregation", "info", "Aggregation stage completed", map[string]interface{}{
			"results_count": resultCount,
			"duration_ms":   endTime.Sub(startTime).Milliseconds(),
		})
		fmt.Println("✅ Aggregation stage complete.")
		close(aggregatedCh)
	}()

	// --- EXPORT STAGE ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("💾 Starting export stage...")
		store.UpdateJobStatus(jobID, "exporting")

		exportResults := ExportData(ctx, aggregatedCh, job, jobID)

		// Process export results
		exportCount := 0
		for result := range exportResults {
			exportCount++
			if result.Success {
				fmt.Printf("✅ Export %d: %d records exported to %s (%s)\n",
					exportCount, result.RecordCount, result.Path, result.Type)
			} else {
				fmt.Printf("❌ Export %d failed: %s\n", exportCount, result.Error)
			}
		}

		fmt.Printf("💾 Export Summary: %d export operations completed\n", exportCount)
	}()

	// Wait for all stages to finish
	wg.Wait()

	// Close errorCh at the very end
	close(errorCh)

	duration := time.Since(start)
	fmt.Printf("🏁 Pipeline completed successfully for job: %s in %v\n", jobID, duration)

	// Update status to completed
	store.UpdateJobStatus(jobID, "completed")
	return nil
}
