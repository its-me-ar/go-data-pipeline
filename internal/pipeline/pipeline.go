package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
)

// ------------------- Pipeline Runner -------------------
func Run(ctx context.Context, jobID string, job model.PipelineJobSpec) error {
	fmt.Printf("🚀 Starting pipeline for job: %s\n", jobID)

	// Channels
	recordsCh := make(chan GenericRecord, job.Concurrency.ChannelBufferSize)
	validatedCh := make(chan GenericRecord, job.Concurrency.ChannelBufferSize)
	errorCh := make(chan error, job.Concurrency.ChannelBufferSize)

	// Start ingestion
	go StartIngestion(ctx, job.Sources, recordsCh, errorCh)

	// Validation stage (example: just forward records)
	go func() {
		for rec := range recordsCh {
			// TODO: add real validation logic
			validatedCh <- rec
		}
		close(validatedCh)
	}()

	// Transformation stage (example: no-op)
	go func() {
		for rec := range validatedCh {
			// TODO: apply real transformations
			fmt.Printf("📝 Processed record: %+v\n", rec)
		}
	}()

	// Collect errors
	go func() {
		for err := range errorCh {
			fmt.Printf("❌ Error in job %s: %v\n", jobID, err)
			// TODO: store in DB
		}
	}()

	// Wait for pipeline completion or timeout
	<-ctx.Done()
	fmt.Printf("🏁 Pipeline finished or cancelled for job: %s\n", jobID)
	return nil
}
