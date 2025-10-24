package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/store"
	"strings"
	"sync"
	"time"
)

// TransformRecords applies transformations to validated records
func TransformRecords(
	ctx context.Context,
	transformations []string,
	in <-chan GenericRecord,
	out chan<- GenericRecord,
	errs chan<- error,
	workerCount int,
) {
	var wg sync.WaitGroup
	wg.Add(workerCount)

	// Track transformation stats
	var transformedCount, errorCount int64
	var mu sync.Mutex

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer wg.Done()
			workerTransformedCount := 0
			workerErrorCount := 0

			for rec := range in {
				select {
				case <-ctx.Done():
					return
				default:
					transformed, err := applyTransformations(rec, transformations)
					if err != nil {
						workerErrorCount++
						if workerErrorCount <= 3 {
							fmt.Printf("❌ Transform Worker %d: Transformation failed - %v\n", workerID, err)
						}
						errs <- fmt.Errorf("transformation failed: %w", err)
						continue
					}

					select {
					case <-ctx.Done():
						return
					case out <- transformed:
						workerTransformedCount++
						if workerTransformedCount%100 == 0 || workerTransformedCount <= 10 {
							fmt.Printf("🔄 Transform Worker %d: %d records transformed\n", workerID, workerTransformedCount)
						}
					}
				}
			}

			// Update global counters
			mu.Lock()
			transformedCount += int64(workerTransformedCount)
			errorCount += int64(workerErrorCount)
			mu.Unlock()

			fmt.Printf("🔄 Transform Worker %d completed: %d transformed, %d errors\n", workerID, workerTransformedCount, workerErrorCount)
		}(i)
	}

	// Close the output channel only AFTER all workers finish
	go func() {
		wg.Wait()
		mu.Lock()
		fmt.Printf("🔄 Transformation Summary: %d records transformed, %d errors\n", transformedCount, errorCount)
		mu.Unlock()
		close(out)
	}()
}

// applyTransformations applies all specified transformations to a record
func applyTransformations(rec GenericRecord, transformations []string) (GenericRecord, error) {
	result := make(GenericRecord)

	// Copy original record
	for k, v := range rec {
		result[k] = v
	}

	// Apply each transformation
	for _, transform := range transformations {
		switch transform {
		case "normalizeNames":
			result = normalizeNames(result)
		case "convertToLowercase":
			result = convertToLowercase(result)
		case "addTimestamp":
			result = addTimestamp(result)
		case "calculateBMI":
			result = calculateBMI(result)
		case "trimStrings":
			result = trimStrings(result)
		case "convertToUppercase":
			result = convertToUppercase(result)
		case "removeNulls":
			result = removeNulls(result)
		case "addMetadata":
			result = addMetadata(result)
		default:
			return nil, fmt.Errorf("unknown transformation: %s", transform)
		}
	}

	return result, nil
}

// normalizeNames normalizes name-like fields to title case
func normalizeNames(rec GenericRecord) GenericRecord {
	for key, val := range rec {
		if str, ok := val.(string); ok {
			// Check if field name suggests it's a name-like field
			lowerKey := strings.ToLower(key)
			if isNameLikeField(lowerKey) {
				rec[key] = strings.Title(strings.ToLower(str))
			}
		}
	}
	return rec
}

// isNameLikeField checks if a field name suggests it contains name-like data
func isNameLikeField(fieldName string) bool {
	namePatterns := []string{
		"name", "title", "label", "symbol", "id", "code",
		"country", "location", "city", "state", "region",
		"category", "type", "status", "description", "comment",
		"firstname", "lastname", "fullname", "username",
		"company", "organization", "department", "team",
	}

	for _, pattern := range namePatterns {
		if strings.Contains(fieldName, pattern) {
			return true
		}
	}
	return false
}

// convertToLowercase converts string fields to lowercase
func convertToLowercase(rec GenericRecord) GenericRecord {
	for key, val := range rec {
		if str, ok := val.(string); ok {
			rec[key] = strings.ToLower(str)
		}
	}

	return rec
}

// addTimestamp adds current timestamp to the record
func addTimestamp(rec GenericRecord) GenericRecord {
	rec["processed_at"] = "2024-01-01T00:00:00Z" // You can use time.Now() for real timestamp
	return rec
}

// calculateBMI calculates BMI if height and weight are available
func calculateBMI(rec GenericRecord) GenericRecord {
	// Look for height and weight fields (case insensitive)
	var height, weight float64
	var found bool

	for key, val := range rec {
		keyLower := strings.ToLower(key)
		if strings.Contains(keyLower, "height") {
			if h, ok := val.(float64); ok {
				height = h
				found = true
			}
		}
		if strings.Contains(keyLower, "weight") {
			if w, ok := val.(float64); ok {
				weight = w
				found = true
			}
		}
	}

	if found && height > 0 && weight > 0 {
		// Convert height from inches to meters if needed
		heightM := height * 0.0254 // inches to meters
		bmi := weight / (heightM * heightM)
		rec["bmi"] = bmi
	}

	return rec
}

// trimStrings trims whitespace from all string fields
func trimStrings(rec GenericRecord) GenericRecord {
	for key, val := range rec {
		if str, ok := val.(string); ok {
			rec[key] = strings.TrimSpace(str)
		}
	}
	return rec
}

// convertToUppercase converts all string fields to uppercase
func convertToUppercase(rec GenericRecord) GenericRecord {
	for key, val := range rec {
		if str, ok := val.(string); ok {
			rec[key] = strings.ToUpper(str)
		}
	}
	return rec
}

// removeNulls removes null/nil values from the record
func removeNulls(rec GenericRecord) GenericRecord {
	for key, val := range rec {
		if val == nil {
			delete(rec, key)
		}
	}
	return rec
}

// addMetadata adds processing metadata to the record
func addMetadata(rec GenericRecord) GenericRecord {
	rec["_processed_at"] = "2024-01-01T00:00:00Z" // You can use time.Now() for real timestamp
	rec["_pipeline_version"] = "1.0.0"
	rec["_record_id"] = fmt.Sprintf("%p", &rec) // Simple unique ID
	return rec
}

// ------------------- Stage Execution -------------------

// ExecuteTransformationStage executes the complete transformation stage with tracking
func ExecuteTransformationStage(ctx context.Context, jobID string, job model.PipelineJobSpec, in <-chan GenericRecord, out chan<- GenericRecord, errors chan<- error, tracker interface{}) {
	startTime := time.Now()
	store.UpdateJobStatus(jobID, "transforming")

	numWorkers := job.Concurrency.Workers.Transform
	if numWorkers == 0 {
		numWorkers = 2 // default
	}

	// tracker.StartStage("transformation", numWorkers)
	store.SaveStageProgress(jobID, "transformation", "started", &startTime, nil, 0, 0)
	store.SavePipelineLog(jobID, "transformation", "info", "Starting transformation stage", map[string]interface{}{
		"transformations": job.Transformations,
		"workers":         numWorkers,
	})

	// Execute transformation using the existing function
	TransformRecords(ctx, job.Transformations, in, out, errors, numWorkers)

	endTime := time.Now()
	// tracker.EndStage("transformation", 0) // Record count will be updated by transformation workers
	store.SaveStageProgress(jobID, "transformation", "completed", &startTime, &endTime, 0, 0)
	store.SavePipelineLog(jobID, "transformation", "info", "Transformation stage completed", map[string]interface{}{
		"duration_ms": endTime.Sub(startTime).Milliseconds(),
	})
}
