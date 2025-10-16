package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/pkg/utils"
	"sync"
)

// ValidateRecords validates data from ingestion according to per-source rules.
func ValidateRecords(
	ctx context.Context,
	sources []model.Source,
	in <-chan GenericRecord,
	out chan<- GenericRecord,
	errs chan<- error,
	workerCount int,
) {
	sourceMap := make(map[string]*model.ValidationRules)
	for _, src := range sources {
		if src.Validation != nil {
			sourceMap[src.URL] = src.Validation
		}
	}

	var wg sync.WaitGroup
	wg.Add(workerCount)

	// Track validation stats
	var validCount, invalidCount int64
	var mu sync.Mutex

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer wg.Done()
			workerValidCount := 0
			workerInvalidCount := 0

			for rec := range in {
				select {
				case <-ctx.Done():
					return
				default:
					sourceURL, _ := rec["SourceURL"].(string)
					if valid, err := validateRecord(rec, sourceMap[sourceURL]); valid {
						out <- rec
						workerValidCount++
						if workerValidCount%100 == 0 || workerValidCount <= 10 {
							fmt.Printf("✅ Validation Worker %d: %d valid records processed\n", workerID, workerValidCount)
						}
					} else if err != nil {
						workerInvalidCount++
						if workerInvalidCount <= 5 {
							fmt.Printf("❌ Validation Worker %d: Invalid record from %s - %v\n", workerID, sourceURL, err)
						}
						errs <- fmt.Errorf("validation failed for source %s: %w", sourceURL, err)
					}
				}
			}

			// Update global counters
			mu.Lock()
			validCount += int64(workerValidCount)
			invalidCount += int64(workerInvalidCount)
			mu.Unlock()

			fmt.Printf("🔍 Validation Worker %d completed: %d valid, %d invalid\n", workerID, workerValidCount, workerInvalidCount)
		}(i)
	}

	// Close the output channel only AFTER all workers finish
	go func() {
		wg.Wait()
		mu.Lock()
		fmt.Printf("🔍 Validation Summary: %d valid records, %d invalid records\n", validCount, invalidCount)
		mu.Unlock()
		close(out)
	}()
}

// validateRecord applies per-source validation rules to a record.
func validateRecord(rec GenericRecord, rules *model.ValidationRules) (bool, error) {
	// Removed verbose logging for better performance

	if rules == nil {
		// No validation rules defined → pass through
		return true, nil
	}

	// Check required fields
	for _, field := range rules.RequiredFields {
		if _, ok := rec[field]; !ok {
			return false, fmt.Errorf("missing required field: %s", field)
		}
	}

	// Check numeric fields
	for _, field := range rules.NumericFields {
		val, ok := rec[field]
		if !ok {
			continue
		}
		switch val.(type) {
		case float64, float32, int, int64:
			// ok
		default:
			return false, fmt.Errorf("field %s must be numeric, got %T", field, val)
		}
	}

	// Check min values
	for field, min := range rules.MinValues {
		if val, ok := rec[field]; ok {
			if utils.Numeric(val) < min {
				return false, fmt.Errorf("field %s below minimum: got %v, want ≥ %v", field, val, min)
			}
		}
	}

	// Check max values
	for field, max := range rules.MaxValues {
		if val, ok := rec[field]; ok {
			if utils.Numeric(val) > max {
				return false, fmt.Errorf("field %s above maximum: got %v, want ≤ %v", field, val, max)
			}
		}
	}

	return true, nil
}
