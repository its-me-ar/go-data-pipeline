package pipeline

import (
	"context"
	"fmt"
	"go-data-pipeline/internal/model"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// AggregatedResult represents the result of data aggregation
type AggregatedResult struct {
	GroupKey    string                 `json:"group_key"`
	GroupValue  interface{}            `json:"group_value"`
	Metrics     map[string]interface{} `json:"metrics"`
	RecordCount int                    `json:"record_count"`
	SourceURL   string                 `json:"source_url,omitempty"`
}

// AggregationWorker handles aggregation for a specific group
type AggregationWorker struct {
	ID           int
	GroupBy      string
	Metrics      []string
	Results      map[string]*AggregatedResult
	Mutex        sync.RWMutex
	RecordCount  int
	ErrorCount   int
}

// AggregateRecords processes records and performs aggregation based on job configuration
func AggregateRecords(ctx context.Context, in <-chan GenericRecord, job model.PipelineJobSpec, workerCount int) <-chan AggregatedResult {
	out := make(chan AggregatedResult, 100)
	
	// If no aggregation config, just pass through records as individual results
	if job.Aggregation == nil {
		go func() {
			defer close(out)
			count := 0
			for rec := range in {
				select {
				case <-ctx.Done():
					return
				default:
					result := AggregatedResult{
						GroupKey:    "individual",
						GroupValue:  fmt.Sprintf("record_%d", count),
						Metrics:     convertRecordToMetrics(rec),
						RecordCount: 1,
						SourceURL:   getSourceURL(rec),
					}
					out <- result
					count++
					if count%100 == 0 || count <= 10 {
						fmt.Printf("📊 Aggregation: Processed %d individual records\n", count)
					}
				}
			}
			fmt.Printf("📊 Aggregation Summary: %d individual records processed\n", count)
		}()
		return out
	}

	// Create aggregation workers
	workers := make([]*AggregationWorker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = &AggregationWorker{
			ID:      i + 1,
			GroupBy: job.Aggregation.GroupBy,
			Metrics: job.Aggregation.Metrics,
			Results: make(map[string]*AggregatedResult),
		}
	}

	// Distribute records to workers
	var wg sync.WaitGroup
	wg.Add(workerCount)
	
	for i := 0; i < workerCount; i++ {
		go func(worker *AggregationWorker) {
			defer wg.Done()
			
			for rec := range in {
				select {
				case <-ctx.Done():
					return
				default:
					worker.processRecord(rec)
					worker.RecordCount++
					
					if worker.RecordCount%100 == 0 || worker.RecordCount <= 10 {
						fmt.Printf("📊 Aggregation Worker %d: Processed %d records\n", worker.ID, worker.RecordCount)
					}
				}
			}
			
			fmt.Printf("📊 Aggregation Worker %d completed: %d records, %d errors\n", worker.ID, worker.RecordCount, worker.ErrorCount)
		}(workers[i])
	}

	// Collect results from all workers
	go func() {
		wg.Wait()
		
		// Merge results from all workers
		finalResults := make(map[string]*AggregatedResult)
		totalRecords := 0
		totalErrors := 0
		
		for _, worker := range workers {
			worker.Mutex.RLock()
			for key, result := range worker.Results {
				if existing, exists := finalResults[key]; exists {
					// Merge metrics
					for metric, value := range result.Metrics {
						if existingMetric, exists := existing.Metrics[metric]; exists {
							existing.Metrics[metric] = mergeMetricValues(existingMetric, value, metric)
						} else {
							existing.Metrics[metric] = value
						}
					}
					existing.RecordCount += result.RecordCount
				} else {
					finalResults[key] = result
				}
			}
			totalRecords += worker.RecordCount
			totalErrors += worker.ErrorCount
			worker.Mutex.RUnlock()
		}
		
		// Send results to output channel
		resultCount := 0
		for _, result := range finalResults {
			select {
			case <-ctx.Done():
				return
			case out <- *result:
				resultCount++
			}
		}
		
		fmt.Printf("📊 Aggregation Summary: %d groups created from %d records, %d errors\n", resultCount, totalRecords, totalErrors)
		close(out)
	}()

	return out
}

// processRecord processes a single record and updates aggregation results
func (w *AggregationWorker) processRecord(rec GenericRecord) {
	// Get group key value
	groupValue, exists := rec[w.GroupBy]
	if !exists {
		w.ErrorCount++
		return
	}
	
	groupKey := fmt.Sprintf("%v", groupValue)
	
	w.Mutex.Lock()
	defer w.Mutex.Unlock()
	
	// Get or create aggregation result for this group
	result, exists := w.Results[groupKey]
	if !exists {
		result = &AggregatedResult{
			GroupKey:   w.GroupBy,
			GroupValue: groupValue,
			Metrics:    make(map[string]interface{}),
			SourceURL:  getSourceURL(rec),
		}
		w.Results[groupKey] = result
	}
	
	// Update metrics
	for _, metric := range w.Metrics {
		updateMetric(result, rec, metric)
	}
	
	result.RecordCount++
}

// updateMetric updates a specific metric for a record
func updateMetric(result *AggregatedResult, rec GenericRecord, metric string) {
	switch strings.ToLower(metric) {
	case "count":
		// Count is handled by RecordCount
		return
	case "sum":
		updateSumMetric(result, rec)
	case "average", "avg":
		updateAverageMetric(result, rec)
	case "min":
		updateMinMetric(result, rec)
	case "max":
		updateMaxMetric(result, rec)
	case "first":
		updateFirstMetric(result, rec)
	case "last":
		updateLastMetric(result, rec)
	default:
		// Custom metric - try to find numeric fields
		updateCustomMetric(result, rec, metric)
	}
}

// updateSumMetric calculates sum of numeric fields
func updateSumMetric(result *AggregatedResult, rec GenericRecord) {
	for key, value := range rec {
		if isNumericField(key, value) {
			if num, ok := convertToFloat(value); ok {
				if existing, exists := result.Metrics["sum_"+key]; exists {
					if existingNum, ok := convertToFloat(existing); ok {
						result.Metrics["sum_"+key] = existingNum + num
					}
				} else {
					result.Metrics["sum_"+key] = num
				}
			}
		}
	}
}

// updateAverageMetric calculates average of numeric fields
func updateAverageMetric(result *AggregatedResult, rec GenericRecord) {
	for key, value := range rec {
		if isNumericField(key, value) {
			if num, ok := convertToFloat(value); ok {
				sumKey := "sum_" + key
				countKey := "count_" + key
				
				// Update sum
				if existing, exists := result.Metrics[sumKey]; exists {
					if existingNum, ok := convertToFloat(existing); ok {
						result.Metrics[sumKey] = existingNum + num
					}
				} else {
					result.Metrics[sumKey] = num
				}
				
				// Update count
				if existing, exists := result.Metrics[countKey]; exists {
					if count, ok := existing.(int); ok {
						result.Metrics[countKey] = count + 1
					}
				} else {
					result.Metrics[countKey] = 1
				}
				
				// Calculate average
				if sum, ok := convertToFloat(result.Metrics[sumKey]); ok {
					if count, ok := result.Metrics[countKey].(int); ok && count > 0 {
						result.Metrics["avg_"+key] = sum / float64(count)
					}
				}
			}
		}
	}
}

// updateMinMetric finds minimum values
func updateMinMetric(result *AggregatedResult, rec GenericRecord) {
	for key, value := range rec {
		if isNumericField(key, value) {
			if num, ok := convertToFloat(value); ok {
				minKey := "min_" + key
				if existing, exists := result.Metrics[minKey]; exists {
					if existingNum, ok := convertToFloat(existing); ok {
						if num < existingNum {
							result.Metrics[minKey] = num
						}
					}
				} else {
					result.Metrics[minKey] = num
				}
			}
		}
	}
}

// updateMaxMetric finds maximum values
func updateMaxMetric(result *AggregatedResult, rec GenericRecord) {
	for key, value := range rec {
		if isNumericField(key, value) {
			if num, ok := convertToFloat(value); ok {
				maxKey := "max_" + key
				if existing, exists := result.Metrics[maxKey]; exists {
					if existingNum, ok := convertToFloat(existing); ok {
						if num > existingNum {
							result.Metrics[maxKey] = num
						}
					}
				} else {
					result.Metrics[maxKey] = num
				}
			}
		}
	}
}

// updateFirstMetric stores first occurrence of values
func updateFirstMetric(result *AggregatedResult, rec GenericRecord) {
	for key, value := range rec {
		firstKey := "first_" + key
		if _, exists := result.Metrics[firstKey]; !exists {
			result.Metrics[firstKey] = value
		}
	}
}

// updateLastMetric stores last occurrence of values
func updateLastMetric(result *AggregatedResult, rec GenericRecord) {
	for key, value := range rec {
		lastKey := "last_" + key
		result.Metrics[lastKey] = value
	}
}

// updateCustomMetric handles custom metric calculations
func updateCustomMetric(result *AggregatedResult, rec GenericRecord, metric string) {
	// Try to find a field that matches the metric name
	for key, value := range rec {
		if strings.EqualFold(key, metric) {
			if isNumericField(key, value) {
				if num, ok := convertToFloat(value); ok {
					if existing, exists := result.Metrics[metric]; exists {
						if existingNum, ok := convertToFloat(existing); ok {
							result.Metrics[metric] = existingNum + num
						}
					} else {
						result.Metrics[metric] = num
					}
				}
			} else {
				// For non-numeric fields, just store the value
				result.Metrics[metric] = value
			}
			break
		}
	}
}

// Helper functions

func convertRecordToMetrics(rec GenericRecord) map[string]interface{} {
	metrics := make(map[string]interface{})
	for key, value := range rec {
		metrics[key] = value
	}
	return metrics
}

func getSourceURL(rec GenericRecord) string {
	if sourceURL, ok := rec["SourceURL"].(string); ok {
		return sourceURL
	}
	return ""
}

func isNumericField(key string, value interface{}) bool {
	// Skip certain fields that shouldn't be aggregated
	skipFields := []string{"SourceURL", "_processed_at", "_pipeline_version", "_record_id"}
	for _, skip := range skipFields {
		if strings.EqualFold(key, skip) {
			return false
		}
	}
	
	// Check if value is numeric
	_, isInt := value.(int)
	_, isFloat := value.(float64)
	_, isFloat32 := value.(float32)
	
	return isInt || isFloat || isFloat32
}

func convertToFloat(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func mergeMetricValues(existing, new interface{}, metric string) interface{} {
	// For sum metrics, add values
	if strings.HasPrefix(metric, "sum_") || strings.HasPrefix(metric, "avg_") {
		if existingNum, ok := convertToFloat(existing); ok {
			if newNum, ok := convertToFloat(new); ok {
				return existingNum + newNum
			}
		}
	}
	
	// For count metrics, add counts
	if strings.HasPrefix(metric, "count_") {
		if existingCount, ok := existing.(int); ok {
			if newCount, ok := new.(int); ok {
				return existingCount + newCount
			}
		}
	}
	
	// For min metrics, take minimum
	if strings.HasPrefix(metric, "min_") {
		if existingNum, ok := convertToFloat(existing); ok {
			if newNum, ok := convertToFloat(new); ok {
				if newNum < existingNum {
					return newNum
				}
				return existingNum
			}
		}
	}
	
	// For max metrics, take maximum
	if strings.HasPrefix(metric, "max_") {
		if existingNum, ok := convertToFloat(existing); ok {
			if newNum, ok := convertToFloat(new); ok {
				if newNum > existingNum {
					return newNum
				}
				return existingNum
			}
		}
	}
	
	// For other metrics, return the new value
	return new
}

// SortAggregatedResults sorts aggregation results by group value
func SortAggregatedResults(results []AggregatedResult, sortBy string, ascending bool) []AggregatedResult {
	sort.Slice(results, func(i, j int) bool {
		var iVal, jVal interface{}
		
		switch sortBy {
		case "group_value":
			iVal = results[i].GroupValue
			jVal = results[j].GroupValue
		case "record_count":
			iVal = results[i].RecordCount
			jVal = results[j].RecordCount
		default:
			// Try to find the metric in the results
			if metric, exists := results[i].Metrics[sortBy]; exists {
				iVal = metric
			} else {
				iVal = results[i].GroupValue
			}
			if metric, exists := results[j].Metrics[sortBy]; exists {
				jVal = metric
			} else {
				jVal = results[j].GroupValue
			}
		}
		
		// Convert to comparable values
		iFloat, iOk := convertToFloat(iVal)
		jFloat, jOk := convertToFloat(jVal)
		
		if iOk && jOk {
			if ascending {
				return iFloat < jFloat
			}
			return iFloat > jFloat
		}
		
		// String comparison
		iStr := fmt.Sprintf("%v", iVal)
		jStr := fmt.Sprintf("%v", jVal)
		
		if ascending {
			return iStr < jStr
		}
		return iStr > jStr
	})
	
	return results
}