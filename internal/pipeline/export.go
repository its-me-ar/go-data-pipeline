package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/store"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ExportResult represents the result of an export operation
type ExportResult struct {
	Type        string    `json:"type"`        // "database", "csv", "json"
	Path        string    `json:"path"`        // File path or table name
	RecordCount int       `json:"record_count"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
	ExportedAt  time.Time `json:"exported_at"`
}

// ExportManager handles data export operations
type ExportManager struct {
	JobID       string
	ExportSpec  *model.Export
	Results     []ExportResult
	Mutex       sync.RWMutex
	RecordCount int
	ErrorCount  int
}

// ExportData exports aggregated data based on job configuration
func ExportData(ctx context.Context, in <-chan AggregatedResult, job model.PipelineJobSpec, jobID string) <-chan ExportResult {
	out := make(chan ExportResult, 10)
	
	// If no export config, just consume the data
	if job.Export == nil {
		go func() {
			defer close(out)
			count := 0
			for range in {
				select {
				case <-ctx.Done():
					return
				default:
					count++
					if count%100 == 0 || count <= 10 {
						fmt.Printf("💾 Export: Consumed %d records (no export configured)\n", count)
					}
				}
			}
			fmt.Printf("💾 Export Summary: %d records consumed (no export configured)\n", count)
		}()
		return out
	}

	exportManager := &ExportManager{
		JobID:      jobID,
		ExportSpec: job.Export,
		Results:    make([]ExportResult, 0),
	}

	go func() {
		defer close(out)
		
		// Collect all data first for batch processing
		var allData []AggregatedResult
		for result := range in {
			select {
			case <-ctx.Done():
				return
			default:
				allData = append(allData, result)
				exportManager.RecordCount++
			}
		}
		
		fmt.Printf("💾 Export: Starting export of %d aggregated records\n", len(allData))
		
		// Export to different destinations based on configuration
		if exportManager.ExportSpec.File != "" {
			result := exportManager.exportToFile(ctx, allData)
			out <- result
		}
		
		if exportManager.ExportSpec.DB != "" {
			result := exportManager.exportToDatabase(ctx, allData)
			out <- result
		}
		
		// If no specific export configured, export to default CSV
		if exportManager.ExportSpec.File == "" && exportManager.ExportSpec.DB == "" {
			result := exportManager.exportToDefaultCSV(ctx, allData)
			out <- result
		}
		
		fmt.Printf("💾 Export Summary: %d records exported successfully\n", exportManager.RecordCount)
	}()

	return out
}

// exportToFile exports data to a file (CSV or JSON)
func (em *ExportManager) exportToFile(ctx context.Context, data []AggregatedResult) ExportResult {
	
	// Determine file type from extension
	ext := strings.ToLower(filepath.Ext(em.ExportSpec.File))
	
	var err error
	var recordCount int
	
	switch ext {
	case ".csv":
		recordCount, err = em.exportToCSV(data)
	case ".json":
		recordCount, err = em.exportToJSON(data)
	default:
		// Default to CSV if no extension or unknown extension
		recordCount, err = em.exportToCSV(data)
	}
	
	result := ExportResult{
		Type:        "file",
		Path:        em.ExportSpec.File,
		RecordCount: recordCount,
		Success:     err == nil,
		ExportedAt:  time.Now(),
	}
	
	if err != nil {
		result.Error = err.Error()
		fmt.Printf("❌ Export to file failed: %v\n", err)
	} else {
		fmt.Printf("✅ Export to file successful: %d records exported to %s\n", recordCount, em.ExportSpec.File)
	}
	
	return result
}

// exportToCSV exports data to CSV format
func (em *ExportManager) exportToCSV(data []AggregatedResult) (int, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(em.ExportSpec.File)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}
	
	file, err := os.Create(em.ExportSpec.File)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Write header
	header := []string{"group_key", "group_value", "record_count", "source_url"}
	
	// Collect all unique metric keys
	metricKeys := make(map[string]bool)
	for _, result := range data {
		for key := range result.Metrics {
			metricKeys[key] = true
		}
	}
	
	// Add metric keys to header
	for key := range metricKeys {
		header = append(header, key)
	}
	
	if err := writer.Write(header); err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}
	
	// Write data rows
	recordCount := 0
	for _, result := range data {
		row := []string{
			result.GroupKey,
			fmt.Sprintf("%v", result.GroupValue),
			strconv.Itoa(result.RecordCount),
			result.SourceURL,
		}
		
		// Add metric values
		for key := range metricKeys {
			if value, exists := result.Metrics[key]; exists {
				row = append(row, fmt.Sprintf("%v", value))
			} else {
				row = append(row, "")
			}
		}
		
		if err := writer.Write(row); err != nil {
			return recordCount, fmt.Errorf("failed to write row: %w", err)
		}
		recordCount++
	}
	
	return recordCount, nil
}

// exportToJSON exports data to JSON format
func (em *ExportManager) exportToJSON(data []AggregatedResult) (int, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(em.ExportSpec.File)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}
	
	file, err := os.Create(em.ExportSpec.File)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	
	// Create export metadata
	exportData := map[string]interface{}{
		"export_info": map[string]interface{}{
			"job_id":       em.JobID,
			"exported_at":  time.Now().UTC(),
			"record_count": len(data),
			"export_type":  "aggregated_results",
		},
		"data": data,
	}
	
	if err := encoder.Encode(exportData); err != nil {
		return 0, fmt.Errorf("failed to encode JSON: %w", err)
	}
	
	return len(data), nil
}

// exportToDatabase exports data to database
func (em *ExportManager) exportToDatabase(ctx context.Context, data []AggregatedResult) ExportResult {
	
	// For now, we'll store aggregated results in the existing SQLite database
	// In a production system, you might want to support other databases
	
	recordCount := 0
	var lastError error
	
	for _, result := range data {
		select {
		case <-ctx.Done():
			break
		default:
			// Store aggregated result in database
			err := store.SaveAggregatedResult(em.JobID, result)
			if err != nil {
				lastError = err
				em.ErrorCount++
				fmt.Printf("❌ Failed to save aggregated result: %v\n", err)
			} else {
				recordCount++
			}
		}
	}
	
	exportResult := ExportResult{
		Type:        "database",
		Path:        em.ExportSpec.DB,
		RecordCount: recordCount,
		Success:     lastError == nil,
		ExportedAt:  time.Now(),
	}
	
	if lastError != nil {
		exportResult.Error = lastError.Error()
		fmt.Printf("❌ Export to database failed: %v\n", lastError)
	} else {
		fmt.Printf("✅ Export to database successful: %d records exported\n", recordCount)
	}
	
	return exportResult
}

// exportToDefaultCSV exports data to a default CSV file
func (em *ExportManager) exportToDefaultCSV(ctx context.Context, data []AggregatedResult) ExportResult {
	// Create default filename with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	defaultFile := fmt.Sprintf("exports/pipeline_%s_%s.csv", em.JobID[:8], timestamp)
	
	// Temporarily set the file path
	originalFile := em.ExportSpec.File
	em.ExportSpec.File = defaultFile
	
	// Export to CSV
	result := em.exportToFile(ctx, data)
	
	// Restore original file path
	em.ExportSpec.File = originalFile
	
	return result
}

// ExportRawRecords exports raw records (for non-aggregated data)
func ExportRawRecords(ctx context.Context, in <-chan GenericRecord, job model.PipelineJobSpec, jobID string) <-chan ExportResult {
	out := make(chan ExportResult, 10)
	
	// If no export config, just consume the data
	if job.Export == nil {
		go func() {
			defer close(out)
			count := 0
			for range in {
				select {
				case <-ctx.Done():
					return
				default:
					count++
					if count%100 == 0 || count <= 10 {
						fmt.Printf("💾 Export: Consumed %d raw records (no export configured)\n", count)
					}
				}
			}
			fmt.Printf("💾 Export Summary: %d raw records consumed (no export configured)\n", count)
		}()
		return out
	}

	exportManager := &ExportManager{
		JobID:      jobID,
		ExportSpec: job.Export,
		Results:    make([]ExportResult, 0),
	}

	go func() {
		defer close(out)
		
		// Collect all data first for batch processing
		var allData []GenericRecord
		for record := range in {
			select {
			case <-ctx.Done():
				return
			default:
				allData = append(allData, record)
				exportManager.RecordCount++
			}
		}
		
		fmt.Printf("💾 Export: Starting export of %d raw records\n", len(allData))
		
		// Export to different destinations based on configuration
		if exportManager.ExportSpec.File != "" {
			result := exportManager.exportRawToFile(ctx, allData)
			out <- result
		}
		
		if exportManager.ExportSpec.DB != "" {
			result := exportManager.exportRawToDatabase(ctx, allData)
			out <- result
		}
		
		// If no specific export configured, export to default CSV
		if exportManager.ExportSpec.File == "" && exportManager.ExportSpec.DB == "" {
			result := exportManager.exportRawToDefaultCSV(ctx, allData)
			out <- result
		}
		
		fmt.Printf("💾 Export Summary: %d raw records exported successfully\n", exportManager.RecordCount)
	}()

	return out
}

// exportRawToFile exports raw records to a file
func (em *ExportManager) exportRawToFile(ctx context.Context, data []GenericRecord) ExportResult {
	
	// Determine file type from extension
	ext := strings.ToLower(filepath.Ext(em.ExportSpec.File))
	
	var err error
	var recordCount int
	
	switch ext {
	case ".csv":
		recordCount, err = em.exportRawToCSV(data)
	case ".json":
		recordCount, err = em.exportRawToJSON(data)
	default:
		// Default to CSV if no extension or unknown extension
		recordCount, err = em.exportRawToCSV(data)
	}
	
	result := ExportResult{
		Type:        "file",
		Path:        em.ExportSpec.File,
		RecordCount: recordCount,
		Success:     err == nil,
		ExportedAt:  time.Now(),
	}
	
	if err != nil {
		result.Error = err.Error()
		fmt.Printf("❌ Export raw data to file failed: %v\n", err)
	} else {
		fmt.Printf("✅ Export raw data to file successful: %d records exported to %s\n", recordCount, em.ExportSpec.File)
	}
	
	return result
}

// exportRawToCSV exports raw records to CSV format
func (em *ExportManager) exportRawToCSV(data []GenericRecord) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	
	// Create directory if it doesn't exist
	dir := filepath.Dir(em.ExportSpec.File)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}
	
	file, err := os.Create(em.ExportSpec.File)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Get all unique keys from all records
	allKeys := make(map[string]bool)
	for _, record := range data {
		for key := range record {
			allKeys[key] = true
		}
	}
	
	// Convert to sorted slice for consistent column order
	var header []string
	for key := range allKeys {
		header = append(header, key)
	}
	
	if err := writer.Write(header); err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}
	
	// Write data rows
	recordCount := 0
	for _, record := range data {
		var row []string
		for _, key := range header {
			if value, exists := record[key]; exists {
				row = append(row, fmt.Sprintf("%v", value))
			} else {
				row = append(row, "")
			}
		}
		
		if err := writer.Write(row); err != nil {
			return recordCount, fmt.Errorf("failed to write row: %w", err)
		}
		recordCount++
	}
	
	return recordCount, nil
}

// exportRawToJSON exports raw records to JSON format
func (em *ExportManager) exportRawToJSON(data []GenericRecord) (int, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(em.ExportSpec.File)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}
	
	file, err := os.Create(em.ExportSpec.File)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	
	// Create export metadata
	exportData := map[string]interface{}{
		"export_info": map[string]interface{}{
			"job_id":       em.JobID,
			"exported_at":  time.Now().UTC(),
			"record_count": len(data),
			"export_type":  "raw_records",
		},
		"data": data,
	}
	
	if err := encoder.Encode(exportData); err != nil {
		return 0, fmt.Errorf("failed to encode JSON: %w", err)
	}
	
	return len(data), nil
}

// exportRawToDatabase exports raw records to database
func (em *ExportManager) exportRawToDatabase(ctx context.Context, data []GenericRecord) ExportResult {
	
	recordCount := 0
	var lastError error
	
	for _, record := range data {
		select {
		case <-ctx.Done():
			break
		default:
			// Store raw record in database
			err := store.SaveRawRecord(em.JobID, record)
			if err != nil {
				lastError = err
				em.ErrorCount++
				fmt.Printf("❌ Failed to save raw record: %v\n", err)
			} else {
				recordCount++
			}
		}
	}
	
	exportResult := ExportResult{
		Type:        "database",
		Path:        em.ExportSpec.DB,
		RecordCount: recordCount,
		Success:     lastError == nil,
		ExportedAt:  time.Now(),
	}
	
	if lastError != nil {
		exportResult.Error = lastError.Error()
		fmt.Printf("❌ Export raw data to database failed: %v\n", lastError)
	} else {
		fmt.Printf("✅ Export raw data to database successful: %d records exported\n", recordCount)
	}
	
	return exportResult
}

// exportRawToDefaultCSV exports raw records to a default CSV file
func (em *ExportManager) exportRawToDefaultCSV(ctx context.Context, data []GenericRecord) ExportResult {
	// Create default filename with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	defaultFile := fmt.Sprintf("exports/raw_pipeline_%s_%s.csv", em.JobID[:8], timestamp)
	
	// Temporarily set the file path
	originalFile := em.ExportSpec.File
	em.ExportSpec.File = defaultFile
	
	// Export to CSV
	result := em.exportRawToFile(ctx, data)
	
	// Restore original file path
	em.ExportSpec.File = originalFile
	
	return result
}