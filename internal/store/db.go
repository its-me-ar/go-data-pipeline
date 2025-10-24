package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"go-data-pipeline/internal/model"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

// Initialize DB connection
func InitDB(dbPath string) error {
	var err error
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	// Create tables if not exists
	jobTable := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		spec TEXT,
		status TEXT,
		created_at DATETIME,
		updated_at DATETIME
	);
	`
	errorTable := `
	CREATE TABLE IF NOT EXISTS job_errors (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		error_message TEXT,
		created_at DATETIME
	);
	`
	aggregatedResultsTable := `
	CREATE TABLE IF NOT EXISTS aggregated_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		group_key TEXT,
		group_value TEXT,
		record_count INTEGER,
		source_url TEXT,
		metrics TEXT,
		created_at DATETIME
	);
	`
	rawRecordsTable := `
	CREATE TABLE IF NOT EXISTS raw_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		record_data TEXT,
		source_url TEXT,
		created_at DATETIME
	);
	`
	pipelineLogsTable := `
	CREATE TABLE IF NOT EXISTS pipeline_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		stage TEXT,
		level TEXT,
		message TEXT,
		metadata TEXT,
		created_at DATETIME
	);
	`
	pipelineMetricsTable := `
	CREATE TABLE IF NOT EXISTS pipeline_metrics (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		stage TEXT,
		records_processed INTEGER,
		records_per_second REAL,
		duration_ms INTEGER,
		worker_count INTEGER,
		error_count INTEGER,
		created_at DATETIME
	);
	`
	stageProgressTable := `
	CREATE TABLE IF NOT EXISTS stage_progress (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		stage TEXT,
		status TEXT,
		start_time DATETIME,
		end_time DATETIME,
		duration_ms INTEGER,
		records_processed INTEGER,
		error_count INTEGER,
		created_at DATETIME
	);
	`
	outputFilesTable := `
	CREATE TABLE IF NOT EXISTS output_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT,
		file_name TEXT,
		file_path TEXT,
		file_type TEXT,
		file_size INTEGER,
		download_url TEXT,
		created_at DATETIME,
		FOREIGN KEY (job_id) REFERENCES jobs (id)
	);
	`

	if _, err := db.Exec(jobTable); err != nil {
		return err
	}
	if _, err := db.Exec(errorTable); err != nil {
		return err
	}
	if _, err := db.Exec(aggregatedResultsTable); err != nil {
		return err
	}
	if _, err := db.Exec(rawRecordsTable); err != nil {
		return err
	}
	if _, err := db.Exec(pipelineLogsTable); err != nil {
		return err
	}
	if _, err := db.Exec(pipelineMetricsTable); err != nil {
		return err
	}
	if _, err := db.Exec(stageProgressTable); err != nil {
		return err
	}
	if _, err := db.Exec(outputFilesTable); err != nil {
		return err
	}

	return nil
}

// SaveJob stores a new pipeline job
func SaveJob(jobID string, spec model.PipelineJobSpec) error {
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	_, err = db.Exec(`INSERT INTO jobs (id, spec, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		jobID, specJSON, "pending", now, now)
	return err
}

// SaveJobError records an error for a job
func SaveJobError(jobID string, err error) error {
	if err == nil {
		return nil
	}
	now := time.Now().UTC()
	_, e := db.Exec(`INSERT INTO job_errors (job_id, error_message, created_at) VALUES (?, ?, ?)`,
		jobID, err.Error(), now)
	return e
}

// ListJobs returns all jobs with basic info
func ListJobs() ([]map[string]interface{}, error) {
	rows, err := db.Query(`SELECT id, status, created_at, updated_at FROM jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []map[string]interface{}
	for rows.Next() {
		var id, status string
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&id, &status, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, map[string]interface{}{
			"id":        id,
			"status":    status,
			"createdAt": createdAt,
			"updatedAt": updatedAt,
		})
	}
	return jobs, nil
}

// GetJob fetches full job spec and status
func GetJob(jobID string) (map[string]interface{}, error) {
	var specJSON string
	var status string
	var createdAt, updatedAt time.Time

	err := db.QueryRow(`SELECT spec, status, created_at, updated_at FROM jobs WHERE id = ?`, jobID).
		Scan(&specJSON, &status, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	var spec model.PipelineJobSpec
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"id":        jobID,
		"spec":      spec,
		"status":    status,
		"createdAt": createdAt,
		"updatedAt": updatedAt,
	}, nil
}

// UpdateJobStatus updates job status
func UpdateJobStatus(jobID string, status string) error {
	now := time.Now().UTC()
	_, err := db.Exec(`UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?`, status, now, jobID)
	return err
}

// SaveAggregatedResult saves an aggregated result to the database
func SaveAggregatedResult(jobID string, result interface{}) error {
	// Convert result to JSON
	metricsJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	_, err = db.Exec(`
		INSERT INTO aggregated_results (job_id, group_key, group_value, record_count, source_url, metrics, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, jobID, "aggregated", "result", 1, "", string(metricsJSON), now)

	return err
}

// SaveRawRecord saves a raw record to the database
func SaveRawRecord(jobID string, record interface{}) error {
	// Convert record to JSON
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Extract source URL if available
	sourceURL := ""
	if recordMap, ok := record.(map[string]interface{}); ok {
		if url, exists := recordMap["SourceURL"]; exists {
			if urlStr, ok := url.(string); ok {
				sourceURL = urlStr
			}
		}
	}

	now := time.Now().UTC()
	_, err = db.Exec(`
		INSERT INTO raw_records (job_id, record_data, source_url, created_at)
		VALUES (?, ?, ?, ?)
	`, jobID, string(recordJSON), sourceURL, now)

	return err
}

// GetAggregatedResults retrieves aggregated results for a job
func GetAggregatedResults(jobID string) ([]map[string]interface{}, error) {
	rows, err := db.Query(`
		SELECT group_key, group_value, record_count, source_url, metrics, created_at
		FROM aggregated_results
		WHERE job_id = ?
		ORDER BY created_at DESC
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var groupKey, groupValue, sourceURL, metricsJSON string
		var recordCount int
		var createdAt time.Time

		err := rows.Scan(&groupKey, &groupValue, &recordCount, &sourceURL, &metricsJSON, &createdAt)
		if err != nil {
			return nil, err
		}

		// Parse metrics JSON
		var metrics map[string]interface{}
		if err := json.Unmarshal([]byte(metricsJSON), &metrics); err != nil {
			metrics = make(map[string]interface{})
		}

		results = append(results, map[string]interface{}{
			"group_key":    groupKey,
			"group_value":  groupValue,
			"record_count": recordCount,
			"source_url":   sourceURL,
			"metrics":      metrics,
			"created_at":   createdAt,
		})
	}

	return results, nil
}

// GetRawRecords retrieves raw records for a job
func GetRawRecords(jobID string, limit int) ([]map[string]interface{}, error) {
	query := `
		SELECT record_data, source_url, created_at
		FROM raw_records
		WHERE job_id = ?
		ORDER BY created_at DESC
	`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.Query(query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []map[string]interface{}
	for rows.Next() {
		var recordJSON, sourceURL string
		var createdAt time.Time

		err := rows.Scan(&recordJSON, &sourceURL, &createdAt)
		if err != nil {
			return nil, err
		}

		// Parse record JSON
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(recordJSON), &record); err != nil {
			continue // Skip invalid records
		}

		records = append(records, map[string]interface{}{
			"record":     record,
			"source_url": sourceURL,
			"created_at": createdAt,
		})
	}

	return records, nil
}

// GetJobErrors retrieves errors for a job
func GetJobErrors(jobID string) ([]map[string]interface{}, error) {
	rows, err := db.Query(`
		SELECT error_message, created_at
		FROM job_errors
		WHERE job_id = ?
		ORDER BY created_at DESC
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var errors []map[string]interface{}
	for rows.Next() {
		var errorMessage string
		var createdAt time.Time

		err := rows.Scan(&errorMessage, &createdAt)
		if err != nil {
			return nil, err
		}

		errors = append(errors, map[string]interface{}{
			"error_message": errorMessage,
			"created_at":    createdAt,
		})
	}

	return errors, nil
}

// SavePipelineLog saves a pipeline log entry
func SavePipelineLog(jobID, stage, level, message string, metadata map[string]interface{}) error {
	metadataJSON := ""
	if metadata != nil {
		metadataBytes, err := json.Marshal(metadata)
		if err != nil {
			metadataJSON = "{}"
		} else {
			metadataJSON = string(metadataBytes)
		}
	}

	now := time.Now().UTC()
	_, err := db.Exec(`
		INSERT INTO pipeline_logs (job_id, stage, level, message, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, jobID, stage, level, message, metadataJSON, now)

	return err
}

// SavePipelineMetrics saves pipeline performance metrics
func SavePipelineMetrics(jobID, stage string, recordsProcessed int, recordsPerSecond float64, durationMs int, workerCount, errorCount int) error {
	now := time.Now().UTC()
	_, err := db.Exec(`
		INSERT INTO pipeline_metrics (job_id, stage, records_processed, records_per_second, duration_ms, worker_count, error_count, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, jobID, stage, recordsProcessed, recordsPerSecond, durationMs, workerCount, errorCount, now)

	return err
}

// SaveStageProgress saves stage progress information
func SaveStageProgress(jobID, stage, status string, startTime, endTime *time.Time, recordsProcessed, errorCount int) error {
	var startTimeStr, endTimeStr string
	var durationMs int

	if startTime != nil {
		startTimeStr = startTime.Format(time.RFC3339)
	}
	if endTime != nil {
		endTimeStr = endTime.Format(time.RFC3339)
		if startTime != nil {
			durationMs = int(endTime.Sub(*startTime).Milliseconds())
		}
	}

	now := time.Now().UTC()
	_, err := db.Exec(`
		INSERT INTO stage_progress (job_id, stage, status, start_time, end_time, duration_ms, records_processed, error_count, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, jobID, stage, status, startTimeStr, endTimeStr, durationMs, recordsProcessed, errorCount, now)

	return err
}

// GetPipelineLogs retrieves pipeline logs for a job
func GetPipelineLogs(jobID string, limit int) ([]map[string]interface{}, error) {
	query := `
		SELECT stage, level, message, metadata, created_at
		FROM pipeline_logs
		WHERE job_id = ?
		ORDER BY created_at DESC
	`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.Query(query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []map[string]interface{}
	for rows.Next() {
		var stage, level, message, metadataJSON string
		var createdAt time.Time

		err := rows.Scan(&stage, &level, &message, &metadataJSON, &createdAt)
		if err != nil {
			return nil, err
		}

		// Parse metadata JSON
		var metadata map[string]interface{}
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			metadata = make(map[string]interface{})
		}

		logs = append(logs, map[string]interface{}{
			"stage":      stage,
			"level":      level,
			"message":    message,
			"metadata":   metadata,
			"created_at": createdAt,
		})
	}

	return logs, nil
}

// GetPipelineMetrics retrieves pipeline metrics for a job
func GetPipelineMetrics(jobID string) ([]map[string]interface{}, error) {
	rows, err := db.Query(`
		SELECT stage, records_processed, records_per_second, duration_ms, worker_count, error_count, created_at
		FROM pipeline_metrics
		WHERE job_id = ?
		ORDER BY created_at ASC
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []map[string]interface{}
	for rows.Next() {
		var stage string
		var recordsProcessed, durationMs, workerCount, errorCount int
		var recordsPerSecond float64
		var createdAt time.Time

		err := rows.Scan(&stage, &recordsProcessed, &recordsPerSecond, &durationMs, &workerCount, &errorCount, &createdAt)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, map[string]interface{}{
			"stage":              stage,
			"records_processed":  recordsProcessed,
			"records_per_second": recordsPerSecond,
			"duration_ms":        durationMs,
			"worker_count":       workerCount,
			"error_count":        errorCount,
			"created_at":         createdAt,
		})
	}

	return metrics, nil
}

// GetStageProgress retrieves stage progress for a job
func GetStageProgress(jobID string) ([]map[string]interface{}, error) {
	rows, err := db.Query(`
		SELECT stage, status, start_time, end_time, duration_ms, records_processed, error_count, created_at
		FROM stage_progress
		WHERE job_id = ?
		ORDER BY created_at ASC
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var progress []map[string]interface{}
	for rows.Next() {
		var stage, status, startTimeStr, endTimeStr string
		var durationMs, recordsProcessed, errorCount int
		var createdAt time.Time

		err := rows.Scan(&stage, &status, &startTimeStr, &endTimeStr, &durationMs, &recordsProcessed, &errorCount, &createdAt)
		if err != nil {
			return nil, err
		}

		progress = append(progress, map[string]interface{}{
			"stage":             stage,
			"status":            status,
			"start_time":        startTimeStr,
			"end_time":          endTimeStr,
			"duration_ms":       durationMs,
			"records_processed": recordsProcessed,
			"error_count":       errorCount,
			"created_at":        createdAt,
		})
	}

	return progress, nil
}

// GetPipelineSummary retrieves a comprehensive summary of pipeline execution
func GetPipelineSummary(jobID string) (map[string]interface{}, error) {
	// Get job details
	job, err := GetJob(jobID)
	if err != nil {
		return nil, err
	}

	// Get logs
	logs, err := GetPipelineLogs(jobID, 50)
	if err != nil {
		logs = []map[string]interface{}{}
	}

	// Get metrics
	metrics, err := GetPipelineMetrics(jobID)
	if err != nil {
		metrics = []map[string]interface{}{}
	}

	// Get progress
	progress, err := GetStageProgress(jobID)
	if err != nil {
		progress = []map[string]interface{}{}
	}

	// Get errors
	errors, err := GetJobErrors(jobID)
	if err != nil {
		errors = []map[string]interface{}{}
	}

	// Get results
	results, err := GetAggregatedResults(jobID)
	if err != nil {
		results = []map[string]interface{}{}
	}

	// Get raw records count
	records, err := GetRawRecords(jobID, 0) // 0 means no limit, just count
	if err != nil {
		records = []map[string]interface{}{}
	}

	return map[string]interface{}{
		"job":      job,
		"logs":     logs,
		"metrics":  metrics,
		"progress": progress,
		"errors":   errors,
		"results":  results,
		"records": map[string]interface{}{
			"count": len(records),
			"data":  records,
		},
		"summary": map[string]interface{}{
			"total_logs":    len(logs),
			"total_metrics": len(metrics),
			"total_errors":  len(errors),
			"total_results": len(results),
			"total_records": len(records),
		},
	}, nil
}

// SaveOutputFile stores information about an output file
func SaveOutputFile(jobID, fileName, filePath, fileType string, fileSize int64, downloadURL string) error {
	query := `
		INSERT INTO output_files (job_id, file_name, file_path, file_type, file_size, download_url, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	_, err := db.Exec(query, jobID, fileName, filePath, fileType, fileSize, downloadURL, time.Now())
	return err
}

// GetOutputFiles retrieves all output files for a job
func GetOutputFiles(jobID string) ([]map[string]interface{}, error) {
	query := `
		SELECT id, job_id, file_name, file_path, file_type, file_size, download_url, created_at
		FROM output_files
		WHERE job_id = ?
		ORDER BY created_at DESC
	`
	rows, err := db.Query(query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []map[string]interface{}
	for rows.Next() {
		var id int
		var jobID, fileName, filePath, fileType, downloadURL string
		var fileSize int64
		var createdAt time.Time

		err := rows.Scan(&id, &jobID, &fileName, &filePath, &fileType, &fileSize, &downloadURL, &createdAt)
		if err != nil {
			return nil, err
		}

		files = append(files, map[string]interface{}{
			"id":           id,
			"job_id":       jobID,
			"file_name":    fileName,
			"file_path":    filePath,
			"file_type":    fileType,
			"file_size":    fileSize,
			"download_url": downloadURL,
			"created_at":   createdAt,
		})
	}

	return files, nil
}

// GetOutputFileByID retrieves a specific output file by ID
func GetOutputFileByID(fileID int) (map[string]interface{}, error) {
	query := `
		SELECT id, job_id, file_name, file_path, file_type, file_size, download_url, created_at
		FROM output_files
		WHERE id = ?
	`
	row := db.QueryRow(query, fileID)

	var id int
	var jobID, fileName, filePath, fileType, downloadURL string
	var fileSize int64
	var createdAt time.Time

	err := row.Scan(&id, &jobID, &fileName, &filePath, &fileType, &fileSize, &downloadURL, &createdAt)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"id":           id,
		"job_id":       jobID,
		"file_name":    fileName,
		"file_path":    filePath,
		"file_type":    fileType,
		"file_size":    fileSize,
		"download_url": downloadURL,
		"created_at":   createdAt,
	}, nil
}

// DeleteJob deletes a job and all its related data
func DeleteJob(jobID string) error {
	// Start a transaction to ensure atomicity
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete related records in the correct order (respecting foreign key constraints)

	// Delete output files
	if _, err := tx.Exec("DELETE FROM output_files WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Delete stage progress
	if _, err := tx.Exec("DELETE FROM stage_progress WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Delete pipeline metrics
	if _, err := tx.Exec("DELETE FROM pipeline_metrics WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Delete pipeline logs
	if _, err := tx.Exec("DELETE FROM pipeline_logs WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Delete raw records
	if _, err := tx.Exec("DELETE FROM raw_records WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Delete aggregated results
	if _, err := tx.Exec("DELETE FROM aggregated_results WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Delete job errors
	if _, err := tx.Exec("DELETE FROM job_errors WHERE job_id = ?", jobID); err != nil {
		return err
	}

	// Finally, delete the job itself
	if _, err := tx.Exec("DELETE FROM jobs WHERE id = ?", jobID); err != nil {
		return err
	}

	// Commit the transaction
	return tx.Commit()
}
