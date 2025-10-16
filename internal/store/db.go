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
