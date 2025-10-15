package store

import (
	"database/sql"
	"encoding/json"
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

	if _, err := db.Exec(jobTable); err != nil {
		return err
	}
	if _, err := db.Exec(errorTable); err != nil {
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
