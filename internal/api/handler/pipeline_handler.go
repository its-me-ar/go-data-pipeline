package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/pipeline"
	"go-data-pipeline/internal/store"
	"go-data-pipeline/pkg/utils"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CreatePipeline creates a new data pipeline job
// @Summary Create a new pipeline
// @Description Create and start a new data pipeline job with the provided configuration
// @Tags pipelines
// @Accept json
// @Produce json
// @Param pipeline body model.PipelineJobSpec true "Pipeline configuration"
// @Success 200 {object} map[string]interface{} "Pipeline created successfully"
// @Failure 400 {object} map[string]interface{} "Invalid request payload"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines [post]
func CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var job model.PipelineJobSpec
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// 1. Validate payload
	if len(job.Sources) == 0 {
		http.Error(w, "At least one source is required", http.StatusBadRequest)
		return
	}

	// 2. Generate job ID
	jobID := uuid.New().String()

	// 3. Save job to DB
	if err := store.SaveJob(jobID, job); err != nil {
		http.Error(w, "Failed to save job", http.StatusInternalServerError)
		return
	}

	// 4. Start pipeline asynchronously
	ctx, cancel := context.WithTimeout(context.Background(), utils.ParseDuration(job.Concurrency.JobTimeout))

	go func() {
		defer cancel() // Cancel context when pipeline completes
		if err := pipeline.Run(ctx, jobID, job); err != nil {
			// Log the error or store in job errors table
			store.SaveJobError(jobID, err)
		}
	}()

	// 5. Return response
	resp := map[string]interface{}{
		"message":   "Pipeline created successfully!",
		"jobID":     jobID,
		"status":    "pending",
		"createdAt": time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ListPipelines retrieves all pipeline jobs
// @Summary List all pipelines
// @Description Get a list of all pipeline jobs with their current status
// @Tags pipelines
// @Accept json
// @Produce json
// @Success 200 {array} map[string]interface{} "List of pipelines"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines [get]
func ListPipelines(w http.ResponseWriter, r *http.Request) {
	jobs, err := store.ListJobs()
	if err != nil {
		http.Error(w, "Failed to fetch pipelines", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// GetPipeline retrieves a specific pipeline job
// @Summary Get pipeline
// @Description Retrieve details of a specific pipeline job
// @Tags pipelines
// @Accept json
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success 200 {object} map[string]interface{} "Pipeline details"
// @Failure 400 {object} map[string]interface{} "Invalid pipeline ID"
// @Failure 404 {object} map[string]interface{} "Pipeline not found"
// @Router /pipelines/{id} [get]
func GetPipeline(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"

	if !strings.HasPrefix(path, prefix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix):]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	job, err := store.GetJob(jobID)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// GetPipelineErrors retrieves errors for a pipeline
// @Summary Get pipeline errors
// @Description Retrieve all errors that occurred during pipeline execution
// @Tags pipelines
// @Accept json
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success 200 {object} map[string]interface{} "Pipeline errors"
// @Failure 400 {object} map[string]interface{} "Invalid pipeline ID"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines/{id}/errors [get]
func GetPipelineErrors(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/errors"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	errors, err := store.GetJobErrors(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve errors", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id": jobID,
		"errors": errors,
		"count":  len(errors),
	})
}

// GetPipelineResults retrieves aggregated results for a pipeline
// @Summary Get pipeline results
// @Description Retrieve aggregated results for a specific pipeline job
// @Tags pipelines
// @Accept json
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success 200 {object} map[string]interface{} "Pipeline results"
// @Failure 400 {object} map[string]interface{} "Invalid pipeline ID"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines/{id}/results [get]
func GetPipelineResults(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/results"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	results, err := store.GetAggregatedResults(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve results", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":  jobID,
		"results": results,
		"count":   len(results),
	})
}

// GET /api/v1/pipelines/{id}/records
func GetPipelineRecords(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/records"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// Get limit from query parameter
	limit := 100 // default
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	records, err := store.GetRawRecords(jobID, limit)
	if err != nil {
		http.Error(w, "Failed to retrieve records", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":  jobID,
		"records": records,
		"count":   len(records),
		"limit":   limit,
	})
}

// RetryPipeline retries a failed or completed pipeline job
// @Summary Retry pipeline
// @Description Retry a pipeline job with the same configuration
// @Tags pipelines
// @Accept json
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success 200 {object} map[string]interface{} "Retry initiated"
// @Failure 400 {object} map[string]interface{} "Invalid pipeline ID"
// @Failure 404 {object} map[string]interface{} "Pipeline not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines/{id}/retry [post]
func RetryPipeline(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/retry"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// Get the job specification
	jobData, err := store.GetJob(jobID)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Extract job spec
	spec, ok := jobData["spec"].(model.PipelineJobSpec)
	if !ok {
		http.Error(w, "Invalid job specification", http.StatusInternalServerError)
		return
	}

	// Start retry in background
	go func() {
		err := pipeline.RetryJob(jobID, spec)
		if err != nil {
			fmt.Printf("❌ Retry failed for job %s: %v\n", jobID, err)
		} else {
			fmt.Printf("✅ Retry successful for job %s\n", jobID)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Retry initiated",
		"job_id":  jobID,
		"status":  "retrying",
	})
}

// GET /api/v1/pipelines/{id}/logs
func GetPipelineLogs(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/logs"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// Get limit from query parameter
	limit := 100 // default
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	logs, err := store.GetPipelineLogs(jobID, limit)
	if err != nil {
		http.Error(w, "Failed to retrieve logs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id": jobID,
		"logs":   logs,
		"count":  len(logs),
		"limit":  limit,
	})
}

// GET /api/v1/pipelines/{id}/metrics
func GetPipelineMetrics(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/metrics"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	metrics, err := store.GetPipelineMetrics(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve metrics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":  jobID,
		"metrics": metrics,
		"count":   len(metrics),
	})
}

// GET /api/v1/pipelines/{id}/progress
func GetPipelineProgress(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/progress"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	progress, err := store.GetStageProgress(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve progress", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":   jobID,
		"progress": progress,
		"count":    len(progress),
	})
}

// GetPipelineSummary retrieves comprehensive pipeline summary
func GetPipelineSummary(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	path := r.URL.Path
	prefix := "/api/v1/pipelines/"
	suffix := "/summary"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	jobID := path[len(prefix) : len(path)-len(suffix)]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	summary, err := store.GetPipelineSummary(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve summary", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

// GetJobFiles retrieves all output files for a specific job
func GetJobFiles(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 {
		http.Error(w, "Invalid URL format", http.StatusBadRequest)
		return
	}
	jobID := pathParts[3]

	// Get files from database
	files, err := store.GetOutputFiles(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve files", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"job_id": jobID,
		"files":  files,
		"count":  len(files),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// DownloadFile serves a file for download
// @Summary Download file
// @Description Download a specific output file from a pipeline job
// @Tags files
// @Accept json
// @Produce application/octet-stream
// @Param jobID path string true "Job ID"
// @Param filename path string true "File name"
// @Success 200 {file} file "File download"
// @Failure 400 {object} map[string]interface{} "Invalid URL format"
// @Failure 404 {object} map[string]interface{} "File not found"
// @Router /download/{jobID}/{filename} [get]
func DownloadFile(w http.ResponseWriter, r *http.Request) {
	// Extract job ID and filename from URL path
	// URL format: /api/v1/download/jobID/filename
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 5 {
		http.Error(w, fmt.Sprintf("Invalid URL format. Expected 5 parts, got %d: %v", len(pathParts), pathParts), http.StatusBadRequest)
		return
	}
	jobID := pathParts[3]
	fileName := pathParts[4]

	// Construct the file path
	filePath := fmt.Sprintf("outputs/%s/%s", jobID, fileName)

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Set appropriate headers for file download
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileName))
	w.Header().Set("Content-Type", "application/octet-stream")

	// Serve the file
	http.ServeFile(w, r, filePath)
}

// GetFileInfo retrieves information about a specific file
// @Summary Get file information
// @Description Get information about a specific file by ID or all files for a job ID
// @Tags files
// @Accept json
// @Produce json
// @Param id path string true "File ID (numeric) or Job ID (UUID)"
// @Success 200 {object} map[string]interface{} "File information"
// @Failure 400 {object} map[string]interface{} "Invalid file ID"
// @Failure 404 {object} map[string]interface{} "File not found"
// @Router /files/{id} [get]
func GetFileInfo(w http.ResponseWriter, r *http.Request) {
	// Extract file ID from URL path
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 {
		http.Error(w, "Invalid URL format", http.StatusBadRequest)
		return
	}

	fileIDStr := pathParts[3]

	// Try to parse as integer first (for numeric file IDs)
	if fileID, err := strconv.Atoi(fileIDStr); err == nil {
		// Get file info from database by numeric ID
		fileInfo, err := store.GetOutputFileByID(fileID)
		if err != nil {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fileInfo)
		return
	}

	// If not a numeric ID, treat as job ID and get all files for that job
	jobID := fileIDStr
	files, err := store.GetOutputFiles(jobID)
	if err != nil {
		http.Error(w, "Failed to retrieve files", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"job_id": jobID,
		"files":  files,
		"count":  len(files),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// CancelPipeline cancels a running pipeline job
// @Summary Cancel pipeline
// @Description Cancel a running pipeline job
// @Tags pipelines
// @Accept json
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success 200 {object} map[string]interface{} "Pipeline cancelled"
// @Failure 400 {object} map[string]interface{} "Invalid pipeline ID or status"
// @Failure 404 {object} map[string]interface{} "Pipeline not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines/{id}/cancel [patch]
func CancelPipeline(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 {
		http.Error(w, "Invalid URL format", http.StatusBadRequest)
		return
	}
	jobID := pathParts[3]

	// Check if job exists
	job, err := store.GetJob(jobID)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Check if job is in a cancellable state
	status, ok := job["status"].(string)
	if !ok {
		http.Error(w, "Invalid job status", http.StatusInternalServerError)
		return
	}

	if status == "completed" || status == "failed" || status == "cancelled" {
		http.Error(w, fmt.Sprintf("Job is already %s and cannot be cancelled", status), http.StatusBadRequest)
		return
	}

	// Update job status to cancelled
	if err := store.UpdateJobStatus(jobID, "cancelled"); err != nil {
		http.Error(w, "Failed to cancel job", http.StatusInternalServerError)
		return
	}

	// Log the cancellation
	store.SavePipelineLog(jobID, "pipeline", "info", "Pipeline cancelled by user", map[string]interface{}{
		"cancelled_at":    time.Now(),
		"previous_status": status,
	})

	response := map[string]interface{}{
		"message": "Pipeline cancelled successfully",
		"job_id":  jobID,
		"status":  "cancelled",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// DeletePipeline deletes a pipeline job and its artifacts
// @Summary Delete pipeline
// @Description Delete a pipeline job and all its associated files and data
// @Tags pipelines
// @Accept json
// @Produce json
// @Param id path string true "Pipeline ID"
// @Success 200 {object} map[string]interface{} "Pipeline deleted"
// @Failure 400 {object} map[string]interface{} "Invalid pipeline ID"
// @Failure 404 {object} map[string]interface{} "Pipeline not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /pipelines/{id} [delete]
func DeletePipeline(w http.ResponseWriter, r *http.Request) {
	// Extract job ID from URL path
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 4 {
		http.Error(w, "Invalid URL format", http.StatusBadRequest)
		return
	}
	jobID := pathParts[3]

	// Check if job exists
	_, err := store.GetJob(jobID)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Get all files for this job
	files, err := store.GetOutputFiles(jobID)
	if err != nil {
		// Log error but continue with deletion
		store.SavePipelineLog(jobID, "pipeline", "warning", "Failed to get files for deletion", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Delete physical files
	for _, file := range files {
		if filePath, ok := file["file_path"].(string); ok {
			if err := os.Remove(filePath); err != nil {
				// Log error but continue
				store.SavePipelineLog(jobID, "pipeline", "warning", "Failed to delete file", map[string]interface{}{
					"file_path": filePath,
					"error":     err.Error(),
				})
			}
		}
	}

	// Delete job directory if it exists
	jobDir := fmt.Sprintf("outputs/%s", jobID)
	if err := os.RemoveAll(jobDir); err != nil {
		// Log error but continue
		store.SavePipelineLog(jobID, "pipeline", "warning", "Failed to delete job directory", map[string]interface{}{
			"directory": jobDir,
			"error":     err.Error(),
		})
	}

	// Delete job from database (this will cascade delete related records due to foreign keys)
	if err := store.DeleteJob(jobID); err != nil {
		http.Error(w, "Failed to delete job from database", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"message":       "Pipeline and all artifacts deleted successfully",
		"job_id":        jobID,
		"files_deleted": len(files),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
