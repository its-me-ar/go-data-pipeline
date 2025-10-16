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
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// POST /api/v1/pipelines
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
	defer cancel()

	go func() {
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

// GET /api/v1/pipelines
func ListPipelines(w http.ResponseWriter, r *http.Request) {
	jobs, err := store.ListJobs()
	if err != nil {
		http.Error(w, "Failed to fetch pipelines", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// GET /api/v1/pipelines/{id}
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

// GET /api/v1/pipelines/{id}/errors
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

// GET /api/v1/pipelines/{id}/results
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

// POST /api/v1/pipelines/{id}/retry
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

// GET /api/v1/pipelines/{id}/summary
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
