package handler

import (
	"context"
	"encoding/json"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/internal/pipeline"
	"go-data-pipeline/internal/store"
	"go-data-pipeline/pkg/utils"
	"net/http"
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
