package handler

import (
	"go-data-pipeline/internal/model"
	"net/http"

	"github.com/gin-gonic/gin"
)

// POST /api/v1/pipelines
func CreatePipeline(c *gin.Context) {
	var job model.PipelineJobSpec
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON payload"})
		return
	}

	// Respond with parsed job
	c.JSON(http.StatusOK, gin.H{
		"message":         "Pipeline created successfully!",
		"sources":         job.Sources,
		"transformations": job.Transformations,
		"aggregation":     job.Aggregation,
		"export":          job.Export,
		"workers":         job.Workers,
	})
}

// GET /api/v1/pipelines
func ListPipelines(c *gin.Context) {
	c.JSON(http.StatusOK, []gin.H{
		{"id": "1", "status": "completed"},
		{"id": "2", "status": "running"},
	})
}
