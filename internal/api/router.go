package api

import (
	"go-data-pipeline/internal/api/handler"
	"go-data-pipeline/pkg/router"
)

func RegisterRoutes(r *router.Router) {
	r.POST("/api/v1/pipelines", handler.CreatePipeline)
	r.GET("/api/v1/pipelines", handler.ListPipelines)
	// More specific routes first
	r.GET("/api/v1/pipelines/*/errors", handler.GetPipelineErrors)
	r.GET("/api/v1/pipelines/*/results", handler.GetPipelineResults)
	r.GET("/api/v1/pipelines/*/records", handler.GetPipelineRecords)
	r.GET("/api/v1/pipelines/*/logs", handler.GetPipelineLogs)
	r.GET("/api/v1/pipelines/*/metrics", handler.GetPipelineMetrics)
	r.GET("/api/v1/pipelines/*/progress", handler.GetPipelineProgress)
	r.GET("/api/v1/pipelines/*/summary", handler.GetPipelineSummary)
	r.POST("/api/v1/pipelines/*/retry", handler.RetryPipeline)
	// Generic pipeline route last
	r.GET("/api/v1/pipelines/*", handler.GetPipeline)
}
