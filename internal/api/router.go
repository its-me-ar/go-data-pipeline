package api

import (
	"go-data-pipeline/internal/api/handler"
	"go-data-pipeline/pkg/router"
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"
)

func RegisterRoutes(r *router.Router) {
	// Swagger documentation route
	r.GET("/swagger/*", func(w http.ResponseWriter, req *http.Request) {
		httpSwagger.WrapHandler(w, req)
	})

	r.POST("/api/v1/pipelines", handler.CreatePipeline)

	r.GET("/api/v1/pipelines", handler.ListPipelines)

	// More specific pipeline routes (must come before wildcard routes)
	r.GET("/api/v1/pipelines/*/errors", handler.GetPipelineErrors)
	r.GET("/api/v1/pipelines/*/results", handler.GetPipelineResults)
	r.GET("/api/v1/pipelines/*/records", handler.GetPipelineRecords)
	r.GET("/api/v1/pipelines/*/logs", handler.GetPipelineLogs)
	r.GET("/api/v1/pipelines/*/metrics", handler.GetPipelineMetrics)
	r.GET("/api/v1/pipelines/*/progress", handler.GetPipelineProgress)
	r.GET("/api/v1/pipelines/*/summary", handler.GetPipelineSummary)
	r.GET("/api/v1/pipelines/*/files", handler.GetJobFiles)
	r.POST("/api/v1/pipelines/*/retry", handler.RetryPipeline)
	r.PATCH("/api/v1/pipelines/*/cancel", handler.CancelPipeline)
	r.DELETE("/api/v1/pipelines/*", handler.DeletePipeline)

	// File download routes (after pipeline routes)
	r.GET("/api/v1/download/*", handler.DownloadFile)
	r.GET("/api/v1/files/*", handler.GetFileInfo)

	// Generic pipeline route last
	r.GET("/api/v1/pipelines/*", handler.GetPipeline)
}
