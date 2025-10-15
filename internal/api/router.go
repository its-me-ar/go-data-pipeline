package api

import (
	"go-data-pipeline/internal/api/handler"
	"go-data-pipeline/pkg/router"
)

func RegisterRoutes(r *router.Router) {
	r.POST("/api/v1/pipelines", handler.CreatePipeline)
	r.GET("/api/v1/pipelines", handler.ListPipelines)
}
