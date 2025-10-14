package api

import (
	"github.com/gin-gonic/gin"

	"go-data-pipeline/internal/api/handler"
)

func NewRouter() *gin.Engine {
	r := gin.Default() // Gin router

	// API versioning group
	api := r.Group("/api/v1")
	{
		api.POST("/pipelines", handler.CreatePipeline)
		api.GET("/pipelines", handler.ListPipelines)
	}

	return r
}
