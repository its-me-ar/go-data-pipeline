package main

import (
	"go-data-pipeline/internal/api"
	"go-data-pipeline/internal/store"
	"go-data-pipeline/pkg/router"
)

func main() {
	// Init DB
	if err := store.InitDB("pipeline.db"); err != nil {
		panic(err)
	}

	// Create router
	r := router.New()

	// Register API routes
	api.RegisterRoutes(r)

	// Start server
	r.Start(":8080")
}
