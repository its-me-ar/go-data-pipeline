// @title 🚀 Go Data Pipeline API
// @version 2.0
// @description A modern, high-performance data pipeline API for processing, transforming, and aggregating data from multiple sources with real-time monitoring and advanced analytics capabilities.
// @termsOfService https://yourcompany.com/terms

// @contact.name API Support Team
// @contact.url https://yourcompany.com/support
// @contact.email api-support@yourcompany.com

// @license.name MIT
// @license.url https://opensource.org/licenses/MIT

// @host localhost:8080
// @BasePath /api/v1
// @schemes http https

// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Type "Bearer" followed by a space and JWT token.

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-API-Key
// @description API Key for authentication

package main

import (
	_ "go-data-pipeline/docs" // Import generated docs
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
