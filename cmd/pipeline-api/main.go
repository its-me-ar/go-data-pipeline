package main

import (
	"go-data-pipeline/internal/api"
)

func main() {
	r := api.NewRouter()
	r.Run(":8080") // Starts server on localhost:8080
}
