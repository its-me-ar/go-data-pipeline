package main

import (
	"log"
	"net/http"

	"github.com/its-me-ar/go-data-pipeline/internal/api"
)

func main() {
	r := api.NewRouter()

	log.Println("🚀 Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
