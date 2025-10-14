package handler

import (
	"encoding/json"
	"net/http"
)

func CreatePipeline(w http.ResponseWriter, r *http.Request) {
	// For now, just respond with a dummy message
	response := map[string]string{"message": "Pipeline created successfully!"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func ListPipelines(w http.ResponseWriter, r *http.Request) {
	response := []map[string]string{
		{"id": "1", "status": "completed"},
		{"id": "2", "status": "running"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
