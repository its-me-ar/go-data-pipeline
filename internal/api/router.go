package api

import (
	"github.com/gorilla/mux"
	"github.com/its-me-ar/go-data-pipeline/internal/api/handler"
)

func NewRouter() *mux.Router {
	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()

	api.HandleFunc("/pipelines", handler.CreatePipeline).Methods("POST")
	api.HandleFunc("/pipelines", handler.ListPipelines).Methods("GET")

	return r
}
