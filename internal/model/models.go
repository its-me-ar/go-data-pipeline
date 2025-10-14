package model

// Record represents a single input record
type Record struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

// Source represents a data source for the pipeline
type Source struct {
	Type string `json:"type"` // csv, json, api
	URL  string `json:"url"`
}

// Aggregation defines how to aggregate data
type Aggregation struct {
	GroupBy string   `json:"groupBy"`
	Metrics []string `json:"metrics"`
}

// Export defines export targets
type Export struct {
	DB   string `json:"db"`   // e.g., postgres, sqlite
	File string `json:"file"` // e.g., output.csv
}

// Workers defines number of workers per stage
type Workers struct {
	Validation int `json:"validation"`
	Transform  int `json:"transform"`
}

// PipelineJobSpec is the struct for POST /api/v1/pipelines
type PipelineJobSpec struct {
	Sources         []Source    `json:"sources"`
	Transformations []string    `json:"transformations"`
	Aggregation     Aggregation `json:"aggregation"`
	Export          Export      `json:"export"`
	Workers         Workers     `json:"workers"`
}
