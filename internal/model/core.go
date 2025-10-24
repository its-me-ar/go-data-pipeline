package model

// GenericRecord is a schema-agnostic map for any data source
type GenericRecord map[string]interface{}

// Record represents a single input record
type Record struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

// ValidationRules defines validation requirements for a source
type ValidationRules struct {
	RequiredFields []string           `json:"requiredFields"` // fields that must be present
	NumericFields  []string           `json:"numericFields"`  // fields that must be numeric
	MinValues      map[string]float64 `json:"minValues"`      // min allowed numeric values
	MaxValues      map[string]float64 `json:"maxValues"`      // optional max limits
	CustomRules    map[string]string  `json:"customRules"`    // e.g., regex or expression
}

// Aggregation defines how to aggregate data (can be global or per source)
type Aggregation struct {
	GroupBy string   `json:"groupBy"`
	Metrics []string `json:"metrics"` // e.g., ["sum", "avg", "count"]
}

// Source represents a data source for the pipeline
type Source struct {
	Type        string           `json:"type"` // csv, json, api
	URL         string           `json:"url"`
	Validation  *ValidationRules `json:"validation,omitempty"`  // per-source validation
	Aggregation *Aggregation     `json:"aggregation,omitempty"` // per-source aggregation
}

// Export defines export targets
type Export struct {
	DB        string `json:"db"`        // e.g., postgres, sqlite
	File      string `json:"file"`      // e.g., output.csv
	BatchSize int    `json:"batchSize"` // optional batch size
}

// Workers defines number of workers per stage
type Workers struct {
	Ingest      int `json:"ingest"`
	Validation  int `json:"validation"`
	Transform   int `json:"transform"`
	Aggregation int `json:"aggregation"`
	Export      int `json:"export"`
}

// ConcurrencyConfig defines extra concurrency and job options
type ConcurrencyConfig struct {
	Workers           Workers `json:"workers"`
	ChannelBufferSize int     `json:"channelBufferSize"`
	JobTimeout        string  `json:"jobTimeout"` // e.g., "5m"
	APIRetry          int     `json:"apiRetry"`
}

// PipelineJobSpec defines the entire pipeline configuration
type PipelineJobSpec struct {
	Sources         []Source          `json:"sources"`               // multiple data sources
	Transformations []string          `json:"transformations"`       // global transformations
	Aggregation     *Aggregation      `json:"aggregation,omitempty"` // optional global aggregation
	Export          *Export           `json:"export,omitempty"`      // output/export rules
	Concurrency     ConcurrencyConfig `json:"concurrency"`           // concurrency and worker config
	Logging         bool              `json:"logging"`               // enable detailed logs
}

// PipelineChannels holds all the channels used for data flow between stages
type PipelineChannels struct {
	Records     chan GenericRecord    `json:"-"`
	Validated   chan GenericRecord    `json:"-"`
	Transformed chan GenericRecord    `json:"-"`
	Aggregated  chan AggregatedResult `json:"-"`
	Errors      chan error            `json:"-"`
}

// PipelineOrchestrator manages the execution of all pipeline stages
type PipelineOrchestrator struct {
	JobID   string           `json:"job_id"`
	Job     PipelineJobSpec  `json:"job"`
	Tracker *PipelineTracker `json:"-"`
}
