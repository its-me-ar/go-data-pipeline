package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"go-data-pipeline/internal/model"
	"go-data-pipeline/pkg/utils"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

// ------------------- Ingestion -------------------

// IngestSource starts ingestion for a single source (CSV/JSON/API)
func IngestSource(ctx context.Context, source model.Source, out chan<- GenericRecord, errors chan<- error) {
	fmt.Printf("➡️ Starting ingestion for source: %s (%s)\n", source.URL, source.Type)
	defer fmt.Printf("✅ Finished ingestion for source: %s (%s)\n", source.URL, source.Type)

	switch strings.ToLower(source.Type) {
	case "csv":
		ingestCSV(ctx, source.URL, out, errors)
	case "json", "api":
		ingestJSON(ctx, source.URL, out, errors)
	default:
		errors <- fmt.Errorf("unknown source type: %s", source.Type)
	}
}

// StartIngestion starts ingestion for all sources in parallel
func StartIngestion(ctx context.Context, sources []model.Source, out chan<- GenericRecord, errors chan<- error) {
	var wg sync.WaitGroup

	for _, src := range sources {
		wg.Add(1)
		go func(s model.Source) {
			defer wg.Done()
			IngestSource(ctx, s, out, errors)
		}(src)
	}

	wg.Wait() // wait for all ingestion goroutines
}

// ------------------- CSV Ingestion -------------------
func ingestCSV(ctx context.Context, pathOrURL string, out chan<- GenericRecord, errors chan<- error) {
	var reader io.Reader
	if strings.HasPrefix(pathOrURL, "http") {
		resp, err := http.Get(pathOrURL)
		if err != nil {
			errors <- fmt.Errorf("failed to GET CSV: %w", err)
			return
		}
		defer resp.Body.Close()
		reader = resp.Body
	} else {
		file, err := os.Open(pathOrURL)
		if err != nil {
			errors <- fmt.Errorf("failed to open CSV file: %w", err)
			return
		}
		defer file.Close()
		reader = file
	}

	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	headers, err := csvReader.Read()
	if err != nil {
		errors <- fmt.Errorf("failed to read CSV header: %w", err)
		return
	}

	recordCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			record, err := csvReader.Read()
			if err == io.EOF {
				fmt.Printf("📄 CSV ingestion done: %d records read from %s\n", recordCount, pathOrURL)
				return
			} else if err != nil {
				errors <- fmt.Errorf("CSV read error: %w", err)
				continue
			}

			recMap := make(GenericRecord)
			for i, h := range headers {
				// Clean header names: trim whitespace and remove ALL quotes
				cleanHeader := strings.TrimSpace(h)
				cleanHeader = strings.ReplaceAll(cleanHeader, `"`, "") // Remove all quotes
				recMap[cleanHeader] = utils.ParseValue(record[i])
			}
			recMap["SourceURL"] = pathOrURL

			select {
			case <-ctx.Done():
				return
			case out <- recMap:
				recordCount++
				if recordCount%50 == 0 || recordCount <= 10 {
					fmt.Printf("📄 CSV: Processed %d records from %s\n", recordCount, pathOrURL)
				}
			}
		}
	}
}

// ------------------- JSON / API Ingestion -------------------
func ingestJSON(ctx context.Context, url string, out chan<- GenericRecord, errors chan<- error) {
	fmt.Printf("🌐 GET JSON: %s\n", url)

	resp, err := http.Get(url)
	if err != nil {
		errors <- fmt.Errorf("failed to GET JSON: %w", err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		errors <- fmt.Errorf("failed to read JSON body: %w", err)
		return
	}

	var raw interface{}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		errors <- fmt.Errorf("failed to decode JSON: %w", err)
		return
	}

	recordCount := 0
	switch data := raw.(type) {
	case []interface{}:
		for _, item := range data {
			select {
			case <-ctx.Done():
				return
			default:
				if m, ok := item.(map[string]interface{}); ok {
					m["SourceURL"] = url
					select {
					case <-ctx.Done():
						return
					case out <- m:
						recordCount++
						if recordCount%10 == 0 || recordCount <= 5 {
							fmt.Printf("🌐 JSON: Processed %d records from %s\n", recordCount, url)
						}
					}
				}
			}
		}
	case map[string]interface{}:
		data["SourceURL"] = url
		select {
		case <-ctx.Done():
			return
		case out <- data:
			recordCount++
			fmt.Printf("🌐 JSON: Processed single record from %s\n", url)
		}
	default:
		errors <- fmt.Errorf("unexpected JSON structure")
		return
	}

	fmt.Printf("🌐 JSON ingestion done: %d records read from %s\n", recordCount, url)
}
