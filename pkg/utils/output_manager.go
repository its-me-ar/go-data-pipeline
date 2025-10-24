package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// OutputManager handles output file organization and path management
type OutputManager struct {
	BaseOutputDir string
}

// NewOutputManager creates a new output manager
func NewOutputManager(baseOutputDir string) *OutputManager {
	return &OutputManager{
		BaseOutputDir: baseOutputDir,
	}
}

// CreateJobOutputDir creates a UUID-based directory for a job's outputs
func (om *OutputManager) CreateJobOutputDir(jobID string) (string, error) {
	jobDir := filepath.Join(om.BaseOutputDir, jobID)

	// Create the directory if it doesn't exist
	err := os.MkdirAll(jobDir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create job output directory: %w", err)
	}

	return jobDir, nil
}

// GetOutputFilePath generates a full path for an output file
func (om *OutputManager) GetOutputFilePath(jobID, fileName string) (string, error) {
	jobDir, err := om.CreateJobOutputDir(jobID)
	if err != nil {
		return "", err
	}

	// Clean the filename to remove any path separators
	cleanFileName := filepath.Base(fileName)

	return filepath.Join(jobDir, cleanFileName), nil
}

// GetDownloadURL generates a download URL for a file
func (om *OutputManager) GetDownloadURL(jobID, fileName string) string {
	cleanFileName := filepath.Base(fileName)
	return fmt.Sprintf("/api/v1/download/%s/%s", jobID, cleanFileName)
}

// GetFileType determines the file type based on extension
func (om *OutputManager) GetFileType(fileName string) string {
	ext := strings.ToLower(filepath.Ext(fileName))
	switch ext {
	case ".csv":
		return "csv"
	case ".json":
		return "json"
	case ".xlsx", ".xls":
		return "excel"
	case ".txt":
		return "text"
	case ".xml":
		return "xml"
	default:
		return "unknown"
	}
}

// GetFileSize returns the size of a file in bytes
func (om *OutputManager) GetFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

// EnsureOutputDirExists ensures the base output directory exists
func (om *OutputManager) EnsureOutputDirExists() error {
	return os.MkdirAll(om.BaseOutputDir, 0755)
}
