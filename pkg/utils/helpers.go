package utils

import (
	"strconv"
	"time"
)

// parseDuration safely parses duration string like "5m"
func ParseDuration(d string) time.Duration {
	if d == "" {
		return 5 * time.Minute
	}
	duration, err := time.ParseDuration(d)
	if err != nil {
		return 5 * time.Minute
	}
	return duration
}

func ParseValue(s string) interface{} {
	// try int
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	// try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}
