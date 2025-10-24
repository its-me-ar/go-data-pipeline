package utils

import (
	"reflect"
	"strconv"
	"strings"
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
	// Trim whitespace first
	s = strings.TrimSpace(s)

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

// numeric safely converts supported types to float64.
func Numeric(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case float32:
		return float64(val)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Float64 {
			return rv.Convert(reflect.TypeOf(float64(0))).Float()
		}
		return 0
	}
}
